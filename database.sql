-- ============================================================
-- AutoIndex: PostgreSQL Self-Tuning Performance Analyzer
-- database.sql  |  PostgreSQL 14+
-- ============================================================

-- ─── Extensions ──────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ─── Schema ──────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS autoindex;

-- ============================================================
-- TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS autoindex.slow_query_log (
    id            BIGSERIAL PRIMARY KEY,
    query_text    TEXT             NOT NULL,
    calls         BIGINT           NOT NULL DEFAULT 0,
    total_time    DOUBLE PRECISION NOT NULL DEFAULT 0,
    mean_time     DOUBLE PRECISION NOT NULL DEFAULT 0,
    rows          BIGINT           NOT NULL DEFAULT 0,
    captured_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sql_mean_time  ON autoindex.slow_query_log (mean_time DESC);
CREATE INDEX IF NOT EXISTS idx_sql_captured   ON autoindex.slow_query_log (captured_at DESC);

-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS autoindex.query_plan_history (
    id                     BIGSERIAL PRIMARY KEY,
    query_id               BIGINT      NOT NULL
                               REFERENCES autoindex.slow_query_log(id) ON DELETE CASCADE,
    plan_json              JSONB       NOT NULL,
    seq_scan_detected      BOOLEAN     NOT NULL DEFAULT FALSE,
    rows_removed_by_filter BIGINT      NOT NULL DEFAULT 0,
    captured_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_qph_query_id ON autoindex.query_plan_history (query_id);
CREATE INDEX IF NOT EXISTS idx_qph_seq_scan ON autoindex.query_plan_history (seq_scan_detected)
    WHERE seq_scan_detected = TRUE;

-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS autoindex.index_recommendations (
    id                    BIGSERIAL PRIMARY KEY,
    query_id              BIGINT      NOT NULL
                              REFERENCES autoindex.slow_query_log(id) ON DELETE CASCADE,
    recommended_index_sql TEXT        NOT NULL,
    reason                TEXT        NOT NULL,
    estimated_cost_before DOUBLE PRECISION,
    estimated_cost_after  DOUBLE PRECISION,
    status                TEXT        NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending','applied','dismissed')),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ir_query_id ON autoindex.index_recommendations (query_id);
CREATE INDEX IF NOT EXISTS idx_ir_status   ON autoindex.index_recommendations (status);

-- ============================================================
-- HELPER: extract_plan_nodes(plan JSONB, node_type TEXT)
-- Recursively walks an EXPLAIN JSON tree and returns all nodes
-- whose "Node Type" matches p_type.
-- ============================================================
CREATE OR REPLACE FUNCTION autoindex.extract_plan_nodes(
    p_plan JSONB,
    p_type TEXT
)
RETURNS JSONB[]
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_nodes JSONB[] := '{}';
    v_child JSONB;
    v_sub   JSONB[];
BEGIN
    IF p_plan IS NULL THEN RETURN v_nodes; END IF;

    -- Array wrapper: EXPLAIN FORMAT JSON returns [{...}]
    IF jsonb_typeof(p_plan) = 'array' THEN
        FOR v_child IN SELECT jsonb_array_elements(p_plan) LOOP
            v_nodes := v_nodes || autoindex.extract_plan_nodes(v_child, p_type);
        END LOOP;
        RETURN v_nodes;
    END IF;

    -- {"Plan": {...}} wrapper from root element
    IF p_plan ? 'Plan' THEN
        RETURN autoindex.extract_plan_nodes(p_plan -> 'Plan', p_type);
    END IF;

    -- Current node match
    IF (p_plan ->> 'Node Type') = p_type THEN
        v_nodes := array_append(v_nodes, p_plan);
    END IF;

    -- Recurse into children
    IF p_plan ? 'Plans' THEN
        FOR v_child IN SELECT jsonb_array_elements(p_plan -> 'Plans') LOOP
            v_nodes := v_nodes || autoindex.extract_plan_nodes(v_child, p_type);
        END LOOP;
    END IF;

    RETURN v_nodes;
END;
$$;

-- ============================================================
-- FUNCTION 1: capture_slow_queries(min_ms INT) → INT
-- Reads pg_stat_statements, inserts rows exceeding the mean_ms
-- threshold into slow_query_log (de-duped per minute).
-- Returns count of new rows inserted.
-- ============================================================
CREATE OR REPLACE FUNCTION autoindex.capture_slow_queries(
    p_min_ms INT DEFAULT 100
)
RETURNS INT
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
    v_inserted INT := 0;
    v_rec      RECORD;
BEGIN
    FOR v_rec IN
        SELECT
            query                             AS query_text,
            calls,
            total_exec_time::DOUBLE PRECISION AS total_time,
            mean_exec_time::DOUBLE PRECISION  AS mean_time,
            rows
        FROM pg_stat_statements
        WHERE mean_exec_time >= p_min_ms
          AND calls           > 0
          AND query NOT ILIKE '%autoindex%'
          AND query NOT ILIKE '%pg_stat_statements%'
        ORDER BY mean_exec_time DESC
        LIMIT 200
    LOOP
        CONTINUE WHEN EXISTS (
            SELECT 1 FROM autoindex.slow_query_log
            WHERE query_text = v_rec.query_text
              AND captured_at >= NOW() - INTERVAL '1 minute'
        );

        INSERT INTO autoindex.slow_query_log
            (query_text, calls, total_time, mean_time, rows, captured_at)
        VALUES
            (v_rec.query_text, v_rec.calls, v_rec.total_time,
             v_rec.mean_time,  v_rec.rows,  NOW());

        v_inserted := v_inserted + 1;
    END LOOP;

    RETURN v_inserted;
END;
$$;

-- ============================================================
-- FUNCTION 2: analyze_query_plan(query_id INT) → BIGINT
-- Runs EXPLAIN (FORMAT JSON, COSTS TRUE) on the stored query
-- (with $N replaced by NULL).  Detects:
--   • Sequential scans on tables > 10 000 rows returning < 10%
--   • High rows-removed-by-filter counts
-- Stores result in query_plan_history.
-- Returns the new plan history row id.
-- ============================================================
CREATE OR REPLACE FUNCTION autoindex.analyze_query_plan(
    p_query_id INT
)
RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
    v_query_text        TEXT;
    v_explain_sql       TEXT;
    v_plan_json         JSONB;
    v_seq_scan_detected BOOLEAN  := FALSE;
    v_rows_removed      BIGINT   := 0;
    v_plan_id           BIGINT;
    v_seq_nodes         JSONB[];
    v_node              JSONB;
    v_rel_name          TEXT;
    v_plan_rows         BIGINT;
    v_filter_removed    BIGINT;
    v_rel_rows          BIGINT;
BEGIN
    -- Fetch query text
    SELECT query_text INTO STRICT v_query_text
    FROM autoindex.slow_query_log WHERE id = p_query_id;

    -- Sanitise: replace bind params, strip trailing semicolons
    v_explain_sql := regexp_replace(v_query_text, '\$[0-9]+', 'NULL', 'g');
    v_explain_sql := rtrim(trim(v_explain_sql), ';');

    -- Run EXPLAIN (no ANALYZE — avoids side-effects and lock issues)
    BEGIN
        EXECUTE 'EXPLAIN (FORMAT JSON, COSTS TRUE) ' || v_explain_sql
        INTO v_plan_json;
    EXCEPTION WHEN OTHERS THEN
        v_plan_json := jsonb_build_array(jsonb_build_object(
            'Plan', jsonb_build_object(
                'Node Type', 'Unanalyzable',
                'Error', SQLERRM
            )
        ));
    END;

    -- Walk plan tree for Seq Scan nodes
    v_seq_nodes := autoindex.extract_plan_nodes(v_plan_json, 'Seq Scan');

    FOREACH v_node IN ARRAY v_seq_nodes LOOP
        v_rel_name       := v_node ->> 'Relation Name';
        v_plan_rows      := COALESCE((v_node ->> 'Plan Rows')::BIGINT, 0);
        v_filter_removed := COALESCE((v_node ->> 'Rows Removed by Filter')::BIGINT, 0);

        SELECT GREATEST(reltuples::BIGINT, 0) INTO v_rel_rows
        FROM pg_class WHERE relname = v_rel_name LIMIT 1;

        v_rel_rows := COALESCE(v_rel_rows, 0);

        -- Detection rule: large table + low selectivity
        IF v_rel_rows > 10000 AND v_plan_rows < (v_rel_rows * 0.10) THEN
            v_seq_scan_detected := TRUE;
        END IF;

        v_rows_removed := v_rows_removed + v_filter_removed;
    END LOOP;

    -- Persist
    INSERT INTO autoindex.query_plan_history
        (query_id, plan_json, seq_scan_detected, rows_removed_by_filter, captured_at)
    VALUES
        (p_query_id, v_plan_json, v_seq_scan_detected, v_rows_removed, NOW())
    RETURNING id INTO v_plan_id;

    RETURN v_plan_id;
END;
$$;

-- ============================================================
-- FUNCTION 3: generate_index_recommendation(query_id INT) → INT
-- Generates index suggestions (A–D) based on the latest plan:
--   A. Single-column B-tree on filtered column (seq scan)
--   B. Partial index when rows_removed_by_filter > 5000
--   C. Composite index for multi-column WHERE clauses
--   D. Index on join column (Hash Join / Nested Loop)
-- Returns count of new recommendation rows inserted.
-- ============================================================
CREATE OR REPLACE FUNCTION autoindex.generate_index_recommendation(
    p_query_id INT
)
RETURNS INT
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
    v_query_text        TEXT;
    v_plan_json         JSONB;
    v_seq_scan_detected BOOLEAN;
    v_rows_removed      BIGINT;
    v_recs_added        INT     := 0;
    v_schema            CONSTANT TEXT := 'public';

    v_seq_nodes   JSONB[];
    v_hash_nodes  JSONB[];
    v_nl_nodes    JSONB[];
    v_node        JSONB;
    v_join_node   JSONB;

    v_rel_name    TEXT;
    v_filter_cond TEXT;
    v_join_cond   TEXT;
    v_col1        TEXT;
    v_col2        TEXT;
    v_tbl         TEXT;
    v_jcol        TEXT;
    v_idx_name    TEXT;
    v_idx_sql     TEXT;
    v_reason      TEXT;
    v_col_match   TEXT[];
    v_col_list    TEXT[];
    v_idx_exists  BOOLEAN;
BEGIN
    -- Fetch query + latest plan
    SELECT s.query_text, p.plan_json, p.seq_scan_detected, p.rows_removed_by_filter
    INTO STRICT v_query_text, v_plan_json, v_seq_scan_detected, v_rows_removed
    FROM autoindex.slow_query_log s
    JOIN autoindex.query_plan_history p ON p.query_id = s.id
    WHERE s.id = p_query_id
    ORDER BY p.captured_at DESC
    LIMIT 1;

    -- ─── A / B : Sequential scan ─────────────────────────────
    IF v_seq_scan_detected THEN
        v_seq_nodes := autoindex.extract_plan_nodes(v_plan_json, 'Seq Scan');

        FOREACH v_node IN ARRAY v_seq_nodes LOOP
            v_rel_name    := v_node ->> 'Relation Name';
            v_filter_cond := v_node ->> 'Filter';
            CONTINUE WHEN v_rel_name IS NULL OR v_filter_cond IS NULL;

            v_col_match := regexp_match(
                v_filter_cond, '\(([a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!~]'
            );
            v_col1 := v_col_match[1];
            CONTINUE WHEN v_col1 IS NULL;

            SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE tablename = v_rel_name
                  AND indexdef ILIKE '%(' || v_col1 || ')%'
            ) INTO v_idx_exists;
            CONTINUE WHEN v_idx_exists;

            v_idx_name := format('idx_ai_%s_%s', lower(v_rel_name), lower(v_col1));

            IF v_rows_removed > 5000 THEN
                -- B: Partial index
                v_idx_sql := format(
                    'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I.%I (%I)'
                    ' WHERE %I IS NOT NULL;',
                    v_idx_name, v_schema, v_rel_name, v_col1, v_col1
                );
                v_reason := format(
                    'Seq scan on %I filtered by %I; %s rows removed by filter '
                    '— partial index on non-NULL values reduces index size.',
                    v_rel_name, v_col1, v_rows_removed
                );
            ELSE
                -- A: Standard B-tree
                v_idx_sql := format(
                    'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I.%I (%I);',
                    v_idx_name, v_schema, v_rel_name, v_col1
                );
                v_reason := format(
                    'Sequential scan on large table %I filtered by %I. '
                    'B-tree index enables index scan and eliminates full table read.',
                    v_rel_name, v_col1
                );
            END IF;

            CONTINUE WHEN EXISTS (
                SELECT 1 FROM autoindex.index_recommendations
                WHERE query_id = p_query_id AND recommended_index_sql = v_idx_sql
            );

            INSERT INTO autoindex.index_recommendations
                (query_id, recommended_index_sql, reason, created_at)
            VALUES (p_query_id, v_idx_sql, v_reason, NOW());
            v_recs_added := v_recs_added + 1;
        END LOOP;
    END IF;

    -- ─── C : Composite index ──────────────────────────────────
    v_seq_nodes := autoindex.extract_plan_nodes(v_plan_json, 'Seq Scan');
    FOREACH v_node IN ARRAY v_seq_nodes LOOP
        v_rel_name    := v_node ->> 'Relation Name';
        v_filter_cond := v_node ->> 'Filter';
        CONTINUE WHEN v_rel_name IS NULL OR v_filter_cond IS NULL;

        SELECT ARRAY(
            SELECT DISTINCT m[1]
            FROM regexp_matches(
                v_filter_cond,
                '([a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]', 'g'
            ) AS m
            WHERE m[1] NOT IN (
                'AND','OR','NOT','NULL','IS','IN','ANY','ALL',
                'LIKE','ILIKE','BETWEEN','TRUE','FALSE'
            )
        ) INTO v_col_list;

        CONTINUE WHEN array_length(v_col_list, 1) < 2;
        v_col1 := v_col_list[1];
        v_col2 := v_col_list[2];

        SELECT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE tablename = v_rel_name
              AND indexdef ILIKE '%' || v_col1 || '%'
              AND indexdef ILIKE '%' || v_col2 || '%'
        ) INTO v_idx_exists;
        CONTINUE WHEN v_idx_exists;

        v_idx_name := format('idx_ai_%s_%s_%s_comp',
            lower(v_rel_name), lower(v_col1), lower(v_col2));
        v_idx_sql := format(
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I.%I (%I, %I);',
            v_idx_name, v_schema, v_rel_name, v_col1, v_col2
        );
        v_reason := format(
            'Multi-column WHERE clause on %I references %I and %I. '
            'Composite index covers both predicates in one lookup.',
            v_rel_name, v_col1, v_col2
        );

        CONTINUE WHEN EXISTS (
            SELECT 1 FROM autoindex.index_recommendations
            WHERE query_id = p_query_id AND recommended_index_sql = v_idx_sql
        );

        INSERT INTO autoindex.index_recommendations
            (query_id, recommended_index_sql, reason, created_at)
        VALUES (p_query_id, v_idx_sql, v_reason, NOW());
        v_recs_added := v_recs_added + 1;
    END LOOP;

    -- ─── D : Join column indexes ──────────────────────────────
    v_hash_nodes := autoindex.extract_plan_nodes(v_plan_json, 'Hash Join');
    v_nl_nodes   := autoindex.extract_plan_nodes(v_plan_json, 'Nested Loop');

    FOREACH v_join_node IN ARRAY (v_hash_nodes || v_nl_nodes) LOOP
        v_join_cond := COALESCE(
            v_join_node ->> 'Hash Cond',
            v_join_node ->> 'Join Filter'
        );
        CONTINUE WHEN v_join_cond IS NULL;

        v_col_match := regexp_match(
            v_join_cond,
            '([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*='
        );
        CONTINUE WHEN v_col_match IS NULL;

        v_tbl  := v_col_match[1];
        v_jcol := v_col_match[2];

        SELECT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE tablename = v_tbl
              AND indexdef ILIKE '%(' || v_jcol || ')%'
        ) INTO v_idx_exists;
        CONTINUE WHEN v_idx_exists;

        v_idx_name := format('idx_ai_%s_%s_join', lower(v_tbl), lower(v_jcol));
        v_idx_sql := format(
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I.%I (%I);',
            v_idx_name, v_schema, v_tbl, v_jcol
        );
        v_reason := format(
            'Join condition [%s] uses column %I.%I with no index. '
            'Index eliminates full scan on the inner side of the join.',
            v_join_cond, v_tbl, v_jcol
        );

        CONTINUE WHEN EXISTS (
            SELECT 1 FROM autoindex.index_recommendations
            WHERE query_id = p_query_id AND recommended_index_sql = v_idx_sql
        );

        INSERT INTO autoindex.index_recommendations
            (query_id, recommended_index_sql, reason, created_at)
        VALUES (p_query_id, v_idx_sql, v_reason, NOW());
        v_recs_added := v_recs_added + 1;
    END LOOP;

    RETURN v_recs_added;
END;
$$;

-- ============================================================
-- FUNCTION 4: estimate_improvement(query_id INT) → VOID
-- Reads root plan cost, then estimates post-index cost by
-- replacing each Seq Scan node cost with 12% (index scan
-- heuristic).  Updates all pending recommendations.
-- ============================================================
CREATE OR REPLACE FUNCTION autoindex.estimate_improvement(
    p_query_id INT
)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
    v_plan_json   JSONB;
    v_cost_before DOUBLE PRECISION := 0;
    v_cost_after  DOUBLE PRECISION;
    v_seq_nodes   JSONB[];
    v_node        JSONB;
    v_node_cost   DOUBLE PRECISION;
    v_reduction   CONSTANT DOUBLE PRECISION := 0.12;
BEGIN
    SELECT plan_json INTO v_plan_json
    FROM autoindex.query_plan_history
    WHERE query_id = p_query_id
    ORDER BY captured_at DESC LIMIT 1;

    RETURN WHEN NOT FOUND OR v_plan_json IS NULL;

    v_cost_before := COALESCE(
        (v_plan_json -> 0 -> 'Plan' ->> 'Total Cost')::DOUBLE PRECISION, 0
    );
    v_cost_after := v_cost_before;

    v_seq_nodes := autoindex.extract_plan_nodes(v_plan_json, 'Seq Scan');
    FOREACH v_node IN ARRAY v_seq_nodes LOOP
        v_node_cost  := COALESCE((v_node ->> 'Total Cost')::DOUBLE PRECISION, 0);
        v_cost_after := v_cost_after - v_node_cost + (v_node_cost * v_reduction);
    END LOOP;

    v_cost_after := GREATEST(v_cost_after, 0.01);

    UPDATE autoindex.index_recommendations
    SET estimated_cost_before = v_cost_before,
        estimated_cost_after  = v_cost_after
    WHERE query_id = p_query_id;
END;
$$;

-- ============================================================
-- PROCEDURE 5: autoindex_run(min_ms INT)  ← Master entry point
-- ============================================================
CREATE OR REPLACE PROCEDURE autoindex.autoindex_run(
    p_min_ms INT DEFAULT 100
)
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
    v_captured   INT;
    v_rec        RECORD;
    v_plan_id    BIGINT;
    v_recs       INT;
    v_total_recs INT := 0;
    v_processed  INT := 0;
BEGIN
    RAISE NOTICE '[AutoIndex] === Run started  threshold=% ms ===', p_min_ms;

    v_captured := autoindex.capture_slow_queries(p_min_ms);
    RAISE NOTICE '[AutoIndex] Captured % new slow query/ies.', v_captured;

    FOR v_rec IN
        SELECT s.id
        FROM autoindex.slow_query_log s
        WHERE NOT EXISTS (
            SELECT 1 FROM autoindex.query_plan_history p
            WHERE p.query_id   = s.id
              AND p.captured_at >= NOW() - INTERVAL '10 minutes'
        )
        ORDER BY s.mean_time DESC
        LIMIT 50
    LOOP
        BEGIN
            v_plan_id := autoindex.analyze_query_plan(v_rec.id);
            v_recs    := autoindex.generate_index_recommendation(v_rec.id);
            PERFORM autoindex.estimate_improvement(v_rec.id);

            RAISE NOTICE '[AutoIndex] query_id=% plan_id=% recs=%',
                v_rec.id, v_plan_id, v_recs;

            v_total_recs := v_total_recs + v_recs;
            v_processed  := v_processed  + 1;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '[AutoIndex] Error query_id=% : %', v_rec.id, SQLERRM;
        END;
    END LOOP;

    RAISE NOTICE '[AutoIndex] === Done  processed=% new_recs=% ===',
        v_processed, v_total_recs;
END;
$$;

-- ============================================================
-- REPORTING VIEWS
-- ============================================================

CREATE OR REPLACE VIEW autoindex.v_recommendations_summary AS
SELECT
    r.id                                                         AS rec_id,
    s.id                                                         AS query_id,
    left(s.query_text, 120)                                      AS query_preview,
    round(s.mean_time::NUMERIC, 2)                               AS mean_ms,
    s.calls,
    r.reason,
    r.recommended_index_sql,
    round(r.estimated_cost_before::NUMERIC, 2)                   AS cost_before,
    round(r.estimated_cost_after::NUMERIC,  2)                   AS cost_after,
    CASE WHEN COALESCE(r.estimated_cost_before,0) > 0 THEN
        round(((r.estimated_cost_before - r.estimated_cost_after)
               / r.estimated_cost_before * 100)::NUMERIC, 1)
    END                                                          AS improvement_pct,
    r.status,
    r.created_at
FROM autoindex.index_recommendations r
JOIN autoindex.slow_query_log s ON s.id = r.query_id
ORDER BY s.mean_time DESC, r.estimated_cost_before DESC NULLS LAST;

CREATE OR REPLACE VIEW autoindex.v_seq_scan_alerts AS
SELECT
    s.id,
    left(s.query_text, 120)              AS query_preview,
    round(s.mean_time::NUMERIC, 2)       AS mean_ms,
    s.calls,
    p.rows_removed_by_filter,
    p.captured_at
FROM autoindex.slow_query_log     s
JOIN autoindex.query_plan_history p ON p.query_id = s.id
WHERE p.seq_scan_detected = TRUE
ORDER BY s.mean_time DESC;

CREATE OR REPLACE VIEW autoindex.v_unoptimized_queries AS
SELECT
    s.id,
    left(s.query_text, 120)          AS query_preview,
    round(s.mean_time::NUMERIC, 2)   AS mean_ms,
    s.calls,
    s.captured_at
FROM autoindex.slow_query_log s
WHERE NOT EXISTS (
    SELECT 1 FROM autoindex.index_recommendations r
    WHERE r.query_id = s.id
)
ORDER BY s.mean_time DESC;
