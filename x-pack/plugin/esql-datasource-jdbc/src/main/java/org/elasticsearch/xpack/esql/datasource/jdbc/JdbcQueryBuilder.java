/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.List;

/**
 * Renders {@code SELECT cols FROM [catalog.][schema.]table [WHERE ...] [LIMIT n]} for the JDBC connector.
 * <p>
 * Identifiers (table / schema / catalog / column names) flow through {@link JdbcDialect#quoteIdentifier(String)} so
 * hostile inputs cannot break out of identifier quoting. Literal values from pushed-down filters are emitted as
 * {@code ?} placeholders by {@link SqlRenderer} and bound at execution time via {@link JdbcDialect#bindParam}; they
 * never appear in the generated SQL string.
 */
final class JdbcQueryBuilder {

    static final BuiltScan EMPTY_FILTER = new BuiltScan("", List.of());

    /**
     * Rendered scan output: the SQL string with {@code ?} placeholders for every pushed-down literal, plus the
     * ordered list of {@link SqlParam} values to bind 1-based at execution time.
     */
    record BuiltScan(String sql, List<SqlParam> params) {}

    private static final Logger logger = LogManager.getLogger(JdbcQueryBuilder.class);

    private final JdbcDialect dialect;
    private final SqlRenderer renderer;

    JdbcQueryBuilder(JdbcDialect dialect) {
        if (dialect == null) {
            throw new IllegalArgumentException("dialect must not be null");
        }
        this.dialect = dialect;
        this.renderer = new SqlRenderer(dialect);
    }

    /**
     * Builds the scan SQL with optional pushed-down WHERE.
     *
     * @param projectedColumns column names to SELECT, in projection order. An empty list is a row-count-only marker
     *        (e.g. {@code STATS COUNT(*)}) -- the rendered SQL becomes {@code SELECT 1 FROM ...} so the driver still
     *        produces one row per source row, but no source columns are read into ESQL blocks.
     * @param catalog optional JDBC catalog (may be {@code null} or empty)
     * @param schema optional JDBC schema (may be {@code null} or empty)
     * @param table table name; must not be null/empty
     * @param rowLimit if &gt; 0, append {@code LIMIT n}
     * @param pushedQuery optional pushed filter to translate into a {@code WHERE} clause; {@code null} means no filter
     * @return rendered SQL plus the ordered parameter list
     */
    BuiltScan buildScan(
        List<String> projectedColumns,
        String catalog,
        String schema,
        String table,
        int rowLimit,
        JdbcPushedQuery pushedQuery
    ) {
        if (projectedColumns == null) {
            throw new IllegalArgumentException("projectedColumns must not be null");
        }
        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("table must not be null or empty");
        }
        StringBuilder sb = new StringBuilder(64 + projectedColumns.size() * 16);
        sb.append("SELECT ");
        if (projectedColumns.isEmpty()) {
            // Row-count-only: emit a constant so the SELECT list is non-empty. We deliberately do not push COUNT(*)
            // server-side -- the planner pushes only the projection at this layer, and the row-counting block is
            // assembled by ESQL's COUNT operator from the page positions returned here.
            sb.append("1");
        } else {
            for (int i = 0; i < projectedColumns.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(dialect.quoteIdentifier(projectedColumns.get(i)));
            }
        }
        sb.append(" FROM ");
        appendQualifiedTable(sb, catalog, schema, table);
        SqlRenderer.Rendered where = pushedQuery == null ? null : renderer.render(pushedQuery.filter());
        if (where != null) {
            sb.append(" WHERE ").append(where.sql());
        }
        if (rowLimit > 0) {
            // ANSI: FETCH FIRST n ROWS ONLY would be more portable, but H2/Postgres/MySQL all accept LIMIT n; we keep
            // LIMIT for now and revisit when a vendor dialect needs the standard form.
            sb.append(" LIMIT ").append(rowLimit);
        }
        String sql = sb.toString();
        // DEBUG-only: SQL placeholders only. Bound values MUST NOT appear in log lines -- driver trace logs already
        // risk leaking them; we will not duplicate that exposure.
        logger.debug("JDBC scan SQL: [{}]", sql);
        return new BuiltScan(sql, where == null ? List.of() : where.params());
    }

    private void appendQualifiedTable(StringBuilder sb, String catalog, String schema, String table) {
        if (catalog != null && catalog.isEmpty() == false) {
            sb.append(dialect.quoteIdentifier(catalog)).append('.');
        }
        if (schema != null && schema.isEmpty() == false) {
            sb.append(dialect.quoteIdentifier(schema)).append('.');
        }
        sb.append(dialect.quoteIdentifier(table));
    }
}
