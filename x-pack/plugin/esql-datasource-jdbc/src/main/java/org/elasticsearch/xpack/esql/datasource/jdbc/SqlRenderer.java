/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import java.util.ArrayList;
import java.util.List;

/**
 * Renders a {@link SqlPredicate} tree into a parameterized SQL fragment plus an ordered list of {@link SqlParam}
 * values for {@link java.sql.PreparedStatement#setObject(int, Object)} binding via {@link JdbcDialect#bindParam}.
 * <p>
 * Constants are NEVER concatenated into the SQL string. The only string content the renderer emits is the
 * column identifiers (delegated to {@link JdbcDialect#quoteIdentifier(String)}), the operator/keyword text, and
 * {@code ?} placeholders. This is what allows {@link JdbcQueryBuilder} to keep the DEBUG-only SQL log free of
 * bound values.
 * <p>
 * The renderer parenthesizes compound predicates aggressively. ANSI precedence (AND binds tighter than OR, NOT
 * binds tighter than AND) is well-defined, but vendors are not above surprising us; explicit parens cost one
 * extra parser-allocated AST node and remove an entire class of operator-precedence escape vectors.
 */
final class SqlRenderer {

    /**
     * Rendered output. {@code sql} is the SQL fragment with {@code ?} placeholders (and the appropriate
     * {@code ESCAPE} clauses), {@code params} is the ordered binding list -- index N in the list maps to the
     * Nth {@code ?} in the fragment, JDBC-style 1-based binding handled by the caller.
     */
    record Rendered(String sql, List<SqlParam> params) {}

    private final JdbcDialect dialect;

    SqlRenderer(JdbcDialect dialect) {
        if (dialect == null) {
            throw new IllegalArgumentException("dialect must not be null");
        }
        this.dialect = dialect;
    }

    Rendered render(SqlPredicate predicate) {
        if (predicate == null) {
            throw new IllegalArgumentException("predicate must not be null");
        }
        StringBuilder sb = new StringBuilder(64);
        List<SqlParam> params = new ArrayList<>();
        renderInto(predicate, sb, params);
        // Single defensive copy: callers may mutate the returned record's list field via reflection if they want to,
        // but the renderer itself never aliases its internal ArrayList.
        return new Rendered(sb.toString(), List.copyOf(params));
    }

    private void renderInto(SqlPredicate p, StringBuilder sb, List<SqlParam> params) {
        switch (p) {
            case SqlPredicate.Comparison(String column, CompOp op, SqlParam param) -> {
                sb.append(dialect.quoteIdentifier(column)).append(' ').append(op.symbol()).append(" ?");
                params.add(param);
            }
            case SqlPredicate.IsNull(String column) -> sb.append(dialect.quoteIdentifier(column)).append(" IS NULL");
            case SqlPredicate.IsNotNull(String column) -> sb.append(dialect.quoteIdentifier(column)).append(" IS NOT NULL");
            case SqlPredicate.InList(String column, List<SqlParam> values) -> {
                sb.append(dialect.quoteIdentifier(column)).append(" IN (");
                for (int i = 0; i < values.size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append('?');
                    params.add(values.get(i));
                }
                sb.append(')');
            }
            case SqlPredicate.Like(String column, String pattern) -> {
                // ESCAPE '\\' is portable across H2, Postgres, MySQL, Snowflake. JdbcFilterPushdownSupport pre-escapes
                // backslash, %, _ in the literal, then translates ESQL * -> %, ? -> _, so the pattern reaching here is
                // already in JDBC form.
                sb.append(dialect.quoteIdentifier(column)).append(" LIKE ? ESCAPE '\\'");
                params.add(new SqlParam(pattern, org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD));
            }
            case SqlPredicate.Range(String column, SqlParam lower, boolean lowerInclusive, SqlParam upper, boolean upperInclusive) -> {
                if (lowerInclusive && upperInclusive) {
                    // Inclusive on both sides: BETWEEN is the standard, vendor-agnostic spelling.
                    sb.append(dialect.quoteIdentifier(column)).append(" BETWEEN ? AND ?");
                    params.add(lower);
                    params.add(upper);
                } else {
                    // Mixed inclusivity -- BETWEEN can't express it. Split into two comparisons.
                    sb.append('(');
                    sb.append(dialect.quoteIdentifier(column)).append(' ').append(lowerInclusive ? ">=" : ">").append(" ?");
                    params.add(lower);
                    sb.append(" AND ");
                    sb.append(dialect.quoteIdentifier(column)).append(' ').append(upperInclusive ? "<=" : "<").append(" ?");
                    params.add(upper);
                    sb.append(')');
                }
            }
            case SqlPredicate.And(List<SqlPredicate> parts) -> renderCompound(parts, " AND ", sb, params);
            case SqlPredicate.Or(List<SqlPredicate> parts) -> renderCompound(parts, " OR ", sb, params);
            case SqlPredicate.Not(SqlPredicate inner) -> {
                sb.append("NOT (");
                renderInto(inner, sb, params);
                sb.append(')');
            }
        }
    }

    private void renderCompound(List<SqlPredicate> parts, String joiner, StringBuilder sb, List<SqlParam> params) {
        sb.append('(');
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append(joiner);
            }
            renderInto(parts.get(i), sb, params);
        }
        sb.append(')');
    }
}
