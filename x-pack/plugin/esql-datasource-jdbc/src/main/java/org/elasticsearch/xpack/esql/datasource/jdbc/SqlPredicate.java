/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import java.util.List;

/**
 * Vendor-neutral, fully-folded SQL predicate tree built by {@link JdbcFilterPushdownSupport} and rendered to a
 * parameterized SQL fragment by {@link SqlRenderer}.
 * <p>
 * Two design choices deserve a callout because they shape every variant below:
 * <ul>
 *   <li><b>Column-on-left, literal-on-right.</b> The translator always rewrites comparisons so the column is the
 *       LHS; {@link CompOp#commute()} handles the flip. {@link Comparison} therefore stores a single column-name
 *       string and a single {@link SqlParam}, never an LHS-as-literal variant. This keeps {@link SqlRenderer}
 *       trivial and eliminates a class of accidentally-asymmetric bugs.</li>
 *   <li><b>No {@code Expression} references.</b> Pushdown happens during local physical optimization on the
 *       coordinator and the pushed filter is consumed in the same JVM, but we still keep this tree free of
 *       ESQL expression objects so the renderer is independently testable and so the tree stays
 *       cheap-to-equality-check for the optimizer's "is this already pushed?" guard.</li>
 * </ul>
 */
public sealed interface SqlPredicate {

    /** {@code column <op> ?}, with {@code op} always understood as column-on-the-left. */
    record Comparison(String column, CompOp op, SqlParam param) implements SqlPredicate {
        public Comparison {
            requireNonEmpty(column);
            if (op == null) {
                throw new IllegalArgumentException("op must not be null");
            }
            if (param == null) {
                throw new IllegalArgumentException("param must not be null");
            }
        }
    }

    /** {@code column IS NULL}. */
    record IsNull(String column) implements SqlPredicate {
        public IsNull {
            requireNonEmpty(column);
        }
    }

    /** {@code column IS NOT NULL}. */
    record IsNotNull(String column) implements SqlPredicate {
        public IsNotNull {
            requireNonEmpty(column);
        }
    }

    /**
     * {@code column IN (?, ?, ...)}. Built only when every list item is foldable and at least one is non-null --
     * see {@link JdbcFilterPushdownSupport}. {@code values} must be non-empty.
     */
    record InList(String column, List<SqlParam> values) implements SqlPredicate {
        public InList {
            requireNonEmpty(column);
            if (values == null || values.isEmpty()) {
                throw new IllegalArgumentException("values must be non-empty");
            }
            values = List.copyOf(values);
        }
    }

    /**
     * JDBC {@code LIKE} pattern match. {@code pattern} is the already-translated JDBC-form pattern -- ESQL wildcards
     * ({@code *}, {@code ?}) have been mapped to ({@code %}, {@code _}) and pre-existing {@code %}/{@code _}
     * occurrences in the literal have been escaped with the chosen escape character. The renderer emits
     * {@code column LIKE ? ESCAPE '\\'} so all vendors interpret the pattern the same way.
     */
    record Like(String column, String pattern) implements SqlPredicate {
        public Like {
            requireNonEmpty(column);
            if (pattern == null) {
                throw new IllegalArgumentException("pattern must not be null");
            }
        }
    }

    /**
     * Closed range: {@code column BETWEEN ? AND ?} (or two inclusive/exclusive comparisons depending on the
     * inclusive flags). Both bounds are always present -- single-sided ranges arrive as plain {@link Comparison}.
     */
    record Range(String column, SqlParam lower, boolean lowerInclusive, SqlParam upper, boolean upperInclusive) implements SqlPredicate {
        public Range {
            requireNonEmpty(column);
            if (lower == null || upper == null) {
                throw new IllegalArgumentException("lower and upper must not be null");
            }
        }
    }

    /** Logical conjunction. Always normalized into a flat list of >= 2 parts -- {@link JdbcFilterPushdownSupport}
     *  unrolls nested {@code And} trees so the renderer can emit a single {@code (a AND b AND c)} group. */
    record And(List<SqlPredicate> parts) implements SqlPredicate {
        public And {
            requirePartsList(parts);
            parts = List.copyOf(parts);
        }
    }

    /** Logical disjunction. Same flattening contract as {@link And}. */
    record Or(List<SqlPredicate> parts) implements SqlPredicate {
        public Or {
            requirePartsList(parts);
            parts = List.copyOf(parts);
        }
    }

    /** Logical negation. Inner predicate must not be null. */
    record Not(SqlPredicate inner) implements SqlPredicate {
        public Not {
            if (inner == null) {
                throw new IllegalArgumentException("inner must not be null");
            }
        }
    }

    private static void requireNonEmpty(String column) {
        if (column == null || column.isEmpty()) {
            throw new IllegalArgumentException("column must not be null or empty");
        }
    }

    private static void requirePartsList(List<SqlPredicate> parts) {
        if (parts == null || parts.size() < 2) {
            throw new IllegalArgumentException("parts must contain at least two predicates");
        }
    }
}
