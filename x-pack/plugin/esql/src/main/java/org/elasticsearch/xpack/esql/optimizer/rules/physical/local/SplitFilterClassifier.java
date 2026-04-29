/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.List;

/**
 * Evaluates filter predicates against per-split (row-group/stripe) statistics to classify
 * each split into one of three states: all rows match the filter, no rows match, or the
 * result is unknown from statistics alone.
 * <p>
 * Used by aggregate pushdown rules to determine whether aggregates (COUNT, MIN, MAX) can
 * be resolved from metadata without scanning data. When all splits are classified as MATCH
 * or MISS, the aggregate can be computed from the MATCH splits' statistics alone.
 * <p>
 * Supports AND, OR, and NOT logical operators, IS NULL / IS NOT NULL null checks, IN lists,
 * and leaf comparisons (field op literal). Unsupported leaf expressions (function calls, etc.)
 * conservatively return AMBIGUOUS.
 * NOT of MATCH yields MISS; NOT of MISS conservatively yields AMBIGUOUS because null rows
 * still produce UNKNOWN under WHERE semantics.
 * <p>
 * NULL handling: a split is only classified as MATCH when the filter column has zero null
 * values ({@code null_count = 0}), because NULL comparisons evaluate to UNKNOWN and are
 * dropped by WHERE clauses.
 */
final class SplitFilterClassifier {

    enum SplitMatch {
        /** All rows in the split satisfy the filter predicate. */
        MATCH,
        /** No rows in the split satisfy the filter predicate. */
        MISS,
        /** Cannot determine from statistics alone; data scan required. */
        AMBIGUOUS
    }

    private SplitFilterClassifier() {}

    /**
     * Classifies a split by recursively evaluating the filter expression tree against its statistics.
     * Supports AND, OR, NOT, and leaf comparisons (field op literal).
     */
    public static SplitMatch classifyExpression(Expression filter, SplitStats splitStats) {
        if (filter == null || splitStats == null || splitStats.columnCount() == 0) {
            return SplitMatch.AMBIGUOUS;
        }
        return classifyRecursive(filter, splitStats);
    }

    /**
     * Classifies a split by evaluating AND-connected filter conjuncts against its statistics.
     * Each conjunct is classified recursively, so nested {@code And}, {@code Or}, and {@code Not}
     * within a conjunct are handled.
     * <p>
     * Conjunction semantics: MISS if any conjunct is MISS (short-circuit), MATCH only if
     * all conjuncts are MATCH, AMBIGUOUS otherwise.
     *
     * @param filterConjuncts AND-separated filter expressions (from {@code splitAnd()})
     * @param splitStats per-split statistics
     * @return the classification for this split
     */
    static SplitMatch classifySplit(List<Expression> filterConjuncts, SplitStats splitStats) {
        if (filterConjuncts.isEmpty() || splitStats == null) {
            return SplitMatch.AMBIGUOUS;
        }
        if (filterConjuncts.size() == 1) {
            return classifyRecursive(filterConjuncts.getFirst(), splitStats);
        }
        boolean allMatch = true;
        for (Expression conjunct : filterConjuncts) {
            SplitMatch result = classifyRecursive(conjunct, splitStats);
            if (result == SplitMatch.MISS) {
                return SplitMatch.MISS;
            }
            if (result != SplitMatch.MATCH) {
                allMatch = false;
            }
        }
        return allMatch ? SplitMatch.MATCH : SplitMatch.AMBIGUOUS;
    }

    private static SplitMatch classifyRecursive(Expression expr, SplitStats splitStats) {
        if (expr instanceof And and) {
            SplitMatch left = classifyRecursive(and.left(), splitStats);
            if (left == SplitMatch.MISS) {
                return SplitMatch.MISS;
            }
            SplitMatch right = classifyRecursive(and.right(), splitStats);
            if (right == SplitMatch.MISS) {
                return SplitMatch.MISS;
            }
            if (left == SplitMatch.MATCH && right == SplitMatch.MATCH) {
                return SplitMatch.MATCH;
            }
            return SplitMatch.AMBIGUOUS;
        }
        if (expr instanceof Or or) {
            SplitMatch left = classifyRecursive(or.left(), splitStats);
            if (left == SplitMatch.MATCH) {
                return SplitMatch.MATCH;
            }
            SplitMatch right = classifyRecursive(or.right(), splitStats);
            if (right == SplitMatch.MATCH) {
                return SplitMatch.MATCH;
            }
            if (left == SplitMatch.MISS && right == SplitMatch.MISS) {
                return SplitMatch.MISS;
            }
            return SplitMatch.AMBIGUOUS;
        }
        if (expr instanceof Not not) {
            SplitMatch child = classifyRecursive(not.field(), splitStats);
            return switch (child) {
                case MATCH -> SplitMatch.MISS;
                case MISS -> SplitMatch.AMBIGUOUS;
                case AMBIGUOUS -> SplitMatch.AMBIGUOUS;
            };
        }
        if (expr instanceof IsNull isNull) {
            return classifyIsNull(isNull, splitStats);
        }
        if (expr instanceof IsNotNull isNotNull) {
            return classifyIsNotNull(isNotNull, splitStats);
        }
        if (expr instanceof In in) {
            return classifyIn(in, splitStats);
        }
        return classifyConjunct(expr, splitStats);
    }

    private static SplitMatch classifyConjunct(Expression expr, SplitStats splitStats) {
        String columnName;
        Object literalValue;
        boolean reversed;

        if (expr instanceof GreaterThan gt) {
            columnName = extractColumnName(gt.left());
            literalValue = extractLiteralValue(gt.right());
            reversed = false;
            if (columnName == null || literalValue == null) {
                columnName = extractColumnName(gt.right());
                literalValue = extractLiteralValue(gt.left());
                reversed = true;
            }
            if (columnName == null || literalValue == null) {
                return SplitMatch.AMBIGUOUS;
            }
            return reversed
                ? classifyLessThan(columnName, literalValue, splitStats)
                : classifyGreaterThan(columnName, literalValue, splitStats);
        } else if (expr instanceof GreaterThanOrEqual gte) {
            columnName = extractColumnName(gte.left());
            literalValue = extractLiteralValue(gte.right());
            reversed = false;
            if (columnName == null || literalValue == null) {
                columnName = extractColumnName(gte.right());
                literalValue = extractLiteralValue(gte.left());
                reversed = true;
            }
            if (columnName == null || literalValue == null) {
                return SplitMatch.AMBIGUOUS;
            }
            return reversed
                ? classifyLessThanOrEqual(columnName, literalValue, splitStats)
                : classifyGreaterThanOrEqual(columnName, literalValue, splitStats);
        } else if (expr instanceof LessThan lt) {
            columnName = extractColumnName(lt.left());
            literalValue = extractLiteralValue(lt.right());
            reversed = false;
            if (columnName == null || literalValue == null) {
                columnName = extractColumnName(lt.right());
                literalValue = extractLiteralValue(lt.left());
                reversed = true;
            }
            if (columnName == null || literalValue == null) {
                return SplitMatch.AMBIGUOUS;
            }
            return reversed
                ? classifyGreaterThan(columnName, literalValue, splitStats)
                : classifyLessThan(columnName, literalValue, splitStats);
        } else if (expr instanceof LessThanOrEqual lte) {
            columnName = extractColumnName(lte.left());
            literalValue = extractLiteralValue(lte.right());
            reversed = false;
            if (columnName == null || literalValue == null) {
                columnName = extractColumnName(lte.right());
                literalValue = extractLiteralValue(lte.left());
                reversed = true;
            }
            if (columnName == null || literalValue == null) {
                return SplitMatch.AMBIGUOUS;
            }
            return reversed
                ? classifyGreaterThanOrEqual(columnName, literalValue, splitStats)
                : classifyLessThanOrEqual(columnName, literalValue, splitStats);
        } else if (expr instanceof Equals eq) {
            columnName = extractColumnName(eq.left());
            literalValue = extractLiteralValue(eq.right());
            if (columnName == null || literalValue == null) {
                columnName = extractColumnName(eq.right());
                literalValue = extractLiteralValue(eq.left());
            }
            if (columnName == null || literalValue == null) {
                return SplitMatch.AMBIGUOUS;
            }
            return classifyEquals(columnName, literalValue, splitStats);
        } else if (expr instanceof NotEquals neq) {
            columnName = extractColumnName(neq.left());
            literalValue = extractLiteralValue(neq.right());
            if (columnName == null || literalValue == null) {
                columnName = extractColumnName(neq.right());
                literalValue = extractLiteralValue(neq.left());
            }
            if (columnName == null || literalValue == null) {
                return SplitMatch.AMBIGUOUS;
            }
            return classifyNotEquals(columnName, literalValue, splitStats);
        }
        return SplitMatch.AMBIGUOUS;
    }

    /**
     * {@code col IS NULL}: MATCH when null_count == row_count, MISS when null_count == 0.
     */
    private static SplitMatch classifyIsNull(IsNull isNull, SplitStats splitStats) {
        String columnName = extractColumnName(isNull.field());
        if (columnName == null) {
            return SplitMatch.AMBIGUOUS;
        }
        long nullCount = splitStats.columnNullCount(columnName);
        if (nullCount < 0) {
            return SplitMatch.AMBIGUOUS;
        }
        if (nullCount == 0) {
            return SplitMatch.MISS;
        }
        if (nullCount == splitStats.rowCount()) {
            return SplitMatch.MATCH;
        }
        return SplitMatch.AMBIGUOUS;
    }

    /**
     * {@code col IS NOT NULL}: MATCH when null_count == 0, MISS when null_count == row_count.
     */
    private static SplitMatch classifyIsNotNull(IsNotNull isNotNull, SplitStats splitStats) {
        String columnName = extractColumnName(isNotNull.field());
        if (columnName == null) {
            return SplitMatch.AMBIGUOUS;
        }
        long nullCount = splitStats.columnNullCount(columnName);
        if (nullCount < 0) {
            return SplitMatch.AMBIGUOUS;
        }
        if (nullCount == 0) {
            return SplitMatch.MATCH;
        }
        if (nullCount == splitStats.rowCount()) {
            return SplitMatch.MISS;
        }
        return SplitMatch.AMBIGUOUS;
    }

    /**
     * {@code col IN (lit1, lit2, ...)}: MISS when all literals fall outside [min, max],
     * MATCH when min == max and that value equals one of the literals and null_count == 0.
     */
    private static SplitMatch classifyIn(In in, SplitStats splitStats) {
        String columnName = extractColumnName(in.value());
        if (columnName == null) {
            return SplitMatch.AMBIGUOUS;
        }
        List<Object> literalValues = in.list().stream().map(SplitFilterClassifier::extractLiteralValue).toList();
        if (literalValues.stream().anyMatch(v -> v == null)) {
            return SplitMatch.AMBIGUOUS;
        }
        Object min = splitStats.columnMin(columnName);
        Object max = splitStats.columnMax(columnName);
        if (min == null || max == null) {
            return SplitMatch.AMBIGUOUS;
        }

        boolean anyInRange = false;
        for (Object lit : literalValues) {
            int cmpMin = compareValues(lit, min);
            int cmpMax = compareValues(lit, max);
            if (cmpMin == Integer.MIN_VALUE || cmpMax == Integer.MIN_VALUE) {
                return SplitMatch.AMBIGUOUS;
            }
            if (cmpMin >= 0 && cmpMax <= 0) {
                anyInRange = true;
            }
        }

        if (anyInRange == false) {
            return SplitMatch.MISS;
        }

        int minMaxCmp = compareValues(min, max);
        if (minMaxCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        if (minMaxCmp == 0) {
            for (Object lit : literalValues) {
                if (compareValues(min, lit) == 0 && hasNoNulls(columnName, splitStats)) {
                    return SplitMatch.MATCH;
                }
            }
        }
        return SplitMatch.AMBIGUOUS;
    }

    /**
     * {@code col > lit}: MATCH when min &gt; lit (and no nulls), MISS when max &lt;= lit
     */
    private static SplitMatch classifyGreaterThan(String columnName, Object literalValue, SplitStats splitStats) {
        Object min = splitStats.columnMin(columnName);
        Object max = splitStats.columnMax(columnName);
        if (min == null || max == null) {
            return SplitMatch.AMBIGUOUS;
        }
        int maxCmp = compareValues(max, literalValue);
        if (maxCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        if (maxCmp <= 0) {
            return SplitMatch.MISS;
        }
        int minCmp = compareValues(min, literalValue);
        if (minCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        if (minCmp > 0 && hasNoNulls(columnName, splitStats)) {
            return SplitMatch.MATCH;
        }
        return SplitMatch.AMBIGUOUS;
    }

    /**
     * {@code col >= lit}: MATCH when min &gt;= lit (and no nulls), MISS when max &lt; lit
     */
    private static SplitMatch classifyGreaterThanOrEqual(String columnName, Object literalValue, SplitStats splitStats) {
        Object min = splitStats.columnMin(columnName);
        Object max = splitStats.columnMax(columnName);
        if (min == null || max == null) {
            return SplitMatch.AMBIGUOUS;
        }
        int maxCmp = compareValues(max, literalValue);
        if (maxCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        if (maxCmp < 0) {
            return SplitMatch.MISS;
        }
        int minCmp = compareValues(min, literalValue);
        if (minCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        if (minCmp >= 0 && hasNoNulls(columnName, splitStats)) {
            return SplitMatch.MATCH;
        }
        return SplitMatch.AMBIGUOUS;
    }

    /**
     * {@code col < lit}: MATCH when max &lt; lit (and no nulls), MISS when min &gt;= lit
     */
    private static SplitMatch classifyLessThan(String columnName, Object literalValue, SplitStats splitStats) {
        Object min = splitStats.columnMin(columnName);
        Object max = splitStats.columnMax(columnName);
        if (min == null || max == null) {
            return SplitMatch.AMBIGUOUS;
        }
        int minCmp = compareValues(min, literalValue);
        if (minCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        if (minCmp >= 0) {
            return SplitMatch.MISS;
        }
        int maxCmp = compareValues(max, literalValue);
        if (maxCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        if (maxCmp < 0 && hasNoNulls(columnName, splitStats)) {
            return SplitMatch.MATCH;
        }
        return SplitMatch.AMBIGUOUS;
    }

    /**
     * {@code col <= lit}: MATCH when max &lt;= lit (and no nulls), MISS when min &gt; lit
     */
    private static SplitMatch classifyLessThanOrEqual(String columnName, Object literalValue, SplitStats splitStats) {
        Object min = splitStats.columnMin(columnName);
        Object max = splitStats.columnMax(columnName);
        if (min == null || max == null) {
            return SplitMatch.AMBIGUOUS;
        }
        int minCmp = compareValues(min, literalValue);
        if (minCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        if (minCmp > 0) {
            return SplitMatch.MISS;
        }
        int maxCmp = compareValues(max, literalValue);
        if (maxCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        if (maxCmp <= 0 && hasNoNulls(columnName, splitStats)) {
            return SplitMatch.MATCH;
        }
        return SplitMatch.AMBIGUOUS;
    }

    /**
     * {@code col = lit}: MATCH when min == max == lit (and no nulls), MISS when lit outside [min, max]
     */
    private static SplitMatch classifyEquals(String columnName, Object literalValue, SplitStats splitStats) {
        Object min = splitStats.columnMin(columnName);
        Object max = splitStats.columnMax(columnName);
        if (min == null || max == null) {
            return SplitMatch.AMBIGUOUS;
        }
        int minCmp = compareValues(literalValue, min);
        int maxCmp = compareValues(literalValue, max);
        if (minCmp == Integer.MIN_VALUE || maxCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        if (minCmp < 0 || maxCmp > 0) {
            return SplitMatch.MISS;
        }
        if (minCmp == 0 && maxCmp == 0 && hasNoNulls(columnName, splitStats)) {
            return SplitMatch.MATCH;
        }
        return SplitMatch.AMBIGUOUS;
    }

    /**
     * {@code col != lit}: MATCH when min == max and min != lit (and no nulls), MISS when min == max == lit
     */
    private static SplitMatch classifyNotEquals(String columnName, Object literalValue, SplitStats splitStats) {
        Object min = splitStats.columnMin(columnName);
        Object max = splitStats.columnMax(columnName);
        if (min == null || max == null) {
            return SplitMatch.AMBIGUOUS;
        }
        int minCmp = compareValues(literalValue, min);
        int maxCmp = compareValues(literalValue, max);
        if (minCmp == Integer.MIN_VALUE || maxCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        int minMaxCmp = compareValues(min, max);
        if (minMaxCmp == Integer.MIN_VALUE) {
            return SplitMatch.AMBIGUOUS;
        }
        if (minMaxCmp == 0 && minCmp == 0) {
            return SplitMatch.MISS;
        }
        if (minMaxCmp == 0 && minCmp != 0 && hasNoNulls(columnName, splitStats)) {
            return SplitMatch.MATCH;
        }
        return SplitMatch.AMBIGUOUS;
    }

    /**
     * Returns true when the column has zero null values in this split.
     * When null_count is unavailable, assumes nulls may exist (conservative).
     */
    private static boolean hasNoNulls(String columnName, SplitStats splitStats) {
        long nullCount = splitStats.columnNullCount(columnName);
        return nullCount == 0;
    }

    private static String extractColumnName(Expression expr) {
        if (expr instanceof Attribute attr) {
            return attr.name();
        }
        return null;
    }

    private static Object extractLiteralValue(Expression expr) {
        if (expr instanceof Literal lit) {
            return lit.value();
        }
        if (expr.foldable()) {
            return expr.fold(org.elasticsearch.xpack.esql.core.expression.FoldContext.small());
        }
        return null;
    }

    /**
     * Compares two values, returning negative, zero, or positive as with {@link Comparable#compareTo}.
     * Returns {@link Integer#MIN_VALUE} when the values are not comparable (type mismatch or null).
     * Uses exact long comparison for integral types to avoid double precision loss.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static int compareValues(Object a, Object b) {
        if (a == null || b == null) {
            return Integer.MIN_VALUE;
        }
        if (a instanceof BytesRef br) {
            a = br.utf8ToString();
        }
        if (b instanceof BytesRef br) {
            b = br.utf8ToString();
        }
        if (a instanceof Number na && b instanceof Number nb) {
            if (isIntegral(na) && isIntegral(nb)) {
                return Long.compare(na.longValue(), nb.longValue());
            }
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        if (a instanceof Comparable ca && a.getClass() == b.getClass()) {
            return ca.compareTo(b);
        }
        return Integer.MIN_VALUE;
    }

    private static boolean isIntegral(Number n) {
        return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
    }
}
