/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reorders filter conjuncts by estimated selectivity and cost using column statistics
 * from source metadata. Most selective predicates (those eliminating the most rows) are
 * placed first so that downstream readers can skip data earlier.
 * <p>
 * Scoring:
 * <ul>
 *   <li>Primary: estimated selectivity from min/max range width vs predicate value (lower = more selective = evaluated first)</li>
 *   <li>Secondary: column size_bytes — smaller column is cheaper to evaluate</li>
 *   <li>Fallback: original order preserved for predicates without statistics</li>
 * </ul>
 */
public final class FilterEvaluationOrderEstimator {

    static final double UNKNOWN_SELECTIVITY = 0.5;
    private static final long UNKNOWN_SIZE = Long.MAX_VALUE;

    private FilterEvaluationOrderEstimator() {}

    /**
     * Returns the conjuncts reordered so the most selective (cheapest) predicates come first.
     * If metadata is null/empty, contains a single predicate, or all conjuncts score equally,
     * returns the original list unchanged.
     */
    public static List<Expression> orderByEstimatedCost(List<Expression> conjuncts, Map<String, Object> sourceMetadata) {
        if (conjuncts == null || conjuncts.size() <= 1 || sourceMetadata == null || sourceMetadata.isEmpty()) {
            return conjuncts;
        }

        Object rcValue = sourceMetadata.get(SourceStatisticsSerializer.STATS_ROW_COUNT);
        if (rcValue instanceof Number == false) {
            return conjuncts;
        }
        long rowCount = ((Number) rcValue).longValue();
        if (rowCount <= 0) {
            return conjuncts;
        }

        List<ScoredExpression> scored = new ArrayList<>(conjuncts.size());
        for (int i = 0; i < conjuncts.size(); i++) {
            Expression expr = conjuncts.get(i);
            double selectivity = estimateSelectivity(expr, sourceMetadata, rowCount);
            long sizeBytes = estimateColumnSize(expr, sourceMetadata);
            scored.add(new ScoredExpression(expr, selectivity, sizeBytes, i));
        }

        scored.sort(
            Comparator.comparingDouble((ScoredExpression s) -> s.selectivity)
                .thenComparingLong(s -> s.sizeBytes)
                .thenComparingInt(s -> s.originalIndex)
        );

        boolean changed = false;
        for (int i = 0; i < scored.size(); i++) {
            if (scored.get(i).originalIndex != i) {
                changed = true;
                break;
            }
        }
        if (changed == false) {
            return conjuncts;
        }

        List<Expression> result = new ArrayList<>(scored.size());
        for (ScoredExpression s : scored) {
            result.add(s.expression);
        }
        return result;
    }

    private record ScoredExpression(Expression expression, double selectivity, long sizeBytes, int originalIndex) {}

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static double estimateSelectivity(Expression expr, Map<String, Object> sourceMetadata, long rowCount) {
        if (expr instanceof Equals eq) {
            String col = columnName(eq.left());
            if (col != null && eq.right().foldable()) {
                return estimateEqualitySelectivity(col, foldValue(eq.right()), sourceMetadata);
            }
        } else if (expr instanceof GreaterThan gt) {
            String col = columnName(gt.left());
            if (col != null && gt.right().foldable()) {
                return estimateRangeSelectivity(col, foldValue(gt.right()), true, sourceMetadata);
            }
        } else if (expr instanceof GreaterThanOrEqual gte) {
            String col = columnName(gte.left());
            if (col != null && gte.right().foldable()) {
                return estimateRangeSelectivity(col, foldValue(gte.right()), true, sourceMetadata);
            }
        } else if (expr instanceof LessThan lt) {
            String col = columnName(lt.left());
            if (col != null && lt.right().foldable()) {
                return estimateRangeSelectivity(col, foldValue(lt.right()), false, sourceMetadata);
            }
        } else if (expr instanceof LessThanOrEqual lte) {
            String col = columnName(lte.left());
            if (col != null && lte.right().foldable()) {
                return estimateRangeSelectivity(col, foldValue(lte.right()), false, sourceMetadata);
            }
        } else if (expr instanceof In in) {
            String col = columnName(in.value());
            if (col != null) {
                int count = 0;
                for (Expression e : in.list()) {
                    if (e.foldable()) {
                        count++;
                    }
                }
                return estimateInSelectivity(col, count, sourceMetadata);
            }
        } else if (expr instanceof IsNull isNull) {
            String col = columnName(isNull.field());
            if (col != null) {
                return estimateNullSelectivity(col, sourceMetadata, rowCount);
            }
        } else if (expr instanceof IsNotNull isNotNull) {
            String col = columnName(isNotNull.field());
            if (col != null) {
                double nullSel = estimateNullSelectivity(col, sourceMetadata, rowCount);
                return 1.0 - nullSel;
            }
        }

        return UNKNOWN_SELECTIVITY;
    }

    /**
     * Folds a foldable expression to its runtime value using a small allocator.
     */
    private static Object foldValue(Expression expr) {
        return expr.fold(FoldContext.small());
    }

    /**
     * Returns the column name if the expression is a field or reference attribute, null otherwise.
     */
    private static String columnName(Expression expr) {
        if (expr instanceof NamedExpression ne) {
            return Expressions.name(ne);
        }
        return null;
    }

    /**
     * Collects all column names referenced by an expression.
     */
    private static Set<String> collectColumnNames(Expression expr) {
        Set<String> names = new LinkedHashSet<>();
        expr.forEachDown(Attribute.class, attr -> {
            if (attr instanceof FieldAttribute || attr instanceof ReferenceAttribute) {
                names.add(Expressions.name(attr));
            }
        });
        return names;
    }

    // -- selectivity estimators --

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static double estimateEqualitySelectivity(String colName, Object value, Map<String, Object> sourceMetadata) {
        Object min = sourceMetadata.get(SourceStatisticsSerializer.columnMinKey(colName));
        Object max = sourceMetadata.get(SourceStatisticsSerializer.columnMaxKey(colName));
        if (min == null || max == null) {
            return UNKNOWN_SELECTIVITY;
        }
        double rangeWidth = numericRangeWidth(min, max);
        if (Double.isNaN(rangeWidth)) {
            return UNKNOWN_SELECTIVITY;
        }
        if (rangeWidth <= 0) {
            if (min instanceof Comparable minC && value instanceof Comparable valC) {
                return minC.compareTo(valC) == 0 ? 1.0 : 0.0;
            }
            return UNKNOWN_SELECTIVITY;
        }
        if (min instanceof Comparable minC && max instanceof Comparable maxC && value instanceof Comparable valC) {
            if (minC.compareTo(valC) > 0 || maxC.compareTo(valC) < 0) {
                return 0.0;
            }
        }
        return Math.min(1.0, 1.0 / rangeWidth);
    }

    private static double estimateRangeSelectivity(
        String colName,
        Object value,
        boolean isGreaterThan,
        Map<String, Object> sourceMetadata
    ) {
        Object min = sourceMetadata.get(SourceStatisticsSerializer.columnMinKey(colName));
        Object max = sourceMetadata.get(SourceStatisticsSerializer.columnMaxKey(colName));
        if (min == null || max == null) {
            return UNKNOWN_SELECTIVITY;
        }
        double minD = toNumeric(min);
        double maxD = toNumeric(max);
        double valD = toNumeric(value);
        if (Double.isNaN(minD) || Double.isNaN(maxD) || Double.isNaN(valD)) {
            return UNKNOWN_SELECTIVITY;
        }
        double rangeWidth = maxD - minD;
        if (rangeWidth <= 0) {
            return UNKNOWN_SELECTIVITY;
        }
        double fraction;
        if (isGreaterThan) {
            fraction = (maxD - valD) / rangeWidth;
        } else {
            fraction = (valD - minD) / rangeWidth;
        }
        return clamp(fraction);
    }

    private static double estimateInSelectivity(String colName, int listSize, Map<String, Object> sourceMetadata) {
        Object min = sourceMetadata.get(SourceStatisticsSerializer.columnMinKey(colName));
        Object max = sourceMetadata.get(SourceStatisticsSerializer.columnMaxKey(colName));
        if (min == null || max == null) {
            return UNKNOWN_SELECTIVITY;
        }
        double rangeWidth = numericRangeWidth(min, max);
        if (Double.isNaN(rangeWidth) || rangeWidth <= 0) {
            return UNKNOWN_SELECTIVITY;
        }
        return Math.min(1.0, listSize / rangeWidth);
    }

    private static double estimateNullSelectivity(String colName, Map<String, Object> sourceMetadata, long rowCount) {
        Object ncValue = sourceMetadata.get(SourceStatisticsSerializer.columnNullCountKey(colName));
        if (ncValue instanceof Number nc) {
            return clamp((double) nc.longValue() / rowCount);
        }
        return UNKNOWN_SELECTIVITY;
    }

    /**
     * Returns the column size in bytes for the smallest column referenced by the expression,
     * or {@link #UNKNOWN_SIZE} if unavailable.
     */
    private static long estimateColumnSize(Expression expr, Map<String, Object> sourceMetadata) {
        Set<String> columns = collectColumnNames(expr);
        long minSize = UNKNOWN_SIZE;
        for (String col : columns) {
            Object sbValue = sourceMetadata.get(SourceStatisticsSerializer.columnSizeBytesKey(col));
            if (sbValue instanceof Number n) {
                minSize = Math.min(minSize, n.longValue());
            }
        }
        return minSize;
    }

    // -- arithmetic helpers --

    /**
     * Returns the numeric range width (max - min) or NaN if either value is non-numeric.
     */
    private static double numericRangeWidth(Object min, Object max) {
        double minD = toNumeric(min);
        double maxD = toNumeric(max);
        if (Double.isNaN(minD) || Double.isNaN(maxD)) {
            return Double.NaN;
        }
        return maxD - minD;
    }

    /**
     * Converts a value to its numeric representation. Returns NaN for non-numeric types.
     */
    private static double toNumeric(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        return Double.NaN;
    }

    private static double clamp(double value) {
        return Math.max(0.0, Math.min(1.0, value));
    }
}
