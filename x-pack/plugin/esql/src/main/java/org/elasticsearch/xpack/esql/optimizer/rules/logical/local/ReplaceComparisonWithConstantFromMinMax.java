/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.stats.SearchStats;

/**
 * Replace comparisons with constants when min/max statistics prove the comparison is always true or always false.
 * <p>
 * This rule uses shard-level min/max statistics to determine if a comparison can be statically evaluated:
 * <ul>
 *   <li>If the comparison is impossible given the min/max bounds, replace with FALSE</li>
 *   <li>If the comparison is always true AND there are no nulls, replace with TRUE</li>
 *   <li>If the comparison is always true BUT there are nulls, replace with IS NOT NULL</li>
 * </ul>
 * <p>
 * Examples:
 * <ul>
 *   <li>{@code age > 100} where max(age) = 80 → FALSE</li>
 *   <li>{@code age < 100} where max(age) = 80 and count(age) = count() → TRUE</li>
 *   <li>{@code age < 100} where max(age) = 80 and count(age) &lt; count() → age IS NOT NULL</li>
 * </ul>
 */
public class ReplaceComparisonWithConstantFromMinMax extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext ctx) {
        return plan.transformDown(
            Filter.class,
            filter -> filter.transformExpressionsOnly(EsqlBinaryComparison.class, comp -> foldComparison(comp, ctx.searchStats()))
        );
    }

    @SuppressWarnings({ "rawtypes" })
    private Expression foldComparison(EsqlBinaryComparison comp, SearchStats stats) {
        // Only handle comparisons where left is a field and right is a literal
        if (comp.left() instanceof FieldAttribute fa
            && comp.right().foldable()
            && comp.right().fold(null) instanceof Comparable literalComp) {
            DataType fieldType = fa.dataType();
            DataType literalType = comp.right().dataType();

            // date_nanos stats are returned in millis (parsePointAsMillis truncates nanos→millis),
            // and unsigned_long stats are returned as Double (convertUnsignedLongToDouble), neither
            // of which is comparable to the ESQL literal representation for those types.
            if (fieldType == DataType.DATE_NANOS || fieldType == DataType.UNSIGNED_LONG) {
                return comp;
            }

            // Skip optimization if the field and literal have different data types.
            // This can happen when comparing fields across indices with type conflicts,
            // or when comparing incompatible temporal types (e.g., DATETIME field vs DATE_NANOS literal).
            // In such cases, the stats may not be in the same domain as the literal.
            if (fieldType != literalType) {
                return comp;
            }
            // Get min/max statistics
            Object min = stats.min(fa.fieldName());
            Object max = stats.max(fa.fieldName());

            // If we don't have statistics, we can't optimize
            if (min == null || max == null) {
                return comp;
            }

            // Ensure all values are comparable
            if (min instanceof Comparable minComp && max instanceof Comparable maxComp) {
                // Coerce numeric stats values to match the literal's type to avoid ClassCastException
                // when compareTo is called between mismatched Number subtypes (e.g. Integer vs Long).
                // This can happen when the field's Lucene point reader returns Integer for a field
                // that ESQL treats as Long (e.g. languages.long).
                if (literalComp instanceof Number literalNum && min instanceof Number minNumber && max instanceof Number maxNumber) {
                    Number coercedMin = coerceToSameNumericType(literalNum, minNumber);
                    Number coercedMax = coerceToSameNumericType(literalNum, maxNumber);
                    if (coercedMin == null || coercedMax == null) {
                        return comp;
                    }
                    minComp = (Comparable) coercedMin;
                    maxComp = (Comparable) coercedMax;
                }

                // Determine if the comparison is always true or always false
                Boolean result = evaluateComparison(comp, minComp, maxComp, literalComp);

                if (result == null) {
                    // Cannot determine statically
                    return comp;
                }

                long fieldCount = stats.count(fa.fieldName());
                long totalCount = stats.count();

                // If counts are unavailable or there are nulls, we cannot safely fold the comparison
                // to TRUE or FALSE because in 3VL (Three-Valued Logic), NULL comparisons yield UNKNOWN.
                // An UNKNOWN value inside a NOT() or OR() behaves differently from TRUE or FALSE.
                if (totalCount < 0 || fieldCount < totalCount) {
                    return comp;
                }

                if (result == false) {
                    // Comparison is always false, and there are no nulls
                    return Literal.of(comp, false);
                }

                // Comparison is always true, and there are no nulls
                return Literal.of(comp, true);
            }
        }

        return comp;
    }

    /**
     * Coerces a stats numeric value to the same concrete Number subtype as the literal,
     * so that {@link Comparable#compareTo} does not throw {@link ClassCastException}.
     * For example, Lucene may return {@link Integer} for a field that ESQL treats as {@link Long}.
     *
     * @return the coerced value, or {@code null} if the coercion is not supported
     */
    private static Number coerceToSameNumericType(Number literal, Number statsValue) {
        if (literal instanceof Long) {
            return statsValue.longValue();
        } else if (literal instanceof Integer) {
            return statsValue.intValue();
        } else if (literal instanceof Double) {
            return statsValue.doubleValue();
        } else if (literal instanceof Float) {
            return statsValue.floatValue();
        }
        // Unsupported numeric type – caller will skip the optimization
        return null;
    }

    /**
     * Evaluate if a comparison is always true, always false, or indeterminate.
     *
     * @param comp the comparison expression
     * @param min the minimum value from statistics
     * @param max the maximum value from statistics
     * @param literal the literal value being compared
     * @return TRUE if always true, FALSE if always false, null if indeterminate
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Boolean evaluateComparison(EsqlBinaryComparison comp, Comparable min, Comparable max, Comparable literal) {
        switch (comp) {
            case GreaterThan ignored -> {
                // field > literal
                // Always false if max <= literal
                if (max.compareTo(literal) <= 0) {
                    return false;
                }
                // Always true if min > literal
                if (min.compareTo(literal) > 0) {
                    return true;
                }
            }
            case GreaterThanOrEqual ignored -> {
                // field >= literal
                // Always false if max < literal
                if (max.compareTo(literal) < 0) {
                    return false;
                }
                // Always true if min >= literal
                if (min.compareTo(literal) >= 0) {
                    return true;
                }
            }
            case LessThan ignored -> {
                // field < literal
                // Always false if min >= literal
                if (min.compareTo(literal) >= 0) {
                    return false;
                }
                // Always true if max < literal
                if (max.compareTo(literal) < 0) {
                    return true;
                }
            }
            case LessThanOrEqual ignored -> {
                // field <= literal
                // Always false if min > literal
                if (min.compareTo(literal) > 0) {
                    return false;
                }
                // Always true if max <= literal
                if (max.compareTo(literal) <= 0) {
                    return true;
                }
            }
            case Equals ignored -> {
                // field == literal
                // Always false if literal is outside [min, max]
                if (literal.compareTo(min) < 0 || literal.compareTo(max) > 0) {
                    return false;
                }
                // Always true if min == max == literal
                if (min.compareTo(literal) == 0 && max.compareTo(literal) == 0) {
                    return true;
                }
            }
            case NotEquals ignored -> {
                // field != literal
                // Always true if literal is outside [min, max]
                if (literal.compareTo(min) < 0 || literal.compareTo(max) > 0) {
                    return true;
                }
                // Always false if min == max == literal
                if (min.compareTo(literal) == 0 && max.compareTo(literal) == 0) {
                    return false;
                }
            }
            default -> {
                return null;
            }
        }

        // Cannot determine statically
        return null;
    }
}
