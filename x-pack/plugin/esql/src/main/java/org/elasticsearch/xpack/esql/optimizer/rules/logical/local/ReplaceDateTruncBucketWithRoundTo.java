/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTime;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateWithTypeToString;

public class ReplaceDateTruncBucketWithRoundTo extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    private static final Logger logger = LogManager.getLogger(ReplaceDateTruncBucketWithRoundTo.class);

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext context) {
        return context.searchStats() != null ? plan.transformUp(Eval.class, eval -> substitute(eval, context.searchStats())) : plan;
    }

    private LogicalPlan substitute(Eval eval, SearchStats searchStats) {
        // check the filter in children plans
        return eval.transformExpressionsOnly(Function.class, f -> substitute(f, eval, searchStats));
    }

    /**
     * Perform the actual substitution with {@code SearchStats} and predicates in the query.
     */
    private Expression substitute(Expression e, Eval eval, SearchStats searchStats) {
        Expression roundTo = null;
        if (e instanceof DateTrunc dateTrunc) {
            roundTo = maybeSubstituteWithRoundTo(
                dateTrunc.source(),
                dateTrunc.field(),
                dateTrunc.interval(),
                searchStats,
                eval,
                (interval, minValue, maxValue) -> DateTrunc.createRounding(interval, dateTrunc.zoneId(), minValue, maxValue)
            );
        } else if (e instanceof Bucket bucket) {
            roundTo = maybeSubstituteWithRoundTo(
                bucket.source(),
                bucket.field(),
                bucket.buckets(),
                searchStats,
                eval,
                (interval, minValue, maxValue) -> bucket.getDateRounding(FoldContext.small(), minValue, maxValue)
            );
        }
        return roundTo != null ? roundTo : e;
    }

    private RoundTo maybeSubstituteWithRoundTo(
        Source source,
        Expression field,
        Expression foldableTimeExpression,
        SearchStats searchStats,
        Eval eval,
        TriFunction<Object, Long, Long, Rounding.Prepared> roundingFunction
    ) {
        if (field instanceof FieldAttribute fa && fa.field() instanceof MultiTypeEsField == false && isDateTime(fa.dataType())) {
            DataType fieldType = fa.dataType();
            FieldAttribute.FieldName fieldName = fa.fieldName();
            // Extract min/max from SearchStats
            Object minFromSearchStats = searchStats.min(fieldName);
            Object maxFromSearchStats = searchStats.max(fieldName);
            Long min = toLong(minFromSearchStats);
            Long max = toLong(maxFromSearchStats);
            // Extract min/max from query
            Tuple<Long, Long> minMaxFromPredicates = minMaxFromPredicates(predicates(eval, field));
            Long minFromPredicates = minMaxFromPredicates.v1();
            Long maxFromPredicates = minMaxFromPredicates.v2();
            // Consolidate min/max from SearchStats and query
            if (minFromPredicates != null) {
                min = min != null ? Math.max(min, minFromPredicates) : minFromPredicates;
            }
            if (maxFromPredicates != null) {
                max = max != null ? Math.min(max, maxFromPredicates) : maxFromPredicates;
            }
            // If min/max is available create rounding with them
            if (min != null && max != null && foldableTimeExpression.foldable() && min <= max) {
                Object foldedInterval = foldableTimeExpression.fold(FoldContext.small() /* TODO remove me */);
                Rounding.Prepared rounding = roundingFunction.apply(foldedInterval, min, max);
                long[] roundingPoints = rounding.fixedRoundingPoints();
                if (roundingPoints == null) {
                    logger.trace(
                        "Fixed rounding point is null for field {}, minValue {} in string format {} and maxValue {} in string format {}",
                        fieldName,
                        min,
                        dateWithTypeToString(min, fieldType),
                        max,
                        dateWithTypeToString(max, fieldType)
                    );
                    return null;
                }
                // Convert to round_to function with the roundings
                List<Expression> points = Arrays.stream(roundingPoints)
                    .mapToObj(l -> new Literal(Source.EMPTY, l, fieldType))
                    .collect(Collectors.toList());
                return new RoundTo(source, field, points);
            }
        }
        return null;
    }

    private List<EsqlBinaryComparison> predicates(Eval eval, Expression field) {
        List<EsqlBinaryComparison> binaryComparisons = new ArrayList<>();
        eval.forEachUp(Filter.class, filter -> {
            Expression condition = filter.condition();
            if (condition instanceof And and) {
                Predicates.splitAnd(and).forEach(e -> addBinaryComparisonOnField(e, field, binaryComparisons));
            } else {
                addBinaryComparisonOnField(condition, field, binaryComparisons);
            }
        });
        return binaryComparisons;
    }

    private void addBinaryComparisonOnField(Expression expression, Expression field, List<EsqlBinaryComparison> binaryComparisons) {
        if (expression instanceof EsqlBinaryComparison esqlBinaryComparison
            && esqlBinaryComparison.right().foldable()
            && esqlBinaryComparison.left().semanticEquals(field)) {
            binaryComparisons.add(esqlBinaryComparison);
        }
    }

    private Tuple<Long, Long> minMaxFromPredicates(List<EsqlBinaryComparison> binaryComparisons) {
        long[] min = new long[] { Long.MIN_VALUE };
        long[] max = new long[] { Long.MAX_VALUE };
        Holder<Boolean> foundMinValue = new Holder<>(false);
        Holder<Boolean> foundMaxValue = new Holder<>(false);
        for (EsqlBinaryComparison binaryComparison : binaryComparisons) {
            if (binaryComparison.right() instanceof Literal l && isDateTime(l.dataType())) {
                Long value = toLong(l.value());
                if (binaryComparison instanceof Equals) {
                    return new Tuple<>(value, value);
                }
                if (binaryComparison instanceof GreaterThan || binaryComparison instanceof GreaterThanOrEqual) {
                    if (value >= min[0]) {
                        min[0] = value;
                        foundMinValue.set(true);
                    }
                } else if (binaryComparison instanceof LessThan || binaryComparison instanceof LessThanOrEqual) {
                    if (value <= max[0]) {
                        max[0] = value;
                        foundMaxValue.set(true);
                    }
                }
            }
        }
        return new Tuple<>(foundMinValue.get() ? min[0] : null, foundMaxValue.get() ? max[0] : null);
    }

    private Long toLong(Object value) {
        return value instanceof Long l ? l : null;
    }
}
