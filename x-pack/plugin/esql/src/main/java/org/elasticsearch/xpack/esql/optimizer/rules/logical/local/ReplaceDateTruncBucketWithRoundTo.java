/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
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
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTime;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateWithTypeToString;

public class ReplaceDateTruncBucketWithRoundTo extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    private static final Logger logger = LogManager.getLogger(ReplaceDateTruncBucketWithRoundTo.class);

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext context) {
        if (context.searchStats() == null) {
            return plan;
        }
        final TransportVersion minVersion = minVersion(context);
        return plan.transformUp(Eval.class, eval -> substitute(eval, context.searchStats(), minVersion));
    }

    private static TransportVersion minVersion(LocalLogicalOptimizerContext context) {
        try {
            return context.minimumVersion();
        } catch (UnsupportedOperationException e) {
            // data nodes don't propagate the minimum cluster version; assume current
            return TransportVersion.current();
        }
    }

    private LogicalPlan substitute(Eval eval, SearchStats searchStats, TransportVersion minVersion) {
        // check the filter in children plans
        return eval.transformExpressionsOnly(Function.class, f -> substitute(f, eval, searchStats, minVersion));
    }

    /**
     * Perform the actual substitution with {@code SearchStats} and predicates in the query.
     */
    private Expression substitute(Expression e, Eval eval, SearchStats searchStats, TransportVersion minVersion) {
        RoundTo roundTo = null;
        if (e instanceof DateTrunc dateTrunc) {
            roundTo = maybeToRoundTo(
                dateTrunc.source(),
                dateTrunc.field(),
                dateTrunc.interval(),
                searchStats,
                eval,
                Rounding.RoundingConvention.DOWN,
                (interval, minValue, maxValue) -> DateTrunc.createRounding(interval, dateTrunc.zoneId(), minValue, maxValue)
            );
        } else if (e instanceof Bucket bucket) {
            Rounding.RoundingConvention convention = bucket.roundingConfiguration();
            if (convention != Rounding.RoundingConvention.DOWN && minVersion.supports(RoundTo.ESQL_ROUND_TO_CONVENTION) == false) {
                return e;
            }
            roundTo = maybeToRoundTo(
                bucket.source(),
                bucket.field(),
                bucket.buckets(),
                searchStats,
                eval,
                convention,
                (interval, minValue, maxValue) -> bucket.getDateRounding(FoldContext.small(), minValue, maxValue)
            );
        }
        return roundTo != null ? roundTo : e;
    }

    private RoundTo maybeToRoundTo(
        Source source,
        Expression field,
        Expression foldableTimeExpression,
        SearchStats searchStats,
        Eval eval,
        Rounding.RoundingConvention convention,
        TriFunction<Object, Long, Long, Rounding.Prepared> roundingFunction
    ) {
        if (field instanceof FieldAttribute fa && fa.field() instanceof UnionTypeEsField == false && isDateTime(fa.dataType())) {
            DataType fieldType = fa.dataType();
            FieldAttribute.FieldName fieldName = fa.fieldName();
            MinMax minMax = minMax(fieldName, searchStats, eval, field);
            // If min/max is available create rounding with them
            if (minMax != null && foldableTimeExpression.foldable()) {
                Object foldedInterval = foldableTimeExpression.fold(FoldContext.small() /* TODO remove me */);
                Rounding.Prepared rounding = roundingFunction.apply(foldedInterval, minMax.min, minMax.max);
                RoundTo roundTo = createRoundTo(source, field, rounding, convention);
                if (roundTo == null) {
                    logger.trace(
                        "Fixed rounding point is null for field {}, minValue {} in string format {} and maxValue {} in string format {}",
                        fieldName,
                        minMax.min,
                        dateWithTypeToString(minMax.min, fieldType),
                        minMax.max,
                        dateWithTypeToString(minMax.max, fieldType)
                    );
                    return null;
                }
                return roundTo;
            }
        }
        return null;
    }

    private MinMax minMax(FieldAttribute.FieldName fieldName, SearchStats searchStats, Eval eval, Expression field) {
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
        return min != null && max != null && min <= max ? new MinMax(min, max) : null;
    }

    private record MinMax(long min, long max) {}

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

    private static RoundTo createRoundTo(
        Source source,
        Expression field,
        Rounding.Prepared rounding,
        Rounding.RoundingConvention convention
    ) {
        long[] roundingPoints = rounding.fixedRoundingPoints();
        if (roundingPoints == null) {
            return null;
        }
        DataType fieldType = field.dataType();
        List<Expression> literals = new ArrayList<>(roundingPoints.length);
        for (long p : roundingPoints) {
            literals.add(new Literal(Source.EMPTY, p, fieldType));
        }
        return new RoundTo(source, field, literals, convention);
    }
}
