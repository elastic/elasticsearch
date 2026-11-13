/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.promql.function.HashOffset;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2.IntermediateResult;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping.WITHOUT;

/**
 * Translates the PromQL reductions across series ({@code topk}, {@code bottomk}, {@code limitk}, {@code limit_ratio}) over a
 * label container: they select series and keep their labels, partitioning by the listed labels of each series.
 */
final class AcrossSeriesReductionV2 {

    private AcrossSeriesReductionV2() {}

    static IntermediateResult translate(AcrossSeriesReduction reduction, TranslationContextV2 context) {
        if (reduction.grouping() == WITHOUT) {
            throw new VerificationException("function [{}] is not yet supported with [{}]", reduction.functionName(), WITHOUT.name());
        }

        // Ranking happens per series; limit_ratio is membership-neutral, so outer partitions are neither required from the
        // child nor materialized below.
        List<String> partitions = PromqlLabels.labelNames(reduction.groupings());
        boolean isLimitRatio = reduction.definition() == PromqlBuiltinFunctionDefinitions.LIMIT_RATIO;
        IntermediateResult childResult = context.translate(reduction.child());
        if (childResult.kind().constant) {
            if (isLimitRatio) {
                // A constant vector's empty label set is a valid identity: sample it directly with an
                // empty key set, without introducing an aggregation.
                LogicalPlan sampled = emitLimitRatioFilter(reduction, childResult, List.of());
                return childResult.with(sampled, childResult.value());
            }
            return childResult;
        }

        var promqlCtx = new PromqlContext(context.time(), AggregateFunction.NO_WINDOW, childResult.step(), context.configuration());
        // Ranking reads every series of the child: a raw child collapses to one row per series, a table already is one.
        IntermediateResult aggregated = childResult.kind().afterInitialAggregation
            ? childResult
            : context.emitInitialAggregate(childResult, childResult.value());
        LogicalPlan result = isLimitRatio
            ? emitLimitRatioFilter(reduction, aggregated, aggregated.labels() == null ? List.of() : List.of(aggregated.labels()))
            : emitTopNBy(reduction, aggregated, partitions, promqlCtx);
        return aggregated.with(result, aggregated.value());
    }

    /** Keeps the series whose hash offset of {@code keys} falls under the ratio. */
    private static LogicalPlan emitLimitRatioFilter(AcrossSeriesReduction reduction, IntermediateResult table, List<Expression> keys) {
        // Validated at analysis (ResolvePromqlFunctions): a foldable numeric non-NaN literal.
        double ratio = ((Number) reduction.parameters().getFirst().fold(FoldContext.small())).doubleValue();
        Source source = reduction.source();
        if (Double.isNaN(ratio) || ratio == 0.0) {
            // No offset falls below zero (and NaN comparisons are always false): keep nothing.
            return new Filter(source, table.plan(), Literal.FALSE);
        }
        if (ratio >= 1.0 || ratio <= -1.0) {
            // Every offset falls below ratios at or above one, and at or above the non-positive
            // complement threshold of ratios at or below minus one: keep everything.
            return table.plan();
        }
        Expression offset = new HashOffset(source, keys);
        if (ratio > 0) {
            return new Filter(source, table.plan(), new LessThan(source, offset, new Literal(source, ratio, DataType.DOUBLE)));
        }
        return new Filter(source, table.plan(), new GreaterThanOrEqual(source, offset, new Literal(source, 1.0 + ratio, DataType.DOUBLE)));
    }

    /** Ranks the already-collapsed per-series rows and keeps the top {@code k} within each step and partition. */
    private static LogicalPlan emitTopNBy(
        AcrossSeriesReduction reduction,
        IntermediateResult table,
        List<String> partitions,
        PromqlContext promqlContext
    ) {
        Source source = reduction.source();
        var order = (Order) reduction.buildEsqlFunction(table.value(), promqlContext);
        // Prometheus converts k with an integer cast: `topk(1.5, v)` keeps one series, and a k below one keeps none.
        Expression k = new ToInteger(source, new Floor(source, reduction.parameters().getFirst()));
        LogicalPlan plan = table.plan();
        var groupings = new ArrayList<Expression>(List.of(table.step()));
        if (reduction.grouping() == AcrossSeriesAggregate.Grouping.BY && partitions.isEmpty() == false && table.labels() != null) {
            // A partition is the series' partition labels; a label absent from every series is one partition, like Prometheus.
            Alias partition = new Alias(
                source,
                TemporaryNameGenerator.locallyUniqueTemporaryName("partition"),
                new TimeSeriesMetadataFilter(
                    table.labels().source(),
                    table.labels(),
                    PromqlLabels.labelLiterals(table.labels().source(), partitions),
                    false
                )
            );
            plan = new Eval(source, plan, List.of(partition));
            groupings.add(partition.toAttribute());
        }
        return new TopNBy(source, plan, order != null ? List.of(order) : List.of(), k, groupings);
    }
}
