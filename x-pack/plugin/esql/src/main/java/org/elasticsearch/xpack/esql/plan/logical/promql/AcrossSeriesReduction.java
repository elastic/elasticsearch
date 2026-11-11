/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.HashOffset;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping.WITHOUT;

/**
 * Across-series reduction such as {@code topk}.
 * <p>
 * Like {@link AcrossSeriesAggregate}, it partitions the input series, but unlike it,
 * doesn't aggregate them or change series identity - labels stay as-is, with reduction
 * applied per partition. Partitions never appear in the output.
 */
public final class AcrossSeriesReduction extends PromqlFunctionCall {

    private final Grouping grouping;
    private final List<Attribute> groupings;

    public AcrossSeriesReduction(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters,
        Grouping grouping,
        List<Attribute> groupings
    ) {
        super(source, child, definition, parameters);
        this.grouping = grouping;
        this.groupings = groupings;
    }

    public Grouping grouping() {
        return grouping;
    }

    public List<Attribute> groupings() {
        return groupings;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(groupings) && super.expressionsResolved();
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, AcrossSeriesReduction::new, child(), definition(), parameters(), grouping(), groupings());
    }

    @Override
    public AcrossSeriesReduction replaceChild(LogicalPlan newChild) {
        return new AcrossSeriesReduction(source(), newChild, definition(), parameters(), grouping(), groupings());
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            AcrossSeriesReduction that = (AcrossSeriesReduction) o;
            return grouping == that.grouping && Objects.equals(groupings, that.groupings);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), grouping, groupings);
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.ACROSS_SERIES_REDUCTION;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Partitions series and reduces per partition: a relabel below it feeds this partition boundary.
        return false;
    }

    /**
     * Translates an {@link AcrossSeriesReduction} ({@code topk}/{@code bottomk}/{@code limitk}/{@code limit_ratio}):
     * collapses the child to one row per series, then keeps rows within each step and partition: ranked by value
     * for the order-statistic functions, or an approximate ratio for {@code limit_ratio}.
     * A {@code by} clause only partitions the reduction; it does not change the output schema.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (grouping() == WITHOUT) {
            throw new VerificationException("function [{}] is not yet supported with [{}]", functionName(), WITHOUT.name());
        }

        // Ranking happens per series (see input()); limit_ratio is membership-neutral, so outer partitions are
        // neither required from the child nor materialized below.
        List<String> partitions = PromqlLabels.labelNames(groupings());
        boolean isLimitRatio = definition() == PromqlBuiltinFunctionDefinitions.LIMIT_RATIO;
        IntermediateResult childResult = context.translate(child());
        if (childResult.kind().constant) {
            if (isLimitRatio) {
                // A constant vector's empty label set is a valid identity: sample it directly with an
                // empty key set, without introducing an aggregation.
                LogicalPlan sampled = emitLimitRatioFilter(childResult, List.of());
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
            ? emitLimitRatioFilter(aggregated, aggregated.labels() == null ? List.of() : List.of(aggregated.labels()))
            : emitTopNBy(context, aggregated, partitions, promqlCtx);
        return aggregated.with(result, aggregated.value());
    }

    /** Ranks the already-collapsed per-series rows and keeps the top {@code k} within each step and partition. */
    private LogicalPlan emitLimitRatioFilter(IntermediateResult table, List<Expression> keys) {
        // Validated at analysis (ResolvePromqlFunctions): a foldable numeric non-NaN literal.
        double ratio = ((Number) parameters().getFirst().fold(FoldContext.small())).doubleValue();
        Source source = source();
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

    private LogicalPlan emitTopNBy(
        TranslationContext context,
        IntermediateResult table,
        List<String> partitions,
        PromqlContext promqlContext
    ) {
        var order = (Order) buildEsqlFunction(table.value(), promqlContext);
        // Prometheus converts k with an integer cast: `topk(1.5, v)` keeps one series, and a k below one keeps none.
        Expression k = new ToInteger(source(), new Floor(source(), parameters().getFirst()));
        LogicalPlan plan = table.plan();
        var groupings = new ArrayList<Expression>(List.of(table.step()));
        if (grouping() == AcrossSeriesAggregate.Grouping.BY && partitions.isEmpty() == false && table.labels() != null) {
            // A partition is the series' partition labels; a label absent from every series is one partition, like Prometheus.
            Alias partition = new Alias(
                source(),
                TemporaryNameGenerator.locallyUniqueTemporaryName("partition"),
                new TimeSeriesMetadataFilter(
                    table.labels().source(),
                    table.labels(),
                    PromqlLabels.labelLiterals(table.labels().source(), partitions),
                    false
                )
            );
            plan = new Eval(source(), plan, List.of(partition));
            groupings.add(partition.toAttribute());
        }
        return new TopNBy(source(), plan, order != null ? List.of(order) : List.of(), k, groupings);
    }

}
