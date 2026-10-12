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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.HashOffset;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping.WITHOUT;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.union;

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

        // Ranking happens per series, so the child stays at series grain whatever the enclosing translation regroups
        // by; the partition labels must be exposed to rank within them. limit_ratio is membership-neutral:
        // its sampling key is solely the input vector's identity, so outer partitions are neither required
        // from the child nor materialized below.
        List<String> partitions = TranslationContext.mapFinite(groupings());
        boolean isLimitRatio = definition() == PromqlBuiltinFunctionDefinitions.LIMIT_RATIO;
        TranslationSchema childRequired = isLimitRatio
            ? union(context.required(), open())
            : union(union(context.required(), open()), finite(partitions));
        IntermediateResult childResult = context.withRequired(childRequired).translate(child());
        if (childResult.kind().constant) {
            if (isLimitRatio) {
                // A constant vector's empty label set is a valid identity: sample it directly with an
                // empty key set, without introducing an aggregation.
                LogicalPlan sampled = emitLimitRatioFilter(childResult);
                return childResult.with(sampled, childResult.schema(), childResult.value());
            }
            return childResult;
        }

        var schema = isLimitRatio ? childResult.schema() : union(childResult.schema(), finite(partitions));

        var promqlCtx = new PromqlContext(context.time(), AggregateFunction.NO_WINDOW, childResult.step(), context.configuration());
        IntermediateResult aggregated = childResult.kind().afterInitialAggregation
            ? context.regroup(childResult, schema, false, childResult.value())
            : context.collapse(childResult, schema, childResult.value());
        LogicalPlan result = isLimitRatio ? emitLimitRatioFilter(aggregated) : emitTopNBy(context, aggregated, partitions, promqlCtx);
        return aggregated.with(result, aggregated.schema(), aggregated.value());
    }

    /** Ranks the already-collapsed per-series rows and keeps the top {@code k} within each step and partition. */
    private LogicalPlan emitTopNBy(
        TranslationContext context,
        IntermediateResult table,
        List<String> partitions,
        PromqlContext promqlContext
    ) {
        ReductionGrouping grouping = reductionGrouping(context, table, partitions);
        var order = (Order) buildEsqlFunction(table.value(), promqlContext);
        // Prometheus converts k with an integer cast: `topk(1.5, v)` keeps one series, and a k below one keeps none.
        Expression k = new ToInteger(source(), new Floor(source(), parameters().getFirst()));
        return new TopNBy(source(), grouping.plan(), order != null ? List.of(order) : List.of(), k, grouping.groupings());
    }

    /**
     * Keeps an approximate {@code ratio} of the already-collapsed per-series rows with a plain
     * {@link Filter} over the internal {@link HashOffset} sampling offset, so no sort order is
     * built and no dedicated plan node or execution operator is needed. The sampling key is
     * membership-neutral: solely the input identity (the {@code _timeseries} blob at series
     * grain, else the surviving grouping labels or packed label sets, with packing-covered
     * labels dropped and the rest ordered by name so key order cannot change the hash).
     * Outer {@code by} partition labels are neither hashed nor materialized as null columns. The filter sits
     * above the aggregation producing its keys, which the optimizer cannot push past.
     */
    private LogicalPlan emitLimitRatioFilter(IntermediateResult table) {
        // The sampling key is the input identity from below. At series grain that is the
        // _timeseries blob; over an aggregated input the rows are groups, so their own grain
        // labels are the key (for example pod and cluster groups for limit_ratio over sum by).
        // With no key columns every row shares one identity, so a single-series result is kept
        // or dropped deterministically.
        List<Expression> keys = new ArrayList<>();
        Attribute series = table.plan().output().stream().filter(MetadataAttribute::isTimeSeriesAttribute).findFirst().orElse(null);
        if (series != null) {
            addIfMissing(keys, series);
        } else {
            // No series blob: the rows are groups. Their identity is the concrete grouping
            // underneath -- packed label sets when the schema packs labels away (for example
            // sum without), else the grain label columns. Packings hold only dimensions, never
            // the step, so the identity is stable across steps.
            var resolvedSkips = new ArrayList<Set<String>>();
            for (Set<String> skip : TranslationContext.finestFirst(table.schema().skips())) {
                Attribute packing = table.packed(skip);
                if (packing != null) {
                    addIfMissing(keys, packing);
                    resolvedSkips.add(skip);
                }
            }
            // A finite label carried inside any packing adds no identity: the packing already
            // determines it (for example pod inside _timeseries$region). The surviving labels
            // sort by name so grouping-key order cannot change the hashed bytes.
            table.schema()
                .labels()
                .stream()
                .filter(label -> resolvedSkips.stream().allMatch(skip -> skip.contains(label)))
                .sorted()
                .forEach(label -> {
                    Attribute carrier = table.label(label);
                    // Guaranteed by emitRegroup, which resolves every schema label (null-filling missing ones).
                    assert carrier != null : "invariant: grouping label [" + label + "] must be carried by the input";
                    addIfMissing(keys, carrier);
                });
        }
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

    private static void addIfMissing(List<Expression> key, Attribute carrier) {
        if (key.stream().noneMatch(e -> e instanceof Attribute a && a.id().equals(carrier.id()))) {
            key.add(carrier);
        }
    }

    /**
     * The grouping a reduction keeps rows within: the step bucket plus one carrier per {@code by} partition label.
     * A partition label absent from every series ranks as one partition, like Prometheus, via a null-carrying
     * {@link Eval} over the collapsed table.
     */
    private ReductionGrouping reductionGrouping(TranslationContext context, IntermediateResult table, List<String> partitions) {
        var groupings = new ArrayList<Expression>();
        groupings.add(table.step());
        LogicalPlan plan = table.plan();
        if (grouping() == AcrossSeriesAggregate.Grouping.BY) {
            var nulls = new ArrayList<Alias>();
            for (String partition : partitions) {
                Attribute carrier = table.label(partition);
                if (carrier == null) {
                    // a partition label absent from every series ranks as one partition, like Prometheus
                    nulls.add(TranslationContext.emitNullExpression(TranslationContext.mapToRef(partition)));
                    carrier = nulls.getLast().toAttribute();
                }
                groupings.add(carrier);
            }
            if (nulls.isEmpty() == false) {
                plan = new Eval(context.cmd().source(), plan, nulls);
            }
        }
        return new ReductionGrouping(plan, groupings);
    }

    private record ReductionGrouping(LogicalPlan plan, List<Expression> groupings) {}
}
