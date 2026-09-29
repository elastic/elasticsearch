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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping.WITHOUT;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.promoted;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.rest;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.union;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.deliveredRequirement;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.emitNullExpression;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.find;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.mapPromoted;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.mapToRef;

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
     * Translates an {@link AcrossSeriesReduction} ({@code topk}/{@code bottomk}): collapse the child to one row
     * per series, then rank and keep the top {@code k}. A {@code by} clause only partitions the ranking; it does
     * not change the exposed labels.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (grouping() == WITHOUT) {
            throw new VerificationException("function [{}] is not yet supported with [{}]", functionName(), WITHOUT.name());
        }

        // Ranking happens per series, so the child stays at series grain whatever the enclosing translation regroups
        // by; the partition labels must be exposed to rank within them.
        List<String> partitions = mapPromoted(groupings());
        // The child requirement already carries the partitions alongside the whole rest.
        TranslationConstraint childRequired = union(union(context.required(), rest()), promoted(partitions));
        IntermediateResult childResult = context.withRequired(childRequired).translate(child());
        if (childResult.kind().constant) {
            return childResult;
        }

        // Group by the child's labels - the ones the relation stores for a raw child, what an aggregated child delivers -
        // plus the partitions, which null-fill when the child lacks them.
        TranslationConstraint childLabels = childResult.kind().afterInitialAggregation
            ? deliveredRequirement(childResult.plan(), childResult.step(), childResult.value())
            : context.rawRequirement(childResult, childRequired);
        TranslationConstraint requirement = union(childLabels, promoted(partitions));

        var promqlCtx = new PromqlContext(context.time(), AggregateFunction.NO_WINDOW, childResult.step(), context.configuration());
        IntermediateResult aggregated = childResult.kind().afterInitialAggregation
            ? context.regroup(childResult, requirement, false, childResult.value())
            : context.collapse(childResult, requirement, childResult.value());
        LogicalPlan result = emitTopNBy(context, aggregated, partitions, promqlCtx);
        return aggregated.with(result, aggregated.value());
    }

    /** Ranks the already-collapsed per-series rows and keeps the top {@code k} within each step and partition. */
    private LogicalPlan emitTopNBy(
        TranslationContext context,
        IntermediateResult table,
        List<String> partitions,
        PromqlContext promqlContext
    ) {
        var groupings = new ArrayList<Expression>();
        groupings.add(table.step());
        LogicalPlan plan = table.plan();
        if (grouping() == AcrossSeriesAggregate.Grouping.BY) {
            var nulls = new ArrayList<Alias>();
            for (String partition : partitions) {
                Attribute carrier = find(table.plan().output(), partition);
                if (carrier == null) {
                    // a partition label absent from every series ranks as one partition, like Prometheus
                    nulls.add(emitNullExpression(mapToRef(partition)));
                    carrier = nulls.getLast().toAttribute();
                }
                groupings.add(carrier);
            }
            if (nulls.isEmpty() == false) {
                plan = new Eval(context.cmd().source(), plan, nulls);
            }
        }
        var order = (Order) buildEsqlFunction(table.value(), promqlContext);
        return new TopNBy(
            source(),
            plan,
            order != null ? List.of(order) : List.of(),
            new ToInteger(source(), parameters().getFirst()),
            groupings
        );
    }
}
