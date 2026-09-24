/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.any;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.of;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.union;

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

    /**
     * {@code topk}/{@code bottomk}: collapse the child to one row per series, then rank and keep the top {@code k}. A
     * {@code by} clause only partitions the ranking; it does not change the output labels.
     */
    @Override
    public TranslationResult translate(TranslationContext translation) {
        if (grouping == Grouping.WITHOUT) {
            // TODO: support function like: topk without(...)
            throw new VerificationException("function [{}] is not yet supported with [{}]", functionName(), Grouping.WITHOUT.name());
        }
        List<String> partitions = PromqlLabels.labelNames(groupings);
        // IN: any + required, plus the partitions as columns to rank within
        TranslationConstraint below = union(translation.required(), any(), of(partitions));
        TranslationResult child = translation.translate(child(), below);
        if (child.kind().constant) {
            return child;
        }
        // OUT: child's labels + partitions, null where the child lacks one
        TranslationConstraint keys = union(child.shape(), of(partitions));
        TranslationResult collapsed = translation.aggregate(child, keys, child.value());
        return collapsed.with(topNBy(collapsed, partitions, translation.promqlContext(collapsed)), collapsed.value());
    }

    /** Ranks the already-collapsed per-series rows and keeps the top {@code k} within each step and partition. */
    private LogicalPlan topNBy(TranslationResult table, List<String> partitions, PromqlContext promqlContext) {
        var partitionKeys = new ArrayList<Expression>();
        partitionKeys.add(table.step());
        if (grouping == Grouping.BY) {
            for (String partition : partitions) {
                Attribute partitionExpr = table.label(partition);
                assert partitionExpr != null : "[INVARIANT]: ranking partition " + partition + " must be produced by the child";
                partitionKeys.add(partitionExpr);
            }
        }
        var order = (Order) buildEsqlFunction(table.value(), promqlContext);
        return new TopNBy(
            source(),
            table.plan(),
            order != null ? List.of(order) : List.of(),
            new ToInteger(source(), parameters().getFirst()),
            partitionKeys
        );
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
}
