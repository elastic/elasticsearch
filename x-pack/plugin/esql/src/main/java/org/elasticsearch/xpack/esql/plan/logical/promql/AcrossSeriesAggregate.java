/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

import static java.util.function.Predicate.not;
import static org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping.WITHOUT;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.labelName;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.labelNames;

/**
 * Represents a PromQL aggregate function call that operates across multiple time series.
 * <p>
 * These functions aggregate elements from multiple time series into a single result vector,
 * optionally grouping by specific labels. This corresponds to PromQL syntax:
 * <pre>
 * function_name(instant_vector) [without|by (label_list)]
 * </pre>
 *
 * Examples:
 * <pre>
 * sum(http_requests_total)
 * sum(rate(http_requests_total[5m]))
 * avg(cpu_usage) by (host, env)
 * max(response_time) without (instance)
 * </pre>
 *
 * These functions reduce the number of time series by aggregating values across series
 * that share the same grouping labels (or all series if no grouping is specified).
 */
public final class AcrossSeriesAggregate extends PromqlFunctionCall {

    public enum Grouping {
        BY,
        WITHOUT,
        NONE
    }

    private final Grouping grouping;
    private final List<Attribute> groupings;

    public AcrossSeriesAggregate(
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
        return NodeInfo.create(this, AcrossSeriesAggregate::new, child(), definition(), parameters(), grouping(), groupings());
    }

    @Override
    public AcrossSeriesAggregate replaceChild(LogicalPlan newChild) {
        return new AcrossSeriesAggregate(source(), newChild, definition(), parameters(), grouping(), groupings());
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            AcrossSeriesAggregate that = (AcrossSeriesAggregate) o;
            return grouping == that.grouping && Objects.equals(groupings, that.groupings);
        }
        return false;
    }

    /**
     * WITHOUT preserves the child's label representation: metadata stays metadata, while named labels lose the
     * exclusions and metric name. BY exposes only its named grouping labels; NONE exposes no labels.
     */
    @Override
    public List<Attribute> output() {
        if (grouping == WITHOUT) {
            var output = child().output();
            var dropping = dropping();
            var ml = PromqlLabels.metadataLabel(output);
            if (ml != null) {
                // Compose child's dropping (e.g. label_replace dst) with this without(...);
                // dropping child dropping would let those labels re-enter the blob.
                var unset = new LinkedHashSet<>(ml.unset());
                unset.addAll(dropping);
                var result = new ArrayList<Attribute>();
                result.add(UnresolvedPromqlAttribute.metadata(source(), unset));
                for (var attr : output) {
                    if (PromqlLabels.isMetadataLabel(attr) == false && dropping.contains(labelName(attr)) == false) {
                        result.add(attr);
                    }
                }
                return result;
            }
            return output.stream().filter(not(a -> dropping.contains(labelName(a)))).toList();
        }
        // absent labels and metric fields cannot supply grouping columns to the command output.
        return groupings.stream()
            .filter(a -> a.resolved() && a.dataType() != DataType.NULL)
            .filter(not(a -> a instanceof FieldAttribute fa && fa.isMetric()))
            .toList();
    }

    /** Labels this without(...) drops from metadata (listed labels + __name__, even if the list is empty). */
    private List<String> dropping() {
        List<String> labels = labelNames(groupings);
        if (labels.contains(LabelMatcher.NAME)) {
            return labels;
        }
        var drop = new ArrayList<>(labels);
        drop.add(LabelMatcher.NAME);
        return drop;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), grouping, groupings);
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.ACROSS_SERIES_AGGREGATION;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Aggregates across series into a grouped result: a relabel below it must be part of this grouping's identity.
        return false;
    }

    /**
     * Translates the child once, then aggregates it keyed by this node's result labels ({@link #output()}), read from the
     * child as references. Within-series functions remain expressions in the first aggregate.
     */
    @Override
    public IntermediateResult translate(TranslationContext ctx) {
        IntermediateResult ir = ctx.translate(child());
        if (ir.kind().constant) {
            return ir;
        }

        var promqlCtx = new PromqlContext(ctx.time(), AggregateFunction.NO_WINDOW, ir.step(), ctx.configuration());
        Expression function = buildEsqlFunction(ir.value(), promqlCtx);

        // A raw operand collapses once, with the operator's function fused into the per-series aggregate; a table regroups.
        List<Attribute> keys = UnresolvedPromqlAttribute.tsIdentity(source(), output());

        if (ir.kind().afterInitialAggregation) {
            return ctx.emitAggregate(ir, keys, grouping == WITHOUT, function);
        }

        return ctx.emitInitialAggregate(ir, keys, function);
    }
}
