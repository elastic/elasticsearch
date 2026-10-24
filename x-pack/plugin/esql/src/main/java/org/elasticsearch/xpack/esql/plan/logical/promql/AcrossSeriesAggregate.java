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
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import static java.util.function.Predicate.not;
import static org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping.WITHOUT;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.hasTsMetadata;
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
    private final Attribute metadata;

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
        this.metadata = FieldAttribute.timeSeriesAttribute(source);
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

    // @Override
    // public String telemetryLabel() {
    // return "PROMQL_ACROSS_SERIES_AGGREGATION";
    // }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            AcrossSeriesAggregate that = (AcrossSeriesAggregate) o;
            return grouping == that.grouping && Objects.equals(groupings, that.groupings);
        }
        return false;
    }

    /**
     * {@code WITHOUT} over a child with metadata identity (a selector, a function over one) uses a dynamic
     * {@code _timeseries} output, because the concrete retained labels are not known until lowering time.
     * {@code WITHOUT} over a child that names every label it exposes (a {@code BY}/{@code NONE} aggregate, a binary
     * operator between two of them) instead exposes those labels minus the excluded ones - the {@code WITHOUT} is a plain
     * re-grouping over known columns, so it must NOT claim a {@code _timeseries} the plan never produces. {@code BY} and
     * {@code NONE} export concrete labels or nothing.
     */
    @Override
    public List<Attribute> output() {
        // Output `_timeseries` if grouping is not constant, e.g. `without(...)`
        if (grouping == Grouping.WITHOUT) {
            var output = child().output();
            if (hasTsMetadata(output)) {
                return List.of(metadata);
            }

            var skip = new HashSet<String>();
            skip.add(LabelMatcher.NAME);
            for (var g : groupings) {
                skip.add(labelName(g));
            }

            return output.stream().filter(not(a -> skip.contains(labelName(a)))).toList();
        }
        // A label that resolved to a metric field is not a real label,
        // so translation drops it from the aggregate;
        // exclude it from the output too, otherwise the command projection
        // references a column the plan never produces.
        // Absent labels are already excluded by the resolved() check.
        return groupings.stream()
            .filter(a -> a.resolved() && a.dataType() != DataType.NULL)
            .filter(not(a -> a instanceof FieldAttribute fa && fa.isMetric()))
            .toList();
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
     * Translates the child once, then materializes grouping keys as scalar expressions over its current labels.
     * Child requirements allow selectors to read named dimensions directly; a child aggregation establishes its
     * own identity before these keys are derived. Within-series functions remain expressions in the first aggregate.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        // WITHOUT needs every input label before removing the exclusions.
        List<String> labelsToPush = null;
        if (grouping != Grouping.WITHOUT) {
            labelsToPush = labelNames(groupings);
        }
        TranslationContext translation = context.withPushedDown(labelsToPush);

        IntermediateResult ir = translation.translate(child());
        if (ir.kind() == Kind.EMPTY) {
            return ir;
        }

        // NONE is BY with no keys: request no source labels and clear the child's grouping.
        if (grouping == WITHOUT) {
            ir = context.addInput(source(), ir, groupings(), true);
        } else {
            ir = context.addInput(source(), ir, output(), false);
        }

        var promqlCtx = new PromqlContext(context.time(), AggregateFunction.NO_WINDOW, ir.step(), context.configuration());
        Expression function = buildEsqlFunction(ir.value(), promqlCtx);

        // A raw operand collapses once, with the operator's function fused into the per-series aggregate; a table regroups.
        if (ir.kind().afterInitialAggregation) {
            return context.regroup(ir, function);
        }
        return context.collapse(ir, function);
    }
}
