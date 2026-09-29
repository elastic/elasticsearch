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

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping.WITHOUT;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.promoted;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.rest;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.subtract;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.union;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.deliveredRequirement;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.mapPromoted;

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
    private final Attribute timeseriesAttribute;

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
        this.timeseriesAttribute = FieldAttribute.timeSeriesAttribute(source);
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
     * {@code WITHOUT} over a non-enumerable child (a selector / full series identity) uses a dynamic
     * {@code _timeseries} output, because the concrete retained labels are not known until lowering time.
     * {@code WITHOUT} over a concrete-output child (a {@code BY}/{@code NONE} aggregate) instead exposes that child's
     * concrete labels minus the excluded ones - the {@code WITHOUT} is a plain re-grouping over known columns, so it
     * must NOT claim a {@code _timeseries} the plan never produces. {@code BY} and {@code NONE} export concrete labels
     * or nothing.
     */
    @Override
    public List<Attribute> output() {
        // Output `_timeseries` if grouping is not constant, e.g. `without(...)`
        if (grouping == Grouping.WITHOUT) {
            if (child() instanceof AcrossSeriesAggregate childAggregate && childAggregate.grouping() != Grouping.WITHOUT) {
                Set<String> excluded = new HashSet<>();
                for (Attribute label : groupings) {
                    excluded.add(labelKey(label));
                }
                return childAggregate.output().stream().filter(a -> excluded.contains(labelKey(a)) == false).toList();
            }
            return List.of(timeseriesAttribute);
        }
        // A label that resolved to a metric field is not a real label, so translation drops it from the
        // aggregate; exclude it from the output too, otherwise the command projection references a column the
        // plan never produces. Absent labels are already excluded by the resolved() check.
        return groupings.stream()
            .filter(a -> a.resolved() && a.dataType() != DataType.NULL)
            .filter(a -> (a instanceof FieldAttribute fa && fa.isMetric()) == false)
            .toList();
    }

    /**
     * The PromQL label key of an attribute: a {@link FieldAttribute}'s backing field name with the Prometheus
     * {@code labels.} passthrough prefix stripped (so {@code labels.pod} compares equal to a bare {@code pod}).
     */
    private static String labelKey(Attribute attr) {
        String name = attr instanceof FieldAttribute fieldAttribute ? fieldAttribute.fieldName().string() : attr.name();
        return name.startsWith("labels.") ? name.substring("labels.".length()) : name;
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
     * Translates {@code AcrossSeriesAggregate} to an ESQL {@code Aggregate}. The requirement transposed below the
     * aggregate names every column the subtree must expose, so the child translates once and the aggregate groups
     * by that same requirement. Only {@code AcrossSeriesAggregate} creates plan-level aggregation nodes;
     * within-series aggregates and function calls lower to expressions.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        List<String> keys = mapPromoted(groupings());
        TranslationConstraint childRequired = switch (grouping()) {
            case BY -> promoted(keys);
            // without () keeps the child's label set; without (K) declares its own and widens every pending one by K
            case WITHOUT -> keys.isEmpty() ? context.required() : union(subtract(context.required(), keys), rest(keys));
            case NONE -> TranslationConstraint.EMPTY;
        };
        TranslationContext childTranslation = context.withRequired(childRequired);
        IntermediateResult ir = childTranslation.translate(child());
        if (ir.kind().constant) {
            return ir;
        }
        TranslationConstraint requirement = switch (grouping()) {
            case BY -> promoted(mapPromoted(output()));
            // A collapse groups by the labels the relation stores; a regroup by what the aggregated child delivers.
            case WITHOUT -> context.regroupWithout(
                ir.kind().afterInitialAggregation
                    ? deliveredRequirement(ir.plan(), ir.step(), ir.value())
                    : context.rawRequirement(ir, childRequired),
                keys
            );
            case NONE -> TranslationConstraint.EMPTY;
        };

        var promqlCtx = new PromqlContext(context.time(), AggregateFunction.NO_WINDOW, ir.step(), context.configuration());
        Expression function = buildEsqlFunction(ir.value(), promqlCtx);
        // A raw operand collapses once, with the operator's function fused into the per-series aggregate; a table regroups.
        return ir.kind().afterInitialAggregation
            ? context.regroup(ir, requirement, grouping() == WITHOUT, function)
            : context.collapse(ir, requirement, function);
    }
}
