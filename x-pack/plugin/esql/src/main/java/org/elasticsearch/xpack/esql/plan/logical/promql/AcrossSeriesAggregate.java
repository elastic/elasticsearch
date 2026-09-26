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
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.any;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.of;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.sub;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.union;

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

    /**
     * {@code by (keys)} keeps exactly the keys, whatever the enclosing node requires. {@code without (dropped)} keeps every
     * other label but the metric name, so the child must deliver a packing that excludes the dropped ones and
     * {@code __name__} - unless it has concrete keys, which then simply lose them. No clause collapses every series into
     * one.
     */
    @Override
    public TranslationResult translate(TranslationContext translation) {
        // The keys are the node's output, not its groupings: a grouping that resolved to a metric field or to nothing is
        // not a label and the command projection never references it.
        List<String> keys = PromqlLabels.labelNames(output());
        // `without` also drops the metric name - Prometheus never carries `__name__` past an aggregation - so `without ()`
        // drops exactly it. NOTE: without excluding `__name__` the aggregation result would be split across metrics.
        List<String> dropped = new ArrayList<>(PromqlLabels.labelNames(groupings));
        if (dropped.contains(LabelMatcher.NAME) == false) {
            dropped.add(LabelMatcher.NAME);
        }
        TranslationConstraint required = translation.required();

        // IN: by (k) -> k; without (d) -> any + required - d, `__name__` always in d; none -> nothing
        TranslationConstraint below = switch (grouping) {
            case BY -> of(keys);
            case WITHOUT -> sub(union(any(), required), of(dropped));
            case NONE -> of();
        };
        TranslationResult child = translation.translate(child(), below);
        if (child.kind().constant) {
            return child;
        }

        // OUT: by (k) -> k, null where the child lacks one; without (d) -> child's labels - d; none -> nothing
        TranslationConstraint by = switch (grouping) {
            case BY -> of(keys);
            case WITHOUT -> sub(child.shape(), of(dropped));
            case NONE -> of();
        };
        Expression function = buildEsqlFunction(child.value(), translation.promqlContext(child));
        return translation.aggregate(child, by, function, grouping == Grouping.WITHOUT);
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.ACROSS_SERIES_AGGREGATION;
    }

    @Override
    public boolean dropsMetricName() {
        // Aggregations group series away: the result carries only grouping labels, never the metric name.
        return true;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Aggregates across series into a grouped result: a relabel below it must be part of this grouping's identity.
        return false;
    }
}
