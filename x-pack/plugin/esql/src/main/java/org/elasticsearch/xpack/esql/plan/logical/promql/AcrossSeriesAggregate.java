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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
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
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintExclude;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintSub;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintUnion;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintUnset;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintWithPromoted;

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
     * {@code WITHOUT} over a child with a packed identity (a selector, a function over one) uses a dynamic
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
            List<Attribute> childOutput = child().output();
            if (childOutput.stream().noneMatch(a -> MetadataAttribute.isTimeSeriesAttributeName(a.name()))) {
                Set<String> excluded = new HashSet<>();
                for (Attribute label : groupings) {
                    excluded.add(labelKey(label));
                }
                return childOutput.stream().filter(a -> excluded.contains(labelKey(a)) == false).toList();
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
        List<String> keys = TranslationContext.asPromotedLabels(groupings());
        if (grouping() == WITHOUT && keys.isEmpty() == false && context.supportsTimeSeriesUnset()) {
            // One _timeseries per node: the child carries its series' whole _timeseries, and the keys are unset from it here,
            // where the identity drops them. A raw child first collapses per series, by that _timeseries.
            TranslationSchema childRequired = newConstraintUnion(newConstraintExclude(context.required(), keys), newConstraintUnset());
            IntermediateResult ir = context.withRequired(childRequired).translate(child());
            if (ir.kind().constant) {
                return ir;
            }
            if (ir.kind().afterInitialAggregation == false) {
                ir = ir.withCollapse(context, context.newConstraintForCollapse(ir, childRequired), ir.value());
            }
            TranslationSchema requirement = context.newConstraintForRegroupUnset(TranslationContext.newConstraintDeliveredBy(ir), keys);
            ir = ir.withUnsetLabels(context, keys);
            var promqlCtx = new PromqlContext(context.time(), AggregateFunction.NO_WINDOW, ir.step(), context.configuration());
            return ir.withRegroup(context, requirement, true, buildEsqlFunction(ir.value(), promqlCtx));
        }
        // Otherwise - an older node somewhere, or nothing to unset - a without (K) asks its child for the _timeseries already
        // excluding K, one _timeseries per exclusion set, and fuses with the per-series aggregate over a raw child.
        TranslationSchema childRequired = switch (grouping()) {
            case BY -> newConstraintWithPromoted(keys);
            // without () keeps the child's label set; without (K) declares its own and widens every pending one by K
            case WITHOUT -> keys.isEmpty()
                ? context.required()
                : newConstraintUnion(newConstraintSub(context.required(), keys), newConstraintUnset(keys));
            case NONE -> TranslationSchema.EMPTY;
        };
        TranslationContext childTranslation = context.withRequired(childRequired);
        IntermediateResult ir = childTranslation.translate(child());
        if (ir.kind().constant) {
            return ir;
        }
        TranslationSchema requirement = switch (grouping()) {
            case BY -> newConstraintWithPromoted(TranslationContext.asPromotedLabels(output()));
            // A collapse groups by the labels the relation stores; a regroup by what the aggregated child delivers.
            case WITHOUT -> context.newConstraintForRegroupWithout(
                ir.kind().afterInitialAggregation
                    ? TranslationContext.newConstraintDeliveredBy(ir)
                    : context.newConstraintForCollapse(ir, childRequired),
                keys
            );
            case NONE -> TranslationSchema.EMPTY;
        };

        var promqlCtx = new PromqlContext(context.time(), AggregateFunction.NO_WINDOW, ir.step(), context.configuration());
        Expression function = buildEsqlFunction(ir.value(), promqlCtx);
        // A raw operand collapses once, with the operator's function fused into the per-series aggregate; a table regroups.
        return ir.kind().afterInitialAggregation
            ? ir.withRegroup(context, requirement, grouping() == WITHOUT, function)
            : ir.withCollapse(context, requirement, function);
    }
}
