/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;

import java.util.ArrayList;
import java.util.List;

/**
 * Moves a {@link TimeSeriesUnset} of a series' {@code _timeseries} into the per-series {@link TimeSeriesAggregate} grouping by
 * that {@code _timeseries}. The result is identical across the series, so the aggregate takes it as one more grouping and
 * computes it once per series, on the data node, instead of once for every row above. The {@link Eval} that defined it keeps
 * its name and id, now an alias of the moved grouping.
 * <p>
 * Only through nodes that carry the {@code _timeseries} through unchanged ({@link Eval}, {@link Filter}, {@link Project},
 * {@link TopNBy}); anywhere else the {@link TimeSeriesUnset} stays where it is. Runs after {@code TranslateTimeSeriesWithout}
 * lowers the {@code _timeseries} grouping and before {@link TranslateTimeSeriesAggregate} splits the aggregate in two.
 */
public final class PushDownTimeSeriesUnset extends AnalyzerRules.AnalyzerRule<Eval> {

    @Override
    protected boolean skipResolved() {
        return false;
    }

    @Override
    protected LogicalPlan rule(Eval eval) {
        LogicalPlan child = eval.child();
        List<Alias> fields = new ArrayList<>(eval.fields().size());
        boolean moved = false;
        for (Alias field : eval.fields()) {
            if (field.child() instanceof TimeSeriesUnset unset && unset.timeseries() instanceof Attribute timeseries) {
                Moved result = pushDown(child, timeseries, unset, field.name());
                if (result != null) {
                    child = result.plan();
                    fields.add(new Alias(field.source(), field.name(), result.grouping(), field.id()));
                    moved = true;
                    continue;
                }
            }
            fields.add(field);
        }
        return moved ? new Eval(eval.source(), child, fields) : eval;
    }

    /** The plan with the unset computed by the aggregate below, and the attribute carrying it up to the plan's output. */
    private record Moved(LogicalPlan plan, Attribute grouping) {}

    private static Moved pushDown(LogicalPlan plan, Attribute timeseries, TimeSeriesUnset unset, String name) {
        return switch (plan) {
            case TimeSeriesAggregate aggregate -> group(aggregate, timeseries, unset, name);
            case Eval eval when eval.fields().stream().noneMatch(field -> field.id().equals(timeseries.id())) -> {
                Moved below = pushDown(eval.child(), timeseries, unset, name);
                yield below == null ? null : new Moved(eval.replaceChild(below.plan()), below.grouping());
            }
            case Filter filter -> {
                Moved below = pushDown(filter.child(), timeseries, unset, name);
                yield below == null ? null : new Moved(filter.replaceChild(below.plan()), below.grouping());
            }
            case TopNBy topNBy -> {
                Moved below = pushDown(topNBy.child(), timeseries, unset, name);
                yield below == null ? null : new Moved(topNBy.replaceChild(below.plan()), below.grouping());
            }
            case Project project when project.output().stream().anyMatch(attribute -> attribute.id().equals(timeseries.id())) -> {
                Moved below = pushDown(project.child(), timeseries, unset, name);
                if (below == null) {
                    yield null;
                }
                List<NamedExpression> projections = new ArrayList<>(project.projections());
                projections.add(below.grouping());
                yield new Moved(new Project(project.source(), below.plan(), projections), below.grouping());
            }
            default -> null;
        };
    }

    /** The aggregate also grouping by the unset of its own {@code _timeseries} grouping, when it groups by {@code timeseries}. */
    private static Moved group(TimeSeriesAggregate aggregate, Attribute timeseries, TimeSeriesUnset unset, String name) {
        Attribute grouped = null;
        int unsets = 0;
        for (Expression grouping : aggregate.groupings()) {
            Attribute attribute = Expressions.attribute(grouping);
            if (attribute != null && attribute.id().equals(timeseries.id())) {
                grouped = attribute;
            }
            if (Alias.unwrap(grouping) instanceof TimeSeriesUnset) {
                unsets++;
            }
        }
        if (grouped == null) {
            return null;
        }
        List<Expression> children = new ArrayList<>(unset.children());
        children.set(0, grouped);
        Alias grouping = new Alias(
            unset.source(),
            Attribute.rawTemporaryName(name, "unset", String.valueOf(unsets)),
            unset.replaceChildren(children)
        );
        List<Expression> groupings = new ArrayList<>(aggregate.groupings());
        groupings.add(grouping);
        List<NamedExpression> aggregates = new ArrayList<>(aggregate.aggregates());
        aggregates.add(grouping.toAttribute());
        return new Moved(aggregate.with(aggregate.child(), groupings, aggregates), grouping.toAttribute());
    }
}
