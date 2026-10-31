/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.stats;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.DocPreserving;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Streaming;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Derives planning statistics without executing logical plans or invoking local-data suppliers. */
public final class StatsDeriver {
    private final LeafStatsProvider leafStatsProvider;

    /** Creates a derivation pass backed only by planning-time leaf metadata. */
    public StatsDeriver() {
        this(new MetadataLeafStatsProvider());
    }

    /** Creates a derivation pass with a provider for leaf statistics. */
    public StatsDeriver(LeafStatsProvider leafStatsProvider) {
        this.leafStatsProvider = Objects.requireNonNull(leafStatsProvider);
    }

    /** Derives statistics for each plan object while preserving identity-based lookup semantics. */
    public Map<LogicalPlan, PlanStats> derive(LogicalPlan root) {
        Objects.requireNonNull(root);
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        derive(root, statistics);
        return Collections.unmodifiableMap(statistics);
    }

    private PlanStats derive(LogicalPlan plan, Map<LogicalPlan, PlanStats> statistics) {
        PlanStats existing = statistics.get(plan);
        if (existing != null) {
            return existing;
        }
        List<PlanStats> children = new ArrayList<>(plan.children().size());
        for (LogicalPlan child : plan.children()) {
            children.add(derive(child, statistics));
        }
        PlanStats result;
        if (plan instanceof LeafPlan leaf) {
            result = Objects.requireNonNull(leafStatsProvider.estimate(leaf));
        } else if (plan instanceof Aggregate aggregate) {
            result = aggregate(aggregate, children.get(0));
        } else if (plan instanceof Join join) {
            result = join(join, children.get(0), children.get(1));
        } else if (plan instanceof MergePlan merge) {
            result = merge(merge, children);
        } else if (plan instanceof Limit limit) {
            result = limit(plan.output(), children.get(0), limit.limit());
        } else if (plan instanceof TopN topN) {
            result = limit(plan.output(), children.get(0), topN.limit());
        } else if (plan instanceof Filter) {
            result = remap(plan.output(), children.get(0), children.get(0).rows(), children.get(0).bytes());
        } else if (plan instanceof Project project) {
            result = project(project, children.get(0));
        } else if (plan instanceof Eval eval) {
            result = eval(eval, children.get(0));
        } else if (plan instanceof UnaryPlan unary && (plan instanceof Streaming || plan instanceof DocPreserving)) {
            result = remap(unary.output(), children.get(0), children.get(0).rows(), children.get(0).bytes());
        } else {
            result = unknown(plan.output());
        }
        statistics.put(plan, result);
        return result;
    }

    private static PlanStats project(Project project, PlanStats child) {
        Map<NameId, ColumnStats> columns = new LinkedHashMap<>();
        for (NamedExpression projection : project.projections()) {
            Attribute output = projection.toAttribute();
            Attribute source = sourceAttribute(projection);
            columns.put(output.id(), source == null ? StatsSupport.withDefaultWidth(output) : child.column(source));
        }
        return create(
            project.output(),
            child.rows(),
            columns,
            StatsSupport.bytes(child.rows(), StatsSupport.averageRowBytes(project.output(), columns))
        );
    }

    private static PlanStats eval(Eval eval, PlanStats child) {
        Map<NameId, ColumnStats> columns = new LinkedHashMap<>();
        for (Attribute output : eval.output()) {
            ColumnStats existing = child.columns().get(output.id());
            if (existing != null) {
                columns.put(output.id(), existing);
                continue;
            }
            Alias generated = findAlias(eval.fields(), output);
            Attribute source = generated == null ? null : sourceAttribute(generated);
            columns.put(output.id(), source == null ? StatsSupport.withDefaultWidth(output) : child.column(source));
        }
        return create(
            eval.output(),
            child.rows(),
            columns,
            StatsSupport.bytes(child.rows(), StatsSupport.averageRowBytes(eval.output(), columns))
        );
    }

    private static PlanStats aggregate(Aggregate aggregate, PlanStats child) {
        Estimate rows = aggregateRows(aggregate, child);
        Map<NameId, ColumnStats> columns = new LinkedHashMap<>();
        for (NamedExpression outputExpression : aggregate.aggregates()) {
            Attribute output = outputExpression.toAttribute();
            Attribute source = sourceAttribute(outputExpression);
            columns.put(output.id(), source == null ? StatsSupport.withDefaultWidth(output) : child.column(source));
        }
        return create(aggregate.output(), rows, columns, Estimate.UNKNOWN);
    }

    private static Estimate aggregateRows(Aggregate aggregate, PlanStats child) {
        if (aggregate.groupings().isEmpty()) {
            return Estimate.exact(1);
        }
        if (child.rows().isUnknown()) {
            return Estimate.UNKNOWN;
        }
        double groups = 1;
        double groupsUpper = 1;
        for (Expression grouping : aggregate.groupings()) {
            Attribute attribute = Expressions.attribute(grouping);
            if (attribute == null) {
                return Estimate.UNKNOWN;
            }
            Estimate ndv = child.column(attribute).ndv();
            if (ndv.isUnknown()) {
                return Estimate.UNKNOWN;
            }
            groups = multiply(groups, Math.max(1, ndv.value()));
            groupsUpper = multiply(groupsUpper, Math.max(1, ndv.upperBound()));
        }
        double value = Math.min(child.rows().value(), groups);
        double upperBound = Math.max(value, Math.min(child.rows().upperBound(), groupsUpper));
        return Estimate.derived(value, upperBound, Math.max(0, upperBound - value));
    }

    private static PlanStats join(Join join, PlanStats left, PlanStats right) {
        Map<NameId, ColumnStats> columns = new LinkedHashMap<>(left.columns());
        columns.putAll(right.columns());
        Estimate rows = joinRows(join, left, right);
        return create(join.output(), rows, columns, Estimate.UNKNOWN);
    }

    private static Estimate joinRows(Join join, PlanStats left, PlanStats right) {
        if (left.rows().isUnknown() || right.rows().isUnknown()) {
            return Estimate.UNKNOWN;
        }
        List<Attribute> leftKeys = join.config().leftFields();
        List<Attribute> rightKeys = join.config().rightFields();
        if (leftKeys.isEmpty() || leftKeys.size() != rightKeys.size()) {
            return Estimate.UNKNOWN;
        }
        double divisor = 1;
        for (int i = 0; i < leftKeys.size(); i++) {
            Estimate leftNdv = left.column(leftKeys.get(i)).ndv();
            Estimate rightNdv = right.column(rightKeys.get(i)).ndv();
            if (leftNdv.isUnknown() || rightNdv.isUnknown()) {
                return Estimate.UNKNOWN;
            }
            divisor = multiply(divisor, Math.max(1, Math.max(leftNdv.value(), rightNdv.value())));
        }
        double matched = multiply(left.rows().value(), right.rows().value()) / divisor;
        double upperBound = multiply(left.rows().upperBound(), right.rows().upperBound());
        if (join.config().type() == JoinTypes.LEFT) {
            matched = Math.max(left.rows().value(), matched);
            upperBound = Math.max(left.rows().upperBound(), upperBound);
        }
        return Estimate.derived(matched, Math.max(matched, upperBound), Math.max(0, upperBound - matched));
    }

    private static PlanStats merge(MergePlan merge, List<PlanStats> children) {
        Estimate rows = Estimate.exact(0);
        Estimate bytes = Estimate.exact(0);
        Map<NameId, ColumnStats> columns = new LinkedHashMap<>();
        boolean rowsKnown = true;
        boolean bytesKnown = true;
        for (int childIndex = 0; childIndex < children.size(); childIndex++) {
            PlanStats child = children.get(childIndex);
            if (child.rows().isUnknown()) {
                rowsKnown = false;
            } else {
                rows = rows.plus(child.rows());
            }
            if (child.bytes().isUnknown()) {
                bytesKnown = false;
            } else {
                bytes = bytes.plus(child.bytes());
            }
            List<Attribute> childOutput = merge.children().get(childIndex).output();
            for (int i = 0; i < Math.min(childOutput.size(), merge.output().size()); i++) {
                Attribute output = merge.output().get(i);
                ColumnStats candidate = child.column(childOutput.get(i));
                columns.merge(output.id(), candidate, StatsDeriver::mergeColumns);
            }
        }
        Estimate mergedRows = rowsKnown ? rows : Estimate.UNKNOWN;
        Estimate mergedBytes = bytesKnown ? bytes : Estimate.UNKNOWN;
        return create(merge.output(), mergedRows, columns, mergedBytes);
    }

    private static ColumnStats mergeColumns(ColumnStats left, ColumnStats right) {
        Estimate ndv = left.ndv().isUnknown() || right.ndv().isUnknown() ? Estimate.UNKNOWN : left.ndv().plus(right.ndv());
        Estimate nullFraction = left.nullFraction().isUnknown() || right.nullFraction().isUnknown()
            ? Estimate.UNKNOWN
            : Estimate.derived(
                (left.nullFraction().value() + right.nullFraction().value()) / 2,
                Math.max(left.nullFraction().upperBound(), right.nullFraction().upperBound()),
                Math.max(left.nullFraction().spread(), right.nullFraction().spread())
            );
        return new ColumnStats(
            ndv,
            nullFraction,
            left.minValue(),
            left.maxValue(),
            left.averageBytes().isUnknown() ? right.averageBytes() : left.averageBytes(),
            Estimate.UNKNOWN
        );
    }

    private static PlanStats limit(List<Attribute> output, PlanStats child, Expression limit) {
        if (limit instanceof Literal literal && literal.value() instanceof Number value && value.doubleValue() >= 0) {
            double cap = value.doubleValue();
            Estimate rows;
            if (child.rows().isUnknown()) {
                rows = Estimate.derived(cap, cap, 0);
            } else {
                double estimate = Math.min(child.rows().value(), cap);
                double upperBound = Math.max(estimate, Math.min(child.rows().upperBound(), cap));
                rows = Estimate.derived(estimate, upperBound, Math.min(child.rows().spread(), upperBound - estimate));
            }
            return remap(output, child, rows, Estimate.UNKNOWN);
        }
        return remap(output, child, child.rows(), child.bytes());
    }

    private static PlanStats remap(List<Attribute> output, PlanStats child, Estimate rows, Estimate bytes) {
        Map<NameId, ColumnStats> columns = new LinkedHashMap<>();
        for (Attribute attribute : output) {
            ColumnStats stats = child.columns().get(attribute.id());
            columns.put(attribute.id(), stats == null ? StatsSupport.withDefaultWidth(attribute) : stats);
        }
        return create(output, rows, columns, bytes);
    }

    private static PlanStats create(List<Attribute> output, Estimate rows, Map<NameId, ColumnStats> columns, Estimate bytes) {
        for (Attribute attribute : output) {
            columns.putIfAbsent(attribute.id(), StatsSupport.withDefaultWidth(attribute));
        }
        Estimate averageRowBytes = StatsSupport.averageRowBytes(output, columns);
        Estimate totalBytes = bytes.isUnknown() ? StatsSupport.bytes(rows, averageRowBytes) : bytes;
        return new PlanStats(rows, averageRowBytes, columns, totalBytes);
    }

    private static PlanStats unknown(List<Attribute> output) {
        Map<NameId, ColumnStats> columns = StatsSupport.unknownColumns(output);
        return new PlanStats(Estimate.UNKNOWN, StatsSupport.averageRowBytes(output, columns), columns, Estimate.UNKNOWN);
    }

    private static Attribute sourceAttribute(NamedExpression expression) {
        if (expression instanceof Attribute attribute) {
            return attribute;
        }
        if (expression instanceof Alias alias && alias.child() instanceof Attribute attribute) {
            return attribute;
        }
        return null;
    }

    private static Alias findAlias(List<Alias> aliases, Attribute output) {
        for (Alias alias : aliases) {
            if (alias.toAttribute().id().equals(output.id())) {
                return alias;
            }
        }
        return null;
    }

    private static double multiply(double left, double right) {
        double result = left * right;
        return Double.isFinite(result) ? result : Double.MAX_VALUE;
    }
}
