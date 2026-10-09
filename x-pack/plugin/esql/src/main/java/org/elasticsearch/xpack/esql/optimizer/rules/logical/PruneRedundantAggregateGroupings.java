/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

/**
 * Removes {@code STATS BY} keys that do not add grouping cardinality and rebuilds their output above the aggregation.
 * <p>
 * Only scalar constants qualify: {@code STATS ... BY 1, x} or {@code EVAL c = 1 | STATS ... BY c, x} group on {@code x}
 * alone and re-emit the constant in an {@link Eval} above the {@link Aggregate}. A constant is single-valued on every
 * row, so this is always equivalent.
 * <p>
 * A key <em>derived</em> from other keys (e.g. {@code EVAL m = x - 1 | STATS ... BY x, m}) is deliberately kept, even
 * though it looks functionally dependent on {@code x}. The dependency holds only while {@code x} is single-valued: on a
 * row where {@code x} is a list, the query as written evaluates {@code x - 1} to {@code null} (with a warning), while
 * the aggregate unrolls the list into one group per element. Recomputing {@code m} per group would then produce a
 * number where the query returns {@code null}, and the group set itself can differ (a list row and a scalar row with a
 * shared element fall into one group after the rewrite but two before it). Nothing in the plan proves a column
 * single-valued (the data type carries no such flag and the external source statistics are partial and format
 * dependent), so the rule does not prune derived keys.
 */
public final class PruneRedundantAggregateGroupings extends OptimizerRules.OptimizerRule<Aggregate>
    implements
        OptimizerRules.LocalAware<Aggregate> {

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        if (shouldSkipAggregate(aggregate)) {
            return aggregate;
        }

        AttributeMap<Expression> evalAliases = evalAliases(aggregate.child());
        List<Expression> newGroupings = new ArrayList<>(aggregate.groupings().size());
        List<PrunedGrouping> prunedGroupings = new ArrayList<>();

        for (Expression grouping : aggregate.groupings()) {
            Expression replacement = replacementFor(grouping, evalAliases);
            if (replacement == null) {
                newGroupings.add(grouping);
            } else {
                prunedGroupings.add(new PrunedGrouping(grouping, replacement));
            }
        }

        if (prunedGroupings.isEmpty() || newGroupings.isEmpty()) {
            return aggregate;
        }

        List<NamedExpression> newAggregates = new ArrayList<>(aggregate.aggregates().size());
        List<Alias> postAggregateEvals = new ArrayList<>();
        for (NamedExpression aggregateExpression : aggregate.aggregates()) {
            PrunedGrouping pruned = matchingPrunedGrouping(aggregateExpression, prunedGroupings);
            if (pruned == null) {
                newAggregates.add(aggregateExpression);
            } else {
                postAggregateEvals.add(reconstruct(aggregateExpression, pruned.replacement()));
            }
        }

        LogicalPlan plan = aggregate.with(
            pruneUnusedChildEvals(aggregate.child(), prunedGroupings, newGroupings, newAggregates),
            newGroupings,
            newAggregates
        );
        if (postAggregateEvals.isEmpty() == false) {
            plan = new Eval(aggregate.source(), plan, postAggregateEvals);
            plan = new Project(aggregate.source(), plan, aggregate.aggregates().stream().map(NamedExpression::toAttribute).toList());
        }
        return plan;
    }

    @Override
    public Rule<Aggregate, LogicalPlan> local() {
        return null;
    }

    private static boolean shouldSkipAggregate(Aggregate aggregate) {
        // Inline stats RHS aggregates use StubRelation while their join keys still mirror the original groupings.
        return aggregate.groupings().isEmpty()
            || aggregate instanceof TimeSeriesAggregate
            || aggregate.child().anyMatch(StubRelation.class::isInstance);
    }

    private static AttributeMap<Expression> evalAliases(LogicalPlan plan) {
        AttributeMap.Builder<Expression> aliases = AttributeMap.builder();
        plan.forEachDown(Eval.class, eval -> eval.fields().forEach(alias -> aliases.put(alias.toAttribute(), alias.child())));
        return aliases.build();
    }

    private static LogicalPlan pruneUnusedChildEvals(
        LogicalPlan child,
        List<PrunedGrouping> prunedGroupings,
        List<Expression> newGroupings,
        List<NamedExpression> newAggregates
    ) {
        if (!(child instanceof Eval eval)) {
            return child;
        }

        AttributeSet requiredByAggregate = Aggregate.computeReferences(newAggregates, newGroupings);
        AttributeSet.Builder removableAttributes = AttributeSet.builder();
        for (PrunedGrouping prunedGrouping : prunedGroupings) {
            Attribute attribute = Expressions.attribute(prunedGrouping.grouping());
            if (attribute != null && requiredByAggregate.contains(attribute) == false) {
                removableAttributes.add(attribute);
            }
        }

        if (removableAttributes.isEmpty()) {
            return child;
        }

        List<Alias> remainingFields = eval.fields()
            .stream()
            .filter(alias -> removableAttributes.contains(alias.toAttribute()) == false)
            .toList();
        if (remainingFields.size() == eval.fields().size()) {
            return child;
        }
        return remainingFields.isEmpty() ? eval.child() : new Eval(eval.source(), eval.child(), remainingFields);
    }

    /**
     * The expression to re-emit above the aggregate in place of {@code grouping}, or {@code null} if the grouping must
     * stay: only a scalar constant, written inline or through an {@link Eval} alias, can be pruned.
     */
    private static Expression replacementFor(Expression grouping, AttributeMap<Expression> evalAliases) {
        Expression unwrapped = Alias.unwrap(grouping);
        if (isScalarFoldable(unwrapped)) {
            return unwrapped;
        }

        Attribute groupingAttribute = Expressions.attribute(grouping);
        if (groupingAttribute == null) {
            return null;
        }

        Expression definition = evalAliases.get(groupingAttribute);
        if (definition != null && isScalarFoldable(definition)) {
            return definition;
        }
        return null;
    }

    private static boolean isScalarFoldable(Expression expression) {
        if (expression.foldable() == false) {
            return false;
        }
        return expression.fold(FoldContext.small()) instanceof List<?> == false;
    }

    private static PrunedGrouping matchingPrunedGrouping(NamedExpression aggregateExpression, List<PrunedGrouping> prunedGroupings) {
        Attribute aggregateAttribute = aggregateExpression.toAttribute();
        Expression aggregateChild = Alias.unwrap(aggregateExpression);
        for (PrunedGrouping prunedGrouping : prunedGroupings) {
            Attribute groupingAttribute = Expressions.attribute(prunedGrouping.grouping());
            if ((groupingAttribute != null && aggregateAttribute.semanticEquals(groupingAttribute))
                || aggregateChild.semanticEquals(Alias.unwrap(prunedGrouping.grouping()))) {
                return prunedGrouping;
            }
        }
        return null;
    }

    private static Alias reconstruct(NamedExpression aggregateExpression, Expression replacement) {
        if (aggregateExpression instanceof Alias alias) {
            return alias.replaceChild(replacement);
        }
        return new Alias(
            aggregateExpression.source(),
            aggregateExpression.name(),
            replacement,
            aggregateExpression.id(),
            aggregateExpression.synthetic()
        );
    }

    private record PrunedGrouping(Expression grouping, Expression replacement) {}
}
