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
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.isIntegral;

/**
 * Removes {@code STATS BY} keys that do not add grouping cardinality and rebuilds their output above the aggregation.
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
        AttributeSet retainedGroupingAttributes = retainedGroupingAttributes(aggregate.groupings(), evalAliases);
        AttributeSet externalAttributes = externalAttributes(aggregate.child());
        List<Expression> newGroupings = new ArrayList<>(aggregate.groupings().size());
        List<PrunedGrouping> prunedGroupings = new ArrayList<>();

        for (Expression grouping : aggregate.groupings()) {
            Expression replacement = replacementFor(grouping, evalAliases, retainedGroupingAttributes, externalAttributes);
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

    private static AttributeSet retainedGroupingAttributes(List<Expression> groupings, AttributeMap<Expression> evalAliases) {
        AttributeSet.Builder retained = AttributeSet.builder();
        for (Expression grouping : groupings) {
            Attribute attribute = Expressions.attribute(grouping);
            if (attribute != null && evalAliases.containsKey(attribute) == false) {
                retained.add(attribute);
            }
        }
        return retained.build();
    }

    private static AttributeSet externalAttributes(LogicalPlan plan) {
        AttributeSet.Builder externalAttributes = AttributeSet.builder();
        plan.forEachDown(ExternalRelation.class, relation -> externalAttributes.addAll(relation.output()));
        return externalAttributes.build();
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

    private static Expression replacementFor(
        Expression grouping,
        AttributeMap<Expression> evalAliases,
        AttributeSet retainedGroupingAttributes,
        AttributeSet externalAttributes
    ) {
        Expression unwrapped = Alias.unwrap(grouping);
        if (isScalarFoldable(unwrapped)) {
            return unwrapped;
        }

        Attribute groupingAttribute = Expressions.attribute(grouping);
        if (groupingAttribute == null) {
            return null;
        }

        Expression definition = evalAliases.get(groupingAttribute);
        if (definition == null) {
            return null;
        }
        if (isScalarFoldable(definition)) {
            return definition;
        }

        Expression expanded = expandAliases(definition, evalAliases, retainedGroupingAttributes, new HashSet<>());
        if (isSafeDerivedExpression(expanded, retainedGroupingAttributes, externalAttributes)) {
            return expanded;
        }
        return null;
    }

    private static Expression expandAliases(
        Expression expression,
        AttributeMap<Expression> evalAliases,
        AttributeSet retainedGroupingAttributes,
        Set<Attribute> expanding
    ) {
        return expression.transformUp(Attribute.class, attribute -> {
            if (retainedGroupingAttributes.contains(attribute)) {
                return attribute;
            }
            Expression replacement = evalAliases.get(attribute);
            if (replacement == null || expanding.add(attribute) == false) {
                return attribute;
            }
            try {
                return expandAliases(replacement, evalAliases, retainedGroupingAttributes, expanding);
            } finally {
                expanding.remove(attribute);
            }
        });
    }

    private static boolean isSafeDerivedExpression(
        Expression expression,
        AttributeSet retainedGroupingAttributes,
        AttributeSet externalAttributes
    ) {
        AttributeSet references = expression.references();
        return references.isEmpty() == false
            && references.subsetOf(retainedGroupingAttributes)
            && references.subsetOf(externalAttributes)
            && references.stream().allMatch(PruneRedundantAggregateGroupings::isSafeIntegralAttribute)
            && expression.anyMatch(PruneRedundantAggregateGroupings::isUnsafeDerivedExpression) == false;
    }

    private static boolean isUnsafeDerivedExpression(Expression expression) {
        if (expression instanceof Attribute) {
            return false;
        }
        if (isScalarFoldable(expression)) {
            return false;
        }
        return expression instanceof Add == false && expression instanceof Sub == false && expression instanceof Neg == false;
    }

    private static boolean isSafeIntegralAttribute(Attribute attribute) {
        return isIntegral(attribute.dataType());
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
