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

    /**
     * Cap on alias substitutions while expanding one grouping. Each substitution recurses and re-expands shared aliases,
     * so an unbounded chain of {@code EVAL}s exhausts the stack or the heap and takes the node down
     * (https://github.com/elastic/elasticsearch/issues/150104). A grouping whose chain exceeds the cap is simply kept.
     * 100 is far above any hand-written chain and far below the depth that overflows a 1 MB thread stack.
     */
    static final int MAX_ALIAS_SUBSTITUTIONS = 100;

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        if (shouldSkipAggregate(aggregate)) {
            return aggregate;
        }

        AttributeMap<Expression> evalAliases = evalAliases(aggregate.child());
        AttributeSet retainedGroupingAttributes = retainedGroupingAttributes(aggregate.groupings(), evalAliases);
        AttributeSet externalAttributes = externalAttributes(aggregate.child());
        // A grouping key may be re-exposed under a different name in the aggregate output, e.g. a renamed column
        // `... | RENAME x AS y | STATS ... BY y` surfaces as `x AS y` in the aggregate's output. A pruned grouping is
        // rebuilt as an Eval above the aggregate, so its expression must read the aggregate's output attribute (`y`),
        // not the pre-aggregate attribute (`x`) which the aggregate no longer surfaces.
        AttributeMap<Attribute> groupingOutputAttributes = groupingOutputAttributes(aggregate.aggregates());
        List<Expression> newGroupings = new ArrayList<>(aggregate.groupings().size());
        List<PrunedGrouping> prunedGroupings = new ArrayList<>();

        for (Expression grouping : aggregate.groupings()) {
            Expression replacement = replacementFor(
                grouping,
                evalAliases,
                retainedGroupingAttributes,
                externalAttributes,
                groupingOutputAttributes
            );
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

    /**
     * Maps each attribute that the aggregate's output is built from to the attribute that actually exposes it. A direct
     * pass-through (e.g. {@code BY x} emitting {@code x}) maps to itself; a rename (e.g. {@code x AS y}) maps the
     * underlying attribute {@code x} to the output attribute {@code y}. This lets a pruned grouping be rebuilt above the
     * aggregate while reading the aggregate's output rather than a pre-aggregate attribute the aggregate no longer
     * surfaces.
     */
    private static AttributeMap<Attribute> groupingOutputAttributes(List<? extends NamedExpression> aggregates) {
        AttributeMap.Builder<Attribute> outputAttributes = AttributeMap.builder();
        for (NamedExpression aggregate : aggregates) {
            // A direct pass-through (e.g. `BY x` emitting `x`) takes precedence over a rename of the same underlying
            // attribute: put() always wins for the identity case, while computeIfAbsent() keeps an alias only when no
            // pass-through has claimed the key.
            if (aggregate instanceof Attribute attribute) {
                outputAttributes.put(attribute, attribute);
            } else if (aggregate instanceof Alias alias && alias.child() instanceof Attribute attribute) {
                outputAttributes.computeIfAbsent(attribute, key -> alias.toAttribute());
            }
        }
        return outputAttributes.build();
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

        AttributeSet.Builder prunedAttributes = AttributeSet.builder();
        for (PrunedGrouping prunedGrouping : prunedGroupings) {
            Attribute attribute = Expressions.attribute(prunedGrouping.grouping());
            if (attribute != null) {
                prunedAttributes.add(attribute);
            }
        }
        // Only a pruned alias may go, and only if nothing left behind reads it: a kept grouping such as b = a * 2, or a field
        // that stays regardless. Fields only reference earlier fields, so one pass from the end sees every remaining reader.
        AttributeSet.Builder required = Aggregate.computeReferences(newAggregates, newGroupings).asBuilder();
        List<Alias> fields = eval.fields();
        for (int i = fields.size() - 1; i >= 0; i--) {
            Attribute attribute = fields.get(i).toAttribute();
            if (prunedAttributes.contains(attribute) == false || required.contains(attribute)) {
                required.addAll(fields.get(i).child().references());
            }
        }
        AttributeSet.Builder removableAttributes = AttributeSet.builder();
        for (Attribute attribute : prunedAttributes.build()) {
            if (required.contains(attribute) == false) {
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
        AttributeSet externalAttributes,
        AttributeMap<Attribute> groupingOutputAttributes
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

        // Derived groupings are only prunable over external relations; don't expand when there is nothing to prune against.
        if (externalAttributes.isEmpty()) {
            return null;
        }

        Expression expanded = expandAliases(
            definition,
            evalAliases,
            retainedGroupingAttributes,
            new HashSet<>(),
            new int[] { MAX_ALIAS_SUBSTITUTIONS }
        );
        if (isSafeDerivedExpression(expanded, retainedGroupingAttributes, externalAttributes, groupingOutputAttributes)) {
            // The expression references the retained grouping attributes as they exist below the aggregate; rebind them
            // to the attributes the aggregate exposes so the rebuilt Eval above the aggregate stays consistent.
            return expanded.transformUp(Attribute.class, attribute -> groupingOutputAttributes.resolve(attribute, attribute));
        }
        return null;
    }

    /**
     * An alias left unexpanded, because of a cycle or an exhausted {@code remainingSubstitutions}, is not a retained
     * grouping attribute, so {@link #isSafeDerivedExpression} rejects the result and the grouping is kept.
     */
    private static Expression expandAliases(
        Expression expression,
        AttributeMap<Expression> evalAliases,
        AttributeSet retainedGroupingAttributes,
        Set<Attribute> expanding,
        int[] remainingSubstitutions
    ) {
        return expression.transformUp(Attribute.class, attribute -> {
            if (retainedGroupingAttributes.contains(attribute)) {
                return attribute;
            }
            Expression replacement = evalAliases.get(attribute);
            if (replacement == null || remainingSubstitutions[0] == 0 || expanding.add(attribute) == false) {
                return attribute;
            }
            remainingSubstitutions[0]--;
            try {
                return expandAliases(replacement, evalAliases, retainedGroupingAttributes, expanding, remainingSubstitutions);
            } finally {
                expanding.remove(attribute);
            }
        });
    }

    private static boolean isSafeDerivedExpression(
        Expression expression,
        AttributeSet retainedGroupingAttributes,
        AttributeSet externalAttributes,
        AttributeMap<Attribute> groupingOutputAttributes
    ) {
        AttributeSet references = expression.references();
        // The containsKey check requires every referenced attribute to be exposed by the aggregate output, otherwise the
        // rebuilt Eval above the aggregate would dangle on an attribute the aggregate no longer surfaces.
        return references.isEmpty() == false
            && references.subsetOf(retainedGroupingAttributes)
            && references.subsetOf(externalAttributes)
            && references.stream().allMatch(groupingOutputAttributes::containsKey)
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
