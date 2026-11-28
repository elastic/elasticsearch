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
import org.elasticsearch.xpack.esql.parser.ExpressionBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.isIntegral;

/**
 * Removes {@code STATS BY} keys that do not add grouping cardinality and rebuilds their output above the aggregation. When it
 * prunes, it also drops fields of the {@code EVAL} directly below the aggregate that nothing reads any more.
 * <p>
 * Two kinds of key qualify. A scalar constant ({@code STATS ... BY 1, x}) is always single-valued, so it is re-emitted in an
 * {@link Eval} above the {@link Aggregate}. A key <em>derived</em> from other keys with {@code +}, {@code -} or unary {@code -}
 * (e.g. {@code EVAL m = x - 1 | STATS ... BY x, m}) is also re-emitted, but only if {@link ValueCardinality} proves every key it
 * reads single-valued.
 * <p>
 * The proof matters because the dependency holds only while {@code x} is single-valued: on a row where {@code x} is a list, the
 * query as written evaluates {@code x - 1} to {@code null} (with a warning), while the aggregate unrolls the list into one group per
 * element. Recomputing {@code m} per group would then produce a number where the query returns {@code null}, and the group set itself
 * can differ (a list row and a scalar row with a shared element fall into one group after the rewrite but two before it). A derived
 * key without the proof is kept.
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
                aggregate.child(),
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
            pruneUnusedChildEvals(aggregate.child(), newGroupings, newAggregates),
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
            if (attribute == null) {
                continue;
            }
            Expression definition = evalAliases.get(attribute);
            // A key defined by an expression this rule cannot recompute, such as MV_MIN(x), stays in the aggregate. Other keys can be
            // derived from it, if it is proven single-valued.
            if (definition == null
                || (isScalarFoldable(definition) == false
                    && definition.anyMatch(PruneRedundantAggregateGroupings::isUnsafeDerivedExpression))) {
                retained.add(attribute);
            }
        }
        return retained.build();
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
        List<Expression> newGroupings,
        List<NamedExpression> newAggregates
    ) {
        if (!(child instanceof Eval eval)) {
            return child;
        }

        // Only the aggregate reads this Eval, so a field is needed only if the aggregate or a needed field after it reads it,
        // such as a kept grouping `b = a * 2` reading a pruned `a`. A field reads only fields before it, so walking backwards
        // settles each field after all of its readers.
        AttributeSet.Builder required = Aggregate.computeReferences(newAggregates, newGroupings).asBuilder();
        List<Alias> fields = eval.fields();
        List<Alias> remainingFields = new ArrayList<>(fields.size());
        for (int i = fields.size() - 1; i >= 0; i--) {
            Alias field = fields.get(i);
            if (required.contains(field.toAttribute())) {
                required.addAll(field.child().references());
                remainingFields.add(field);
            }
        }
        if (remainingFields.size() == fields.size()) {
            return child;
        }
        Collections.reverse(remainingFields);
        return remainingFields.isEmpty() ? eval.child() : new Eval(eval.source(), eval.child(), remainingFields);
    }

    private static Expression replacementFor(
        Expression grouping,
        AttributeMap<Expression> evalAliases,
        AttributeSet retainedGroupingAttributes,
        LogicalPlan input,
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

        // Bound the expansion by the nodes it visits: a query may chain thousands of aliases, and a longer chain than the parser
        // allows is not worth following.
        Expression expanded = expandAliases(
            definition,
            evalAliases,
            retainedGroupingAttributes,
            new int[] { ExpressionBuilder.MAX_EXPRESSION_DEPTH }
        );
        if (expanded == null) {
            return null;
        }
        if (isSafeDerivedExpression(expanded, retainedGroupingAttributes, input, groupingOutputAttributes)) {
            // The expression references the retained grouping attributes as they exist below the aggregate; rebind them
            // to the attributes the aggregate exposes so the rebuilt Eval above the aggregate stays consistent.
            return expanded.transformUp(Attribute.class, attribute -> groupingOutputAttributes.resolve(attribute, attribute));
        }
        return null;
    }

    /**
     * {@code expression} with the {@code EVAL} aliases it reads replaced by their definitions, down to the retained grouping
     * attributes. Returns {@code null} if the budget runs out or if it holds anything but {@code +}, {@code -}, unary {@code -},
     * scalar constants and attributes. An alias whose definition cannot be expanded stays an attribute, which is not a retained key,
     * so {@link #isSafeDerivedExpression} rejects it.
     */
    private static Expression expandAliases(
        Expression expression,
        AttributeMap<Expression> evalAliases,
        AttributeSet retainedGroupingAttributes,
        int[] budget
    ) {
        if (--budget[0] < 0) {
            return null;
        }
        if (expression instanceof Attribute attribute) {
            if (retainedGroupingAttributes.contains(attribute)) {
                return attribute;
            }
            Expression definition = evalAliases.get(attribute);
            if (definition == null) {
                return attribute;
            }
            Expression expanded = expandAliases(definition, evalAliases, retainedGroupingAttributes, budget);
            return expanded != null ? expanded : attribute;
        }
        if (isScalarFoldable(expression)) {
            return expression;
        }
        if (expression instanceof Add == false && expression instanceof Sub == false && expression instanceof Neg == false) {
            return null;
        }
        List<Expression> children = new ArrayList<>(expression.children().size());
        for (Expression child : expression.children()) {
            Expression expanded = expandAliases(child, evalAliases, retainedGroupingAttributes, budget);
            if (expanded == null) {
                return null;
            }
            children.add(expanded);
        }
        return expression.replaceChildren(children);
    }

    private static boolean isSafeDerivedExpression(
        Expression expression,
        AttributeSet retainedGroupingAttributes,
        LogicalPlan input,
        AttributeMap<Attribute> groupingOutputAttributes
    ) {
        AttributeSet references = expression.references();
        // The containsKey check requires every referenced attribute to be exposed by the aggregate output, otherwise the
        // rebuilt Eval above the aggregate would dangle on an attribute the aggregate no longer surfaces.
        return references.isEmpty() == false
            && references.subsetOf(retainedGroupingAttributes)
            && references.stream().allMatch(attribute -> ValueCardinality.ofOutput(attribute, input) == ValueCardinality.SINGLE)
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
