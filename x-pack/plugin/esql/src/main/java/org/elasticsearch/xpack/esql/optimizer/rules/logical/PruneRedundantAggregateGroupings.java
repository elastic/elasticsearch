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
 * Two kinds of key qualify:
 * <ul>
 *   <li>A scalar constant: {@code STATS ... BY 1, x} or {@code EVAL c = 1 | STATS ... BY c, x} group on {@code x} alone and
 *       re-emit the constant in an {@link Eval} above the {@link Aggregate}. A constant is single-valued on every row, so this is
 *       always equivalent.</li>
 *   <li>A key <em>derived</em> from other keys, such as {@code EVAL m = x - 1 | STATS ... BY x, m}, when every key it reads is
 *       proven single-valued by {@link ValueCardinality}. The derived key is recomputed above the aggregate from the retained keys.</li>
 * </ul>
 * A derived key without that proof is kept, even though it looks functionally dependent. The dependency holds only while {@code x} is
 * single-valued: on a row where {@code x} is a list, the query as written evaluates {@code x - 1} to {@code null} (with a warning),
 * while the aggregate unrolls the list into one group per element. Recomputing {@code m} per group would then produce a number where
 * the query returns {@code null}, and the group set itself can differ (a list row and a scalar row with a shared element fall into one
 * group after the rewrite but two before it). The data type carries no single-value flag, so the proof has to come from the plan or
 * from a mapping guarantee, see {@link ValueCardinality}.
 * <p>
 * A derived key must also be built only from integral {@code +}, {@code -} and unary {@code -} over those keys and constants. Nothing
 * else is recomputed, because a function that is not listed may behave differently on the materialized key than on the row.
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
        List<Expression> groupings = aggregate.groupings();
        Expression[] replacements = new Expression[groupings.size()];
        AttributeSet.Builder keptKeys = AttributeSet.builder();
        for (int i = 0; i < replacements.length; i++) {
            replacements[i] = replacementFor(groupings.get(i), evalAliases);
        }
        // A derived key may only read keys that stay in the aggregate, so it is decided once the constants are known.
        for (int i = 0; i < replacements.length; i++) {
            if (replacements[i] == null && Expressions.attribute(groupings.get(i)) != null) {
                keptKeys.add(Expressions.attribute(groupings.get(i)));
            }
        }
        AttributeSet retainedKeys = keptKeys.build();
        AttributeMap<Attribute> groupingOutputs = null;
        for (int i = 0; i < replacements.length; i++) {
            if (replacements[i] == null) {
                if (groupingOutputs == null) {
                    groupingOutputs = groupingOutputAttributes(aggregate.aggregates());
                }
                replacements[i] = derivedReplacementFor(groupings.get(i), aggregate, evalAliases, retainedKeys, groupingOutputs);
            }
        }

        List<Expression> newGroupings = new ArrayList<>(groupings.size());
        List<PrunedGrouping> prunedGroupings = new ArrayList<>();
        for (int i = 0; i < replacements.length; i++) {
            if (replacements[i] == null) {
                newGroupings.add(groupings.get(i));
            } else {
                prunedGroupings.add(new PrunedGrouping(groupings.get(i), replacements[i]));
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

    /**
     * The expression to re-emit above the aggregate in place of {@code grouping}, or {@code null} if the grouping must stay: it is
     * built only from integral {@code +}, {@code -} and unary {@code -}, constants, and keys that stay in the aggregate and are
     * proven single-valued at the aggregate's input. References are rewritten to the attributes the aggregate outputs for those keys.
     */
    private static Expression derivedReplacementFor(
        Expression grouping,
        Aggregate aggregate,
        AttributeMap<Expression> evalAliases,
        AttributeSet retainedKeys,
        AttributeMap<Attribute> groupingOutputs
    ) {
        Expression unwrapped = Alias.unwrap(grouping);
        if (unwrapped instanceof Attribute attribute && evalAliases.containsKey(attribute) == false) {
            // A plain key with no definition is a key itself, not a function of other keys.
            return null;
        }
        // Bound the recursion: a query may chain thousands of aliases, and a longer chain than the parser allows is not worth following.
        int[] budget = { ExpressionBuilder.MAX_EXPRESSION_DEPTH };
        Expression expanded = expand(unwrapped, evalAliases, budget);
        if (expanded == null || expanded.references().isEmpty()) {
            return null;
        }
        if (expanded instanceof Attribute attribute && attribute.semanticEquals(Expressions.attribute(grouping))) {
            // The definition could not be expanded, so the key is a key of its own, not a function of other keys.
            return null;
        }
        for (Attribute leaf : expanded.references()) {
            if (isIntegral(leaf.dataType()) == false
                || retainedKeys.contains(leaf) == false
                || groupingOutputs.containsKey(leaf) == false
                || ValueCardinality.of(leaf, aggregate.child()) != ValueCardinality.SINGLE) {
                return null;
            }
        }
        // The aggregate may output a key under another name (a rename), and only that output exists above the aggregate.
        return expanded.transformUp(Attribute.class, attribute -> groupingOutputs.resolve(attribute, attribute));
    }

    /**
     * {@code expression} with the {@code EVAL} aliases it reads replaced by their definitions, or {@code null} if it is not made only
     * of integral {@code +}, {@code -}, unary {@code -}, scalar constants and attributes. An alias whose definition cannot be
     * expanded stays as an attribute: it is a key of its own and is judged as such by the caller.
     */
    private static Expression expand(Expression expression, AttributeMap<Expression> evalAliases, int[] budget) {
        if (--budget[0] < 0) {
            return null;
        }
        if (isScalarFoldable(expression)) {
            return expression;
        }
        if (expression instanceof Attribute attribute) {
            Expression definition = evalAliases.get(attribute);
            if (definition == null) {
                return attribute;
            }
            Expression expanded = expand(definition, evalAliases, budget);
            return expanded != null ? expanded : attribute;
        }
        if ((expression instanceof Add || expression instanceof Sub || expression instanceof Neg) && isIntegral(expression.dataType())) {
            List<Expression> children = expression.children();
            List<Expression> expandedChildren = new ArrayList<>(children.size());
            for (Expression child : children) {
                Expression expanded = expand(child, evalAliases, budget);
                if (expanded == null) {
                    return null;
                }
                expandedChildren.add(expanded);
            }
            return expression.replaceChildren(expandedChildren);
        }
        return null;
    }

    /**
     * For each attribute the aggregate reads as a key, the attribute the aggregate outputs for it, which is a different one when
     * the output renames the key.
     */
    private static AttributeMap<Attribute> groupingOutputAttributes(List<? extends NamedExpression> aggregates) {
        AttributeMap.Builder<Attribute> outputAttributes = AttributeMap.builder();
        for (NamedExpression aggregate : aggregates) {
            // A direct pass-through (e.g. `BY x` emitting `x`) takes precedence over a rename of the same underlying attribute:
            // put() always wins for the identity case, while computeIfAbsent() keeps an alias only when no pass-through claimed it.
            if (aggregate instanceof Attribute attribute) {
                outputAttributes.put(attribute, attribute);
            } else if (aggregate instanceof Alias alias && alias.child() instanceof Attribute attribute) {
                outputAttributes.computeIfAbsent(attribute, key -> alias.toAttribute());
            }
        }
        return outputAttributes.build();
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
