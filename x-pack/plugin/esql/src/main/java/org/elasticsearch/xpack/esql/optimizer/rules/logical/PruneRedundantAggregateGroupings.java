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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.parser.ExpressionBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.isIntegral;

/**
 * Removes {@code STATS BY} keys that do not add grouping cardinality and rebuilds their output above the aggregation. When it
 * prunes, it also drops fields of the {@code EVAL} directly below the aggregate that nothing reads any more.
 */
public final class PruneRedundantAggregateGroupings extends OptimizerRules.OptimizerRule<Aggregate>
    implements
        OptimizerRules.LocalAware<Aggregate> {

    /**
     * How many nodes expanding one derived grouping may visit. The expansion never recurses deeper than the nodes it has
     * visited, and with constants folded each visit adds at most one node to the rebuilt expression, so this caps both the
     * recursion and the depth and size of the expression rebuilt above the aggregate. Tying it to the parser keeps the rule
     * from building an expression deeper than one a query could spell out, whatever the length of the alias chain behind it.
     * Literals and alias hops count as visits too, so a grouping is kept well before its expansion reaches that depth: a chain
     * of {@code - 1} links costs three visits per link.
     */
    private static final int MAX_DERIVED_EXPANSION_NODES = ExpressionBuilder.MAX_EXPRESSION_DEPTH;

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

        // Only external groupings can back a pruned derived grouping, so without any there is nothing worth expanding.
        if (externalAttributes.isEmpty()) {
            return null;
        }
        return new DerivedExpansion(evalAliases, retainedGroupingAttributes, externalAttributes, groupingOutputAttributes).rebuild(
            definition
        );
    }

    /**
     * Rebuilds a derived grouping from the retained groupings it is computed from, or gives up so the grouping is kept. It
     * gives up as soon as the definition reads anything but retained, external, integral groupings, uses anything but
     * addition, subtraction and negation over them, or needs more than {@link #MAX_DERIVED_EXPANSION_NODES} visits. Checking
     * while building means an unprunable grouping costs at most that many visits, however long the alias chain behind it.
     */
    private static final class DerivedExpansion {
        private final AttributeMap<Expression> evalAliases;
        private final AttributeSet retainedGroupingAttributes;
        private final AttributeSet externalAttributes;
        private final AttributeMap<Attribute> groupingOutputAttributes;
        private final Set<Attribute> expanding = new HashSet<>();
        private int remainingVisits = MAX_DERIVED_EXPANSION_NODES;
        private boolean readsRetainedGrouping;

        DerivedExpansion(
            AttributeMap<Expression> evalAliases,
            AttributeSet retainedGroupingAttributes,
            AttributeSet externalAttributes,
            AttributeMap<Attribute> groupingOutputAttributes
        ) {
            this.evalAliases = evalAliases;
            this.retainedGroupingAttributes = retainedGroupingAttributes;
            this.externalAttributes = externalAttributes;
            this.groupingOutputAttributes = groupingOutputAttributes;
        }

        /** The rebuilt grouping, or {@code null} to keep it. */
        Expression rebuild(Expression definition) {
            Expression expanded = expand(definition);
            return expanded != null && readsRetainedGrouping ? expanded : null;
        }

        private Expression expand(Expression expression) {
            if (remainingVisits == 0) {
                return null;
            }
            remainingVisits--;
            if (expression instanceof Attribute attribute) {
                return expandAttribute(attribute);
            }
            if (expression instanceof Add || expression instanceof Sub || expression instanceof Neg) {
                List<Expression> children = new ArrayList<>(expression.children().size());
                for (Expression child : expression.children()) {
                    Expression expandedChild = expand(child);
                    if (expandedChild == null) {
                        return null;
                    }
                    children.add(expandedChild);
                }
                return expression.replaceChildrenSameSize(children);
            }
            if (expression.foldable() == false) {
                return null;
            }
            // Folded rather than kept whole, so that this visit adds a single node to the rebuilt expression.
            Object value = expression.fold(FoldContext.small());
            return value instanceof List<?> ? null : Literal.of(expression, value);
        }

        private Expression expandAttribute(Attribute attribute) {
            if (retainedGroupingAttributes.contains(attribute)) {
                // The rebuilt Eval sits above the aggregate, so a grouping it reads must be one the aggregate exposes, possibly
                // under a rename; otherwise the rebuilt expression would dangle.
                if (externalAttributes.contains(attribute) == false
                    || isIntegral(attribute.dataType()) == false
                    || groupingOutputAttributes.containsKey(attribute) == false) {
                    return null;
                }
                readsRetainedGrouping = true;
                return groupingOutputAttributes.resolve(attribute, attribute);
            }
            Expression definition = evalAliases.get(attribute);
            if (definition == null || expanding.add(attribute) == false) {
                return null;
            }
            try {
                return expand(definition);
            } finally {
                expanding.remove(attribute);
            }
        }
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
