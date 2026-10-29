/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.NonDeterministic;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator.locallyUniqueTemporaryName;

/**
 * Copies the conjuncts of a filter above an {@link InlineJoin} that only reference the aggregated values into the right-hand side, on
 * top of its {@link Aggregate}. The right-hand side is executed first and its result is shipped to the data nodes as a
 * {@link LocalRelation}, so dropping the groups that the filter rejects anyway keeps that
 * result small. The filter stays above the join, where it removes the left rows whose group was dropped.
 * <pre>
 *     ... | INLINE STATS first_seen = MIN(@timestamp) BY host | WHERE @timestamp == first_seen AND first_seen &gt;= "2026-01-01"
 * </pre>
 * Only conjuncts that are false or null on a null aggregate value are copied: a left row whose group was dropped joins with nulls.
 * <p>
 * The copy evaluates the condition, including the aliases it references on the right-hand side, a second time. Nothing is copied
 * when that condition contains a {@link NonDeterministic} expression: the two evaluations could disagree, and dropping a group
 * would then also drop rows that the filter above the join keeps.
 * <p>
 * A row with a multi-valued key joins every group of its key values, and gets one aggregate value per group. Dropping one of those
 * groups changes the row's values, so groups that contain such a row are always kept. A group contains one exactly when
 * {@code COUNT(key)}, which counts every value of a multi-valued key, is greater than {@code COUNT(*)}. Every row of the left-hand
 * side has to reach the {@link Aggregate} for that check, so the rule only applies when nothing below the {@link Aggregate} drops rows.
 * <p>
 * Runs once after the operator optimizations converged, so that the filter above the join holds every conjunct that can reach it.
 * The copied condition references freshly named counts, so a fixed-point rule could not recognize it on the next iteration.
 */
public final class PushDownFilterIntoInlineJoinAggregate extends OptimizerRules.ParameterizedOptimizerRule<
    Filter,
    LogicalOptimizerContext> {

    public PushDownFilterIntoInlineJoinAggregate() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Filter filter, LogicalOptimizerContext ctx) {
        if (filter.child() instanceof InlineJoin ij && ij.config().leftFields().isEmpty() == false) {
            AttributeSet aggregatedValues = ij.right().outputSet().subtract(AttributeSet.of(ij.config().rightFields()));
            List<Expression> pushable = new ArrayList<>();
            for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
                if (conjunct.references().isEmpty() == false
                    && conjunct.references().subsetOf(aggregatedValues)
                    && PushDownAndCombineFilters.isRightPushableFilter(conjunct, ctx.foldCtx())) {
                    pushable.add(conjunct);
                }
            }
            if (pushable.isEmpty() == false) {
                LogicalPlan right = pushDownIntoAggregate(ij.right(), pushable);
                if (right != null) {
                    return filter.replaceChild(ij.replaceRight(right));
                }
            }
        }
        return filter;
    }

    /**
     * Returns the new right-hand side, or {@code null} if the filter cannot be pushed into it.
     */
    private static LogicalPlan pushDownIntoAggregate(LogicalPlan right, List<Expression> conditions) {
        Expression condition = Predicates.combineAnd(conditions);
        LogicalPlan plan = right;
        while (plan instanceof Aggregate == false) {
            if (plan instanceof Project project) {
                condition = inlineAliases(condition, project.projections());
            } else if (plan instanceof Eval eval) {
                condition = inlineAliases(condition, eval.fields());
            } else if (plan instanceof Filter == false) {
                return null;
            }
            plan = ((UnaryPlan) plan).child();
        }
        Aggregate aggregate = (Aggregate) plan;
        if (aggregate.getClass() != Aggregate.class
            || readsEveryRow(aggregate.child()) == false
            || condition.references().subsetOf(aggregate.outputSet()) == false
            || condition.anyMatch(NonDeterministic.class::isInstance)) {
            return null;
        }

        Source source = aggregate.source();
        Count countStar = new Count(source, Literal.keyword(source, StringUtils.WILDCARD));
        Alias countAll = findAggregate(aggregate, countStar);
        List<NamedExpression> aggregates = new ArrayList<>();
        if (countAll == null) {
            countAll = new Alias(source, locallyUniqueTemporaryName("count"), countStar);
            aggregates.add(countAll);
        }
        List<Expression> keepGroup = new ArrayList<>();
        keepGroup.add(condition);
        for (Expression grouping : aggregate.groupings()) {
            if (grouping instanceof Attribute == false) {
                return null;
            }
            Attribute key = (Attribute) grouping;
            Count countKey = new Count(source, key);
            if (countKey.resolved() == false) {
                return null;
            }
            Alias countKeyAlias = findAggregate(aggregate, countKey);
            if (countKeyAlias == null) {
                countKeyAlias = new Alias(source, locallyUniqueTemporaryName(key.name()), countKey);
                aggregates.add(countKeyAlias);
            }
            keepGroup.add(new GreaterThan(source, countKeyAlias.toAttribute(), countAll.toAttribute()));
        }
        aggregates.addAll(aggregate.aggregates());

        Filter filtered = new Filter(source, aggregate.with(aggregate.groupings(), aggregates), Predicates.combineOr(keepGroup));
        LogicalPlan replacement = new Project(source, filtered, aggregate.output());
        return right.transformUp(Aggregate.class, a -> a == aggregate ? replacement : a);
    }

    /**
     * Returns the alias of an aggregate that computes {@code function}, or {@code null} if there is none. The intermediate state of
     * an aggregation holds each distinct aggregate function once, so a second copy of an existing one misaligns the aggregators with
     * their state.
     */
    private static Alias findAggregate(Aggregate aggregate, Expression function) {
        for (NamedExpression aggregateExpression : aggregate.aggregates()) {
            if (aggregateExpression instanceof Alias alias && alias.child().semanticEquals(function)) {
                return alias;
            }
        }
        return null;
    }

    /**
     * Replaces the references to the given aliases with their definitions. Iterating backwards also replaces the references to an
     * alias that the definition of a later alias of the same {@link Eval} introduces.
     */
    private static Expression inlineAliases(Expression condition, List<? extends NamedExpression> namedExpressions) {
        for (int i = namedExpressions.size() - 1; i >= 0; i--) {
            if (namedExpressions.get(i) instanceof Alias alias) {
                Attribute attribute = alias.toAttribute();
                condition = condition.transformUp(ReferenceAttribute.class, r -> r.semanticEquals(attribute) ? alias.child() : r);
            }
        }
        return condition;
    }

    private static boolean readsEveryRow(LogicalPlan plan) {
        while (plan instanceof Eval || plan instanceof Project) {
            plan = ((UnaryPlan) plan).child();
        }
        return plan instanceof StubRelation;
    }
}
