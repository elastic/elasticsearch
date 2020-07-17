/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.optimizer;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.plan.logical.Join;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.LimitWithOffset;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.eql.util.MathUtils;
import org.elasticsearch.xpack.eql.util.StringUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.Order.NullsPosition;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanEqualsSimplification;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanLiteralsOnTheRight;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanSimplification;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.CombineBinaryComparisons;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.ConstantFolding;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.OptimizerRule;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PropagateEquals;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PruneLiteralsInOrderBy;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.ReplaceSurrogateFunction;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SetAsOptimized;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.TransformDirection;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;

public class Optimizer extends RuleExecutor<LogicalPlan> {

    public LogicalPlan optimize(LogicalPlan verified) {
        return verified.optimized() ? verified : execute(verified);
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch substitutions = new Batch("Operator Replacement", Limiter.ONCE, new ReplaceSurrogateFunction());

        Batch operators = new Batch("Operator Optimization",
                new ConstantFolding(),
                // boolean
                new BooleanSimplification(),
                new BooleanLiteralsOnTheRight(),
                new BooleanEqualsSimplification(),
                // needs to occur before BinaryComparison combinations
                new ReplaceWildcards(),
                new ReplaceNullChecks(),
                new PropagateEquals(),
                new CombineBinaryComparisons(),
                // prune/elimination
                new PruneFilters(),
                new PruneLiteralsInOrderBy(),
                new CombineLimits());

        Batch ordering = new Batch("Implicit Order",
                new SortByLimit(),
                new PushDownOrderBy());

        Batch local = new Batch("Skip Elasticsearch",
                new SkipEmptyFilter(),
                new SkipEmptyJoin(),
                new SkipQueryOnLimitZero());

        Batch label = new Batch("Set as Optimized", Limiter.ONCE,
                new SetAsOptimized());

        return Arrays.asList(substitutions, operators, ordering, local, label);
    }


    private static class ReplaceWildcards extends OptimizerRule<Filter> {

        private static boolean isWildcard(Expression expr) {
            if (expr.foldable()) {
                Object value = expr.fold();
                return value instanceof String && value.toString().contains("*");
            }
            return false;
        }

        @Override
        protected LogicalPlan rule(Filter filter) {
            return filter.transformExpressionsUp(e -> {
                // expr == "wildcard*phrase" || expr != "wildcard*phrase"
                if (e instanceof Equals || e instanceof NotEquals) {
                    BinaryComparison cmp = (BinaryComparison) e;

                    if (isWildcard(cmp.right())) {
                        String wcString = cmp.right().fold().toString();
                        Expression like = new Like(e.source(), cmp.left(), StringUtils.toLikePattern(wcString));

                        if (e instanceof NotEquals) {
                            like = new Not(e.source(), like);
                        }

                        e = like;
                    }
                }

                return e;
            });
        }
    }

    private static class ReplaceNullChecks extends OptimizerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {

            return filter.transformExpressionsUp(e -> {
                // expr == null || expr != null
                if (e instanceof Equals || e instanceof NotEquals) {
                    BinaryComparison cmp = (BinaryComparison) e;

                    if (cmp.right().foldable() && cmp.right().fold() == null) {
                        if (e instanceof Equals) {
                            e = new IsNull(e.source(), cmp.left());
                        } else {
                            e = new IsNotNull(e.source(), cmp.left());
                        }
                    }
                }

                return e;
            });
        }
    }

    static class PruneFilters extends org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PruneFilters {

        @Override
        protected LogicalPlan skipPlan(Filter filter) {
            return Optimizer.skipPlan(filter);
        }
    }

    static class SkipEmptyFilter extends OptimizerRule<UnaryPlan> {

        SkipEmptyFilter() {
            super(TransformDirection.UP);
        }

        @Override
        protected LogicalPlan rule(UnaryPlan plan) {
            if ((plan instanceof KeyedFilter) == false && plan.child() instanceof LocalRelation) {
                return new LocalRelation(plan.source(), plan.output(), Results.Type.SEARCH_HIT);
            }
            return plan;
        }
    }

    static class SkipQueryOnLimitZero extends org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SkipQueryOnLimitZero {

        @Override
        protected LogicalPlan skipPlan(Limit limit) {
            return Optimizer.skipPlan(limit);
        }
    }

    private static LogicalPlan skipPlan(UnaryPlan plan) {
        return new LocalRelation(plan.source(), plan.output());
    }

    /**
     * Combine tail and head into one limit.
     * The rules moves up since the first limit is the one that defines whether it's the head (positive) or
     * the tail (negative) limit of the data and the rest simply work in this space.
     */
    static final class CombineLimits extends OptimizerRule<LimitWithOffset> {

        CombineLimits() {
            super(TransformDirection.UP);
        }

        @Override
        protected LogicalPlan rule(LimitWithOffset limit) {
            // bail out early
            if (limit.child() instanceof LimitWithOffset == false) {
                return limit;
            }

            LimitWithOffset primary = (LimitWithOffset) limit.child();

            int primaryLimit = (Integer) primary.limit().fold();
            int primaryOffset = primary.offset();
            // +1 means ASC, -1 descending and 0 if there are no results
            int sign = Integer.signum(primaryLimit);

            int secondaryLimit = (Integer) limit.limit().fold();
            if (limit.offset() != 0) {
                throw new EqlIllegalArgumentException("Limits with different offset not implemented yet");
            }

            // for the same direction
            if (primaryLimit > 0 && secondaryLimit > 0) {
                // consider the minimum
                primaryLimit = Math.min(primaryLimit, secondaryLimit);
            } else if (primaryLimit < 0 && secondaryLimit < 0) {
                primaryLimit = Math.max(primaryLimit, secondaryLimit);
            } else {
                // the secondary limit cannot go beyond the primary - if it does it gets ignored
                if (MathUtils.abs(secondaryLimit) < MathUtils.abs(primaryLimit)) {
                    primaryOffset += MathUtils.abs(primaryLimit + secondaryLimit);
                    // preserve order
                    primaryLimit = MathUtils.abs(secondaryLimit) * sign;
                }
            }

            Literal literal = new Literal(primary.limit().source(), primaryLimit, DataTypes.INTEGER);
            return new LimitWithOffset(primary.source(), literal, primaryOffset, primary.child());
        }
    }

    /**
     * Align the implicit order with the limit (head means ASC or tail means DESC).
     */
    static final class SortByLimit extends OptimizerRule<LimitWithOffset> {

        @Override
        protected LogicalPlan rule(LimitWithOffset limit) {
            if (limit.limit().foldable()) {
                LogicalPlan child = limit.child();
                if (child instanceof OrderBy) {
                    OrderBy ob = (OrderBy) child;
                    if (PushDownOrderBy.isDefaultOrderBy(ob)) {
                        int l = (Integer) limit.limit().fold();
                        OrderDirection direction = Integer.signum(l) > 0 ? OrderDirection.ASC : OrderDirection.DESC;
                        ob = new OrderBy(ob.source(), ob.child(), PushDownOrderBy.changeOrderDirection(ob.order(), direction));
                        limit = new LimitWithOffset(limit.source(), limit.limit(), limit.offset(), ob);
                    }
                }
            }
            
            return limit;
        }
    }

    /**
     * Push down the OrderBy into the actual queries before translating them.
     * There is always an implicit order (timestamp + tiebreaker ascending).
     */
    static final class PushDownOrderBy extends OptimizerRule<OrderBy> {

        @Override
        protected LogicalPlan rule(OrderBy orderBy) {
            LogicalPlan plan = orderBy;
            if (isDefaultOrderBy(orderBy)) {
                LogicalPlan child = orderBy.child();
                //
                // When dealing with sequences, the matching needs to happen ascending
                // hence why the queries will always be ascending
                // but if the order is descending, apply that only to the first query
                // which is used to discover the window for which matching is being applied.
                //
                if (child instanceof Join) {
                    Join join = (Join) child;
                    List<KeyedFilter> queries = join.queries();

                    // the main reason ASC is used is the lack of search_before (which is emulated through search_after + ASC)
                    List<Order> ascendingOrders = changeOrderDirection(orderBy.order(), OrderDirection.ASC);
                    // preserve the order direction as is (can be DESC) for the base query
                    List<KeyedFilter> orderedQueries = new ArrayList<>(queries.size());
                    boolean baseFilter = true;
                    for (KeyedFilter filter : queries) {
                        // preserve the order for the base query, everything else needs to be ascending
                        List<Order> pushedOrder = baseFilter ? orderBy.order() : ascendingOrders;
                        OrderBy order = new OrderBy(filter.source(), filter.child(), pushedOrder);
                        orderedQueries.add((KeyedFilter) filter.replaceChildren(singletonList(order)));
                        baseFilter = false;
                    }

                    KeyedFilter until = join.until();
                    OrderBy order = new OrderBy(until.source(), until.child(), ascendingOrders);
                    until = (KeyedFilter) until.replaceChildren(singletonList(order));

                    OrderDirection direction = orderBy.order().get(0).direction();
                    plan = join.with(orderedQueries, until, direction);
                }
            }
            return plan;
        }

        private static boolean isDefaultOrderBy(OrderBy orderBy) {
            LogicalPlan child = orderBy.child();
            // the default order by is the first pipe
            // so it has to be on top of a event query or join/sequence
            return child instanceof Project || child instanceof Join;
        }

        private static List<Order> changeOrderDirection(List<Order> orders, Order.OrderDirection direction) {
            List<Order> changed = new ArrayList<>(orders.size());
            boolean hasChanged = false;
            for (Order order : orders) {
                if (order.direction() != direction) {
                    order = new Order(order.source(), order.child(), direction,
                            direction == OrderDirection.ASC ? NullsPosition.FIRST : NullsPosition.LAST);
                    hasChanged = true;
                }
                changed.add(order);
            }
            return hasChanged ? changed : orders;
        }
    }

    static class SkipEmptyJoin extends OptimizerRule<Join> {

        @Override
        protected LogicalPlan rule(Join plan) {
            // check for empty filters
            for (KeyedFilter filter : plan.queries()) {
                if (filter.anyMatch(LocalRelation.class::isInstance)) {
                    return new LocalRelation(plan.source(), plan.output(), Results.Type.SEQUENCE);
                }
            }
            return plan;
        }
    }
}