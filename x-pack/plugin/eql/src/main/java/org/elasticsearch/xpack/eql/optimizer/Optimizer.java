/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.optimizer;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.ToString;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveBinaryComparison;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveWildcardEquals;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveWildcardNotEquals;
import org.elasticsearch.xpack.eql.plan.logical.Join;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.LimitWithOffset;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.eql.session.Payload.Type;
import org.elasticsearch.xpack.eql.util.MathUtils;
import org.elasticsearch.xpack.eql.util.StringUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.Order.NullsPosition;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BinaryComparisonSimplification;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanFunctionEqualsElimination;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanSimplification;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.CombineBinaryComparisons;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.CombineDisjunctionsToIn;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.ConstantFolding;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.LiteralsOnTheRight;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.OptimizerRule;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PropagateEquals;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PruneLiteralsInOrderBy;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PushDownAndCombineFilters;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.ReplaceSurrogateFunction;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SetAsOptimized;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SimplifyComparisonsArithmetics;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.TransformDirection;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PropagateNullable;

public class Optimizer extends RuleExecutor<LogicalPlan> {

    public LogicalPlan optimize(LogicalPlan verified) {
        return verified.optimized() ? verified : execute(verified);
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch substitutions = new Batch("Substitution", Limiter.ONCE,
                new ReplaceWildcards(),
                new ReplaceSurrogateFunction(),
                new ReplaceRegexMatch(),
                new ReplaceNullChecks());

        Batch operators = new Batch("Operator Optimization",
                new ConstantFolding(),
                // boolean
                new BooleanSimplification(),
                new LiteralsOnTheRight(),
                new BinaryComparisonSimplification(),
                new BooleanFunctionEqualsElimination(),
                // needs to occur before BinaryComparison combinations
                new PropagateEquals(),
                new PropagateNullable(),
                new CombineBinaryComparisons(),
                new CombineDisjunctionsToIn(),
                new SimplifyComparisonsArithmetics(DataTypes::areCompatible),
                // prune/elimination
                new PruneFilters(),
                new PruneLiteralsInOrderBy(),
                new PruneCast(),
                new CombineLimits(),
                new PushDownAndCombineFilters()
            );

        Batch constraints = new Batch("Infer constraints", Limiter.ONCE,
                new PropagateJoinKeyConstraints());

        Batch ordering = new Batch("Implicit Order",
                new SortByLimit(),
                new PushDownOrderBy());

        Batch local = new Batch("Skip Elasticsearch",
                new SkipEmptyFilter(),
                new SkipEmptyJoin(),
                new SkipQueryOnLimitZero());

        Batch label = new Batch("Set as Optimized", Limiter.ONCE,
                new SetAsOptimized());

        return asList(substitutions, operators, constraints, operators, ordering, local, label);
    }

    private static class ReplaceWildcards extends OptimizerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {
            return filter.transformExpressionsUp(InsensitiveBinaryComparison.class, cmp -> {
                Expression result = cmp;
                if (cmp instanceof InsensitiveWildcardEquals || cmp instanceof InsensitiveWildcardNotEquals) {
                    // expr : "wildcard*phrase?" || expr !: "wildcard*phrase?"
                    Expression target = null;
                    String wildString = null;

                    // check only the right side
                    if (isWildcard(cmp.right())) {
                        wildString = (String) cmp.right().fold();
                        target = cmp.left();
                    }

                    if (target != null) {
                        Expression like = new Like(cmp.source(), target, StringUtils.toLikePattern(wildString), true);
                        if (cmp instanceof InsensitiveWildcardNotEquals) {
                            like = new Not(cmp.source(), like);
                        }

                        result = like;
                    }
                }
                return result;
            });
        }

        private static boolean isWildcard(Expression expr) {
            if (expr instanceof Literal) {
                Object value = expr.fold();
                if (value instanceof String) {
                    String string = (String) value;
                    return string.contains("*") || string.contains("?");
                }
            }
            return false;
        }
    }

    private static class ReplaceNullChecks extends OptimizerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {

            return filter.transformExpressionsUp(BinaryComparison.class, cmp -> {
                Expression result = cmp;
                // expr == null || expr != null
                if (cmp instanceof Equals || cmp instanceof NotEquals) {
                    Expression comparableToNull = null;
                    if (cmp.right().foldable() && cmp.right().fold() == null) {
                        comparableToNull = cmp.left();
                    } else if (cmp.left().foldable() && cmp.left().fold() == null) {
                        comparableToNull = cmp.right();
                    }
                    if (comparableToNull != null) {
                        if (cmp instanceof Equals) {
                            result = new IsNull(cmp.source(), comparableToNull);
                        } else {
                            result = new IsNotNull(cmp.source(), comparableToNull);
                        }
                    }
                }
                return result;
            });
        }
    }

    static class ReplaceRegexMatch extends org.elasticsearch.xpack.ql.optimizer.OptimizerRules.ReplaceRegexMatch {
        @Override
        protected Expression regexToEquals(RegexMatch<?> regexMatch, Literal literal) {
            return regexMatch.caseInsensitive()
                ? new InsensitiveEquals(regexMatch.source(), regexMatch.field(), literal, null)
                : new Equals(regexMatch.source(), regexMatch.field(), literal);
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
                return new LocalRelation(plan.source(), plan.output(), Type.EVENT);
            }
            return plan;
        }
    }

    static class PruneCast extends OptimizerRules.PruneCast<ToString> {

        PruneCast() {
            super(ToString.class);
        }

        @Override
        protected Expression maybePruneCast(ToString cast) {
            return cast.dataType().equals(cast.value().dataType()) ? cast.value() : cast;
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
     * Any condition applied on a join/sequence key, gets propagated to all rules.
     */
    static class PropagateJoinKeyConstraints extends OptimizerRule<Join> {

        static class Constraint {
            private final Expression condition;
            private final KeyedFilter keyedFilter;
            private final int keyPosition;

            Constraint(Expression condition, KeyedFilter filter, int keyPosition) {
                this.condition = condition;
                this.keyedFilter = filter;
                this.keyPosition = keyPosition;
            }

            Expression constraintFor(KeyedFilter keyed) {
                if (keyed == keyedFilter) {
                    return null;
                }

                Expression localKey = keyed.keys().get(keyPosition);
                Expression key = keyedFilter.keys().get(keyPosition);

                Expression newCond = condition.transformDown(e -> key.semanticEquals(e) ? localKey : e);
                return newCond;
            }

            @Override
            public String toString() {
                return condition.toString();
            }
        }

        @Override
        protected LogicalPlan rule(Join join) {
            List<Constraint> constraints = new ArrayList<>();

            // collect constraints for each filter
            join.queries().forEach(k ->
                k.forEachDown(Filter.class, f -> constraints.addAll(detectKeyConstraints(f.condition(), k))
                ));

            if (constraints.isEmpty() == false) {
                List<KeyedFilter> queries = join.queries().stream()
                        .map(k -> addConstraint(k, constraints))
                        .collect(toList());

                join = join.with(queries, join.until(), join.direction());
            }

            return join;
        }

        private List<Constraint> detectKeyConstraints(Expression condition, KeyedFilter filter) {
            List<Constraint> constraints = new ArrayList<>();
            List<? extends NamedExpression> keys = filter.keys();

            List<Expression> and = Predicates.splitAnd(condition);
            for (Expression exp : and) {
                // if there are no conjunction and at least one key matches, save the expression along with the key
                // and its ordinal so it can be replaced
                if (exp.anyMatch(Or.class::isInstance) == false) {
                    // comparisons against variables are not done
                    // hence why on the first key match, the expression is picked up
                    exp.anyMatch(e -> {
                        for (int i = 0; i < keys.size(); i++) {
                            Expression key = keys.get(i);
                            if (e.semanticEquals(key)) {
                                constraints.add(new Constraint(exp, filter, i));
                                return true;
                            }
                        }
                        return false;
                    });
                }
            }
            return constraints;
        }

        // adapt constraint to the given filter by replacing the keys accordingly in the expressions
        private KeyedFilter addConstraint(KeyedFilter k, List<Constraint> constraints) {
            Expression constraint = Predicates.combineAnd(constraints.stream()
                .map(c -> c.constraintFor(k))
                .filter(Objects::nonNull)
                .collect(toList()));

            return constraint != null
                    ? new KeyedFilter(k.source(), new Filter(k.source(), k.child(), constraint), k.keys(), k.timestamp(), k.tiebreaker())
                    : k;
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

                    // the main reason DESC is used is the lack of search_before (which is emulated through search_after + ASC)
                    // see https://github.com/elastic/elasticsearch/issues/62118
                    List<Order> ascendingOrders = changeOrderDirection(orderBy.order(), OrderDirection.ASC);
                    // preserve the order direction as is (can be DESC) for the base query
                    List<KeyedFilter> orderedQueries = new ArrayList<>(queries.size());
                    boolean baseFilter = true;
                    for (KeyedFilter filter : queries) {
                        // preserve the order for the base query, everything else needs to be ascending
                        List<Order> pushedOrder = baseFilter ? orderBy.order() : ascendingOrders;
                        OrderBy order = new OrderBy(filter.source(), filter.child(), pushedOrder);
                        orderedQueries.add(filter.replaceChild(order));
                        baseFilter = false;
                    }

                    KeyedFilter until = join.until();
                    OrderBy order = new OrderBy(until.source(), until.child(), ascendingOrders);
                    until = until.replaceChild(order);

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
            return child instanceof Filter || child instanceof Join;
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
                    return new LocalRelation(plan.source(), plan.output(), Type.SEQUENCE);
                }
            }
            return plan;
        }
    }
}
