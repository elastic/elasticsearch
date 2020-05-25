/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.optimizer;

import org.elasticsearch.xpack.eql.plan.logical.Join;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.eql.util.StringUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
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
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;

import java.util.Arrays;

public class Optimizer extends RuleExecutor<LogicalPlan> {

    public LogicalPlan optimize(LogicalPlan verified) {
        return verified.optimized() ? verified : execute(verified);
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch substitutions = new Batch("Operator Replacement", Limiter.ONCE,
                new ReplaceSurrogateFunction());
                
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
                new PruneLiteralsInOrderBy()
                );

        Batch local = new Batch("Skip Elasticsearch",
                new SkipEmptyFilter(),
                new SkipEmptyJoin());

        Batch label = new Batch("Set as Optimized", Limiter.ONCE,
                new SetAsOptimized());

        return Arrays.asList(substitutions, operators, local, label);
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
        protected LogicalPlan nonMatchingFilter(Filter filter) {
            return new LocalRelation(filter.source(), filter.output());
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
    
    static class SkipEmptyJoin extends OptimizerRule<Join> {

        @Override
        protected LogicalPlan rule(Join plan) {
            // check for empty filters
            for (KeyedFilter filter : plan.queries()) {
                if (filter.child() instanceof LocalRelation) {
                    return new LocalRelation(plan.source(), plan.output(), Results.Type.SEQUENCE);
                }
            }
            return plan;
        }
    }
}