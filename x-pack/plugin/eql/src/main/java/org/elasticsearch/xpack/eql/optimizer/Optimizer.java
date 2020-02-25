/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.optimizer;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanLiteralsOnTheRight;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanSimplification;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.CombineBinaryComparisons;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.ConstantFolding;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.OptimizerRule;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PropagateEquals;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PruneFilters;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PruneLiteralsInOrderBy;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SetAsOptimized;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;

import java.util.Arrays;

public class Optimizer extends RuleExecutor<LogicalPlan> {

    public LogicalPlan optimize(LogicalPlan verified) {
        return verified.optimized() ? verified : execute(verified);
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch operators = new Batch("Operator Optimization",
                new ConstantFolding(),
                // boolean
                new BooleanSimplification(),
                new BooleanLiteralsOnTheRight(),
                // needs to occur before BinaryComparison combinations
                new ReplaceWildcards(),
                new PropagateEquals(),
                new CombineBinaryComparisons(),
                // prune/elimination
                new PruneFilters(),
                new PruneLiteralsInOrderBy()
                );

        Batch label = new Batch("Set as Optimized", Limiter.ONCE,
                new SetAsOptimized());

        return Arrays.asList(operators, label);
    }


    private static class ReplaceWildcards extends OptimizerRule<Filter> {

        private static boolean isWildcard(Expression expr) {
            if (expr instanceof Literal) {
                Literal l = (Literal) expr;
                if (l.value() instanceof String) {
                    String s = (String) l.value();
                    return s.contains("*");
                }
            }

            return false;
        }

        private static LikePattern toLikePattern(String s) {
            // pick a character that is guaranteed not to be in the string, because it isn't allowed to escape itself
            char escape = 1;

            // replace wildcards with % and escape special characters
            String likeString = s.replace("%", escape + "%")
                .replace("_", escape + "_")
                .replace("*", "%");

            return new LikePattern(likeString, escape);
        }

        @Override
        protected LogicalPlan rule(Filter filter) {
            return filter.transformExpressionsUp(e -> {
                // expr == "wildcard*phrase"
                if (e instanceof Equals) {
                    Equals eq = (Equals) e;

                    if (isWildcard(eq.right())) {
                        String wcString = (String) eq.right().fold();
                        e = new Like(e.source(), eq.left(), toLikePattern(wcString));
                    }
                }

                // expr != "wildcard*phrase"
                else if (e instanceof NotEquals) {
                    NotEquals eq = (NotEquals) e;

                    if (isWildcard(eq.right())) {
                        String wcString = (String) eq.right().fold();
                        Like inner = new Like(eq.source(), eq.left(), toLikePattern(wcString));
                        e = new Not(e.source(), inner);
                    }
                }
                return e;
            });
        }
    }
}
