/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.LocalExecutable;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BinaryComparisonSimplification;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanFunctionEqualsElimination;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.CombineDisjunctionsToIn;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.ConstantFolding;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.LiteralsOnTheRight;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PruneLiteralsInOrderBy;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PushDownAndCombineFilters;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SetAsOptimized;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SimplifyComparisonsArithmetics;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class LogicalPlanOptimizer extends RuleExecutor<LogicalPlan> {

    public LogicalPlan optimize(LogicalPlan verified) {
        return verified.optimized() ? verified : execute(verified);
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch operators = new Batch(
            "Operator Optimization",
            new CombineProjections(),
            new FoldNull(),
            new ConstantFolding(),
            // boolean
            new BooleanSimplification(),
            new LiteralsOnTheRight(),
            new BinaryComparisonSimplification(),
            new BooleanFunctionEqualsElimination(),
            new CombineDisjunctionsToIn(),
            new SimplifyComparisonsArithmetics(DataTypes::areCompatible),
            // prune/elimination
            new PruneFilters(),
            new PruneLiteralsInOrderBy(),
            new CombineLimits(),
            new PushDownAndCombineFilters()
        );

        Batch local = new Batch("Skip Compute", new SkipQueryOnLimitZero());
        Batch label = new Batch("Set as Optimized", Limiter.ONCE, new SetAsOptimized());

        return asList(operators, local, label);
    }

    static class CombineProjections extends OptimizerRules.OptimizerRule<UnaryPlan> {

        CombineProjections() {
            super(OptimizerRules.TransformDirection.UP);
        }

        @Override
        protected LogicalPlan rule(UnaryPlan plan) {
            LogicalPlan child = plan.child();

            if (plan instanceof Project project) {
                if (child instanceof Project p) {
                    // eliminate lower project but first replace the aliases in the upper one
                    return new Project(p.source(), p.child(), combineProjections(project.projections(), p.projections()));
                }

                if (child instanceof Aggregate a) {
                    return new Aggregate(a.source(), a.child(), a.groupings(), combineProjections(project.projections(), a.aggregates()));
                }
            }

            // Agg with underlying Project (group by on sub-queries)
            if (plan instanceof Aggregate a) {
                if (child instanceof Project p) {
                    return new Aggregate(a.source(), p.child(), a.groupings(), combineProjections(a.aggregates(), p.projections()));
                }
            }
            return plan;
        }

        // normally only the upper projections should survive but since the lower list might have aliases definitions
        // that might be reused by the upper one, these need to be replaced.
        // for example an alias defined in the lower list might be referred in the upper - without replacing it the alias becomes invalid
        private List<NamedExpression> combineProjections(List<? extends NamedExpression> upper, List<? extends NamedExpression> lower) {

            // collect aliases in the lower list
            AttributeMap.Builder<NamedExpression> aliasesBuilder = AttributeMap.builder();
            for (NamedExpression ne : lower) {
                if ((ne instanceof Attribute) == false) {
                    aliasesBuilder.put(ne.toAttribute(), ne);
                }
            }

            AttributeMap<NamedExpression> aliases = aliasesBuilder.build();
            List<NamedExpression> replaced = new ArrayList<>();

            // replace any matching attribute with a lower alias (if there's a match)
            // but clean-up non-top aliases at the end
            for (NamedExpression ne : upper) {
                NamedExpression replacedExp = (NamedExpression) ne.transformUp(Attribute.class, a -> aliases.resolve(a, a));
                replaced.add((NamedExpression) trimNonTopLevelAliases(replacedExp));
            }
            return replaced;
        }

        public static Expression trimNonTopLevelAliases(Expression e) {
            if (e instanceof Alias a) {
                return new Alias(a.source(), a.name(), a.qualifier(), trimAliases(a.child()), a.id());
            }
            return trimAliases(e);
        }

        private static Expression trimAliases(Expression e) {
            return e.transformDown(Alias.class, Alias::child);
        }
    }

    static class FoldNull extends OptimizerRules.OptimizerExpressionRule<Expression> {

        FoldNull() {
            super(OptimizerRules.TransformDirection.UP);
        }

        @Override
        protected Expression rule(Expression e) {
            if (e instanceof Alias == false
                && e.nullable() == Nullability.TRUE
                && Expressions.anyMatch(e.children(), Expressions::isNull)) {
                return Literal.of(e, null);
            }
            return e;
        }
    }

    static class CombineLimits extends OptimizerRules.OptimizerRule<Limit> {

        @Override
        protected LogicalPlan rule(Limit limit) {
            if (limit.child()instanceof Limit childLimit) {
                var limitSource = limit.limit();
                var l1 = (int) limitSource.fold();
                var l2 = (int) childLimit.limit().fold();
                return new Limit(limit.source(), Literal.of(limitSource, Math.min(l1, l2)), childLimit.child());
            }
            return limit;
        }
    }

    private static class BooleanSimplification extends org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanSimplification {

        BooleanSimplification() {
            super();
        }

        @Override
        protected Expression maybeSimplifyNegatable(Expression e) {
            return null;
        }

    }

    static class PruneFilters extends OptimizerRules.PruneFilters {

        @Override
        protected LogicalPlan skipPlan(Filter filter) {
            return LogicalPlanOptimizer.skipPlan(filter);
        }
    }

    static class SkipQueryOnLimitZero extends OptimizerRules.SkipQueryOnLimitZero {

        @Override
        protected LogicalPlan skipPlan(Limit limit) {
            return LogicalPlanOptimizer.skipPlan(limit);
        }
    }

    private static LogicalPlan skipPlan(UnaryPlan plan) {
        return new LocalRelation(plan.source(), new LocalExecutable() {
            @Override
            public List<Attribute> output() {
                return plan.output();
            }

            @Override
            public void execute(EsqlSession session, ActionListener<Result> listener) {

            }
        });
    }
}
