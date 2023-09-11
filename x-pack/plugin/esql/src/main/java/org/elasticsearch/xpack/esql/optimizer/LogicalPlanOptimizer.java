/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.ExpressionSet;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BinaryComparisonSimplification;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanFunctionEqualsElimination;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.ConstantFolding;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.LiteralsOnTheRight;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PruneLiteralsInOrderBy;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SetAsOptimized;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SimplifyComparisonsArithmetics;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.Holder;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputExpressions;
import static org.elasticsearch.xpack.ql.expression.Expressions.asAttributes;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.FoldNull;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PropagateEquals;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PropagateNullable;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.TransformDirection;

public class LogicalPlanOptimizer extends RuleExecutor<LogicalPlan> {

    public LogicalPlan optimize(LogicalPlan verified) {
        return verified.optimized() ? verified : execute(verified);
    }

    @Override
    protected List<Batch<LogicalPlan>> batches() {
        return rules();
    }

    protected static List<Batch<LogicalPlan>> rules() {
        var substitutions = new Batch<>(
            "Substitutions",
            Limiter.ONCE,
            new SubstituteSurrogates(),
            new ReplaceRegexMatch(),
            new ReplaceFieldAttributesWithExactSubfield()
        );

        var operators = new Batch<>(
            "Operator Optimization",
            new CombineProjections(),
            new CombineEvals(),
            new PruneEmptyPlans(),
            new PropagateEmptyRelation(),
            new ConvertStringToByteRef(),
            new FoldNull(),
            new SplitInWithFoldableValue(),
            new ConstantFolding(),
            new PropagateEvalFoldables(),
            // boolean
            new BooleanSimplification(),
            new LiteralsOnTheRight(),
            new BinaryComparisonSimplification(),
            // needs to occur before BinaryComparison combinations (see class)
            new PropagateEquals(),
            new PropagateNullable(),
            new BooleanFunctionEqualsElimination(),
            new CombineDisjunctionsToIn(),
            new SimplifyComparisonsArithmetics(EsqlDataTypes::areCompatible),
            // prune/elimination
            new PruneFilters(),
            new PruneColumns(),
            new PruneLiteralsInOrderBy(),
            new PushDownAndCombineLimits(),
            new PushDownAndCombineFilters(),
            new PushDownEval(),
            new PushDownRegexExtract(),
            new PushDownEnrich(),
            new PushDownAndCombineOrderBy(),
            new PruneOrderByBeforeStats(),
            new PruneRedundantSortClauses()
        );

        var skip = new Batch<>("Skip Compute", new SkipQueryOnLimitZero());
        var cleanup = new Batch<>("Clean Up", new ReplaceLimitAndSortAsTopN());
        var label = new Batch<>("Set as Optimized", Limiter.ONCE, new SetAsOptimized());

        return asList(substitutions, operators, skip, cleanup, label);
    }

    // TODO: currently this rule only works for aggregate functions (AVG)
    static class SubstituteSurrogates extends OptimizerRules.OptimizerRule<Aggregate> {

        SubstituteSurrogates() {
            super(TransformDirection.UP);
        }

        @Override
        protected LogicalPlan rule(Aggregate aggregate) {
            var aggs = aggregate.aggregates();
            List<NamedExpression> newAggs = new ArrayList<>(aggs.size());
            // existing aggregate and their respective attributes
            Map<AggregateFunction, Attribute> aggFuncToAttr = new HashMap<>();
            // surrogate functions eval
            List<Alias> transientEval = new ArrayList<>();
            boolean changed = false;

            // first pass to check existing aggregates (to avoid duplication and alias waste)
            for (NamedExpression agg : aggs) {
                if (agg instanceof Alias a && a.child() instanceof AggregateFunction af && af instanceof SurrogateExpression == false) {
                    aggFuncToAttr.put(af, a.toAttribute());
                }
            }

            // 0. check list of surrogate expressions
            for (NamedExpression agg : aggs) {
                Expression e = agg instanceof Alias a ? a.child() : agg;
                if (e instanceof SurrogateExpression sf) {
                    changed = true;
                    Expression s = sf.surrogate();

                    // if the expression is NOT a 1:1 replacement need to add an eval
                    if (s instanceof AggregateFunction == false) {
                        // 1. collect all aggregate functions from the expression
                        var surrogateWithRefs = s.transformUp(AggregateFunction.class, af -> {
                            // 2. check if they are already use otherwise add them to the Aggregate with some made-up aliases
                            // 3. replace them inside the expression using the given alias
                            var attr = aggFuncToAttr.get(af);
                            // the agg doesn't exist in the Aggregate, create an alias for it and save its attribute
                            if (attr == null) {
                                var temporaryName = temporaryName(agg, af);
                                // create a synthetic alias (so it doesn't clash with a user defined name)
                                var newAlias = new Alias(agg.source(), temporaryName, null, af, null, true);
                                attr = newAlias.toAttribute();
                                aggFuncToAttr.put(af, attr);
                                newAggs.add(newAlias);
                            }
                            return attr;
                        });
                        // 4. move the expression as an eval using the original alias
                        // copy the original alias id so that other nodes using it down stream (e.g. eval referring to the original agg)
                        // don't have to updated
                        var aliased = new Alias(agg.source(), agg.name(), null, surrogateWithRefs, agg.toAttribute().id());
                        transientEval.add(aliased);
                    }
                    // the replacement is another aggregate function, so replace it in place
                    else {
                        newAggs.add((NamedExpression) agg.replaceChildren(Collections.singletonList(s)));
                    }
                } else {
                    newAggs.add(agg);
                }
            }

            LogicalPlan plan = aggregate;
            if (changed) {
                var source = aggregate.source();
                plan = new Aggregate(aggregate.source(), aggregate.child(), aggregate.groupings(), newAggs);
                // 5. force the initial projection in place
                if (transientEval.size() > 0) {
                    plan = new Eval(source, plan, transientEval);
                    // project away transient fields and re-enforce the original order using references (not copies) to the original aggs
                    // this works since the replaced aliases have their nameId copied to avoid having to update all references (which has
                    // a cascading effect)
                    plan = new EsqlProject(source, plan, Expressions.asAttributes(aggs));
                }
            }

            return plan;
        }

        private static String temporaryName(NamedExpression agg, AggregateFunction af) {
            return "__" + agg.name() + "_" + af.functionName() + "@" + Integer.toHexString(af.hashCode());
        }
    }

    static class ConvertStringToByteRef extends OptimizerRules.OptimizerExpressionRule<Literal> {

        ConvertStringToByteRef() {
            super(TransformDirection.UP);
        }

        @Override
        protected Expression rule(Literal lit) {
            if (lit.value() == null) {
                return lit;
            }
            if (lit.value() instanceof String s) {
                return Literal.of(lit, new BytesRef(s));
            }
            if (lit.value() instanceof List<?> l) {
                if (l.isEmpty() || false == l.get(0) instanceof String) {
                    return lit;
                }
                return Literal.of(lit, l.stream().map(v -> new BytesRef((String) v)).toList());
            }
            return lit;
        }
    }

    static class CombineProjections extends OptimizerRules.OptimizerRule<UnaryPlan> {

        CombineProjections() {
            super(TransformDirection.UP);
        }

        @Override
        protected LogicalPlan rule(UnaryPlan plan) {
            LogicalPlan child = plan.child();

            if (plan instanceof Project project) {
                if (child instanceof Project p) {
                    // eliminate lower project but first replace the aliases in the upper one
                    return p.withProjections(combineProjections(project.projections(), p.projections()));
                } else if (child instanceof Aggregate a) {
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

    /**
     * Combine multiple Evals into one in order to reduce the number of nodes in a plan.
     * TODO: eliminate unnecessary fields inside the eval as well
     */
    static class CombineEvals extends OptimizerRules.OptimizerRule<Eval> {

        CombineEvals() {
            super(TransformDirection.UP);
        }

        @Override
        protected LogicalPlan rule(Eval eval) {
            LogicalPlan plan = eval;
            if (eval.child() instanceof Eval subEval) {
                plan = new Eval(eval.source(), subEval.child(), CollectionUtils.combine(subEval.fields(), eval.fields()));
            }
            return plan;
        }
    }

    //
    // Replace any reference attribute with its source, if it does not affect the result.
    // This avoids ulterior look-ups between attributes and its source across nodes.
    //
    static class PropagateEvalFoldables extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            var collectRefs = new AttributeMap<Expression>();
            // collect aliases
            plan.forEachExpressionUp(Alias.class, a -> {
                var c = a.child();
                if (c.foldable()) {
                    collectRefs.put(a.toAttribute(), c);
                }
            });
            if (collectRefs.isEmpty()) {
                return plan;
            }
            java.util.function.Function<ReferenceAttribute, Expression> replaceReference = r -> collectRefs.resolve(r, r);

            plan = plan.transformUp(p -> {
                // Apply the replacement inside Filter and Eval (which shouldn't make a difference)
                if (p instanceof Filter || p instanceof Eval) {
                    p = p.transformExpressionsOnly(ReferenceAttribute.class, replaceReference);
                }
                return p;
            });

            return plan;
        }
    }

    static class PushDownAndCombineLimits extends OptimizerRules.OptimizerRule<Limit> {

        @Override
        protected LogicalPlan rule(Limit limit) {
            if (limit.child() instanceof Limit childLimit) {
                var limitSource = limit.limit();
                var l1 = (int) limitSource.fold();
                var l2 = (int) childLimit.limit().fold();
                return new Limit(limit.source(), Literal.of(limitSource, Math.min(l1, l2)), childLimit.child());
            } else if (limit.child() instanceof UnaryPlan unary) {
                if (unary instanceof Eval || unary instanceof Project || unary instanceof RegexExtract || unary instanceof Enrich) {
                    return unary.replaceChild(limit.replaceChild(unary.child()));
                }
                // check if there's a 'visible' descendant limit lower than the current one
                // and if so, align the current limit since it adds no value
                // this applies for cases such as | limit 1 | sort field | limit 10
                else {
                    Limit descendantLimit = descendantLimit(unary);
                    if (descendantLimit != null) {
                        var l1 = (int) limit.limit().fold();
                        var l2 = (int) descendantLimit.limit().fold();
                        if (l2 <= l1) {
                            return new Limit(limit.source(), Literal.of(limit.limit(), l2), limit.child());
                        }
                    }
                }
            }
            return limit;
        }

        /**
         * Checks the existence of another 'visible' Limit, that exists behind an operation that doesn't produce output more data than
         * its input (that is not a relation/source nor aggregation).
         * P.S. Typically an aggregation produces less data than the input.
         */
        private static Limit descendantLimit(UnaryPlan unary) {
            UnaryPlan plan = unary;
            while (plan instanceof Aggregate == false) {
                if (plan instanceof Limit limit) {
                    return limit;
                }
                if (plan.child() instanceof UnaryPlan unaryPlan) {
                    plan = unaryPlan;
                } else {
                    break;
                }
            }
            return null;
        }
    }

    // 3 in (field, 4, 5) --> 3 in (field) or 3 in (4, 5)
    public static class SplitInWithFoldableValue extends OptimizerRules.OptimizerExpressionRule<In> {

        SplitInWithFoldableValue() {
            super(TransformDirection.UP);
        }

        @Override
        protected Expression rule(In in) {
            if (in.value().foldable()) {
                List<Expression> foldables = new ArrayList<>(in.list().size());
                List<Expression> nonFoldables = new ArrayList<>(in.list().size());
                in.list().forEach(e -> {
                    if (e.foldable() && Expressions.isNull(e) == false) { // keep `null`s, needed for the 3VL
                        foldables.add(e);
                    } else {
                        nonFoldables.add(e);
                    }
                });
                if (foldables.size() > 0 && nonFoldables.size() > 0) {
                    In withFoldables = new In(in.source(), in.value(), foldables);
                    In withoutFoldables = new In(in.source(), in.value(), nonFoldables);
                    return new Or(in.source(), withFoldables, withoutFoldables);
                }
            }
            return in;
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

    static class PruneEmptyPlans extends OptimizerRules.OptimizerRule<UnaryPlan> {

        @Override
        protected LogicalPlan rule(UnaryPlan plan) {
            return plan.output().isEmpty() ? skipPlan(plan) : plan;
        }
    }

    static class PropagateEmptyRelation extends OptimizerRules.OptimizerRule<UnaryPlan> {

        @Override
        protected LogicalPlan rule(UnaryPlan plan) {
            LogicalPlan p = plan;
            if (plan.child() instanceof LocalRelation local && local.supplier() == LocalSupplier.EMPTY) {
                // only care about non-grouped aggs might return something (count)
                if (plan instanceof Aggregate agg && agg.groupings().isEmpty()) {
                    p = skipPlan(plan, aggsFromEmpty(agg.aggregates()));
                } else {
                    p = skipPlan(plan);
                }
            }
            return p;
        }

        private static LocalSupplier aggsFromEmpty(List<? extends NamedExpression> aggs) {
            var result = new ArrayList<Object>(aggs.size());
            for (var agg : aggs) {
                // there needs to be an alias
                if (agg instanceof Alias a && a.child() instanceof AggregateFunction aggFunc) {
                    result.add(aggFunc instanceof Count ? 0L : null);
                } else {
                    throw new EsqlIllegalArgumentException("Did not expect a non-aliased aggregation {}", agg);
                }
            }
            var blocks = BlockUtils.fromListRow(result);
            return LocalSupplier.of(blocks);
        }
    }

    private static LogicalPlan skipPlan(UnaryPlan plan) {
        return new LocalRelation(plan.source(), plan.output(), LocalSupplier.EMPTY);
    }

    private static LogicalPlan skipPlan(UnaryPlan plan, LocalSupplier supplier) {
        return new LocalRelation(plan.source(), plan.output(), supplier);
    }

    protected static class PushDownAndCombineFilters extends OptimizerRules.OptimizerRule<Filter> {
        @Override
        protected LogicalPlan rule(Filter filter) {
            LogicalPlan plan = filter;
            LogicalPlan child = filter.child();
            Expression condition = filter.condition();

            if (child instanceof Filter f) {
                // combine nodes into a single Filter with updated ANDed condition
                plan = f.with(Predicates.combineAnd(List.of(f.condition(), condition)));
            } else if (child instanceof Aggregate agg) { // TODO: re-evaluate along with multi-value support
                // Only push [parts of] a filter past an agg if these/it operates on agg's grouping[s], not output.
                plan = maybePushDownPastUnary(
                    filter,
                    agg,
                    e -> e instanceof Attribute && agg.output().contains(e) && agg.groupings().contains(e) == false
                        || e instanceof AggregateFunction
                );
            } else if (child instanceof Eval eval) {
                // Don't push if Filter (still) contains references of Eval's fields.
                var attributes = new AttributeSet(Expressions.asAttributes(eval.fields()));
                plan = maybePushDownPastUnary(filter, eval, attributes::contains);
            } else if (child instanceof RegexExtract re) {
                // Push down filters that do not rely on attributes created by RegexExtract
                var attributes = new AttributeSet(Expressions.asAttributes(re.extractedFields()));
                plan = maybePushDownPastUnary(filter, re, attributes::contains);
            } else if (child instanceof Enrich enrich) {
                // Push down filters that do not rely on attributes created by Enrich
                var attributes = new AttributeSet(Expressions.asAttributes(enrich.enrichFields()));
                plan = maybePushDownPastUnary(filter, enrich, attributes::contains);
            } else if (child instanceof Project) {
                return pushDownPastProject(filter);
            } else if (child instanceof OrderBy orderBy) {
                // swap the filter with its child
                plan = orderBy.replaceChild(filter.with(orderBy.child(), condition));
            }
            // cannot push past a Limit, this could change the tailing result set returned
            return plan;
        }

        private static LogicalPlan maybePushDownPastUnary(Filter filter, UnaryPlan unary, Predicate<Expression> cannotPush) {
            LogicalPlan plan;
            List<Expression> pushable = new ArrayList<>();
            List<Expression> nonPushable = new ArrayList<>();
            for (Expression exp : Predicates.splitAnd(filter.condition())) {
                (exp.anyMatch(cannotPush) ? nonPushable : pushable).add(exp);
            }
            // Push the filter down even if it might not be pushable all the way to ES eventually: eval'ing it closer to the source,
            // potentially still in the Exec Engine, distributes the computation.
            if (pushable.size() > 0) {
                if (nonPushable.size() > 0) {
                    Filter pushed = new Filter(filter.source(), unary.child(), Predicates.combineAnd(pushable));
                    plan = filter.with(unary.replaceChild(pushed), Predicates.combineAnd(nonPushable));
                } else {
                    plan = unary.replaceChild(filter.with(unary.child(), filter.condition()));
                }
            } else {
                plan = filter;
            }
            return plan;
        }
    }

    /**
     * Pushes Evals past OrderBys. Although it seems arbitrary whether the OrderBy or the Eval is executed first,
     * this transformation ensures that OrderBys only separated by an eval can be combined by PushDownAndCombineOrderBy.
     *
     * E.g.:
     *
     * ... | sort a | eval x = b + 1 | sort x
     *
     * becomes
     *
     * ... | eval x = b + 1 | sort a | sort x
     *
     * Ordering the evals before the orderBys has the advantage that it's always possible to order the plans like this.
     * E.g., in the example above it would not be possible to put the eval after the two orderBys.
     */
    protected static class PushDownEval extends OptimizerRules.OptimizerRule<Eval> {
        @Override
        protected LogicalPlan rule(Eval eval) {
            LogicalPlan child = eval.child();

            if (child instanceof OrderBy orderBy) {
                return orderBy.replaceChild(eval.replaceChild(orderBy.child()));
            } else if (child instanceof Project) {
                var projectWithEvalChild = pushDownPastProject(eval);
                var fieldProjections = asAttributes(eval.fields());
                return projectWithEvalChild.withProjections(mergeOutputExpressions(fieldProjections, projectWithEvalChild.projections()));
            }

            return eval;
        }
    }

    // same as for PushDownEval
    protected static class PushDownRegexExtract extends OptimizerRules.OptimizerRule<RegexExtract> {
        @Override
        protected LogicalPlan rule(RegexExtract re) {
            LogicalPlan child = re.child();

            if (child instanceof OrderBy orderBy) {
                return orderBy.replaceChild(re.replaceChild(orderBy.child()));
            } else if (child instanceof Project) {
                var projectWithChild = pushDownPastProject(re);
                return projectWithChild.withProjections(mergeOutputExpressions(re.extractedFields(), projectWithChild.projections()));
            }

            return re;
        }
    }

    // TODO double-check: this should be the same as EVAL and GROK/DISSECT, needed to avoid unbounded sort
    protected static class PushDownEnrich extends OptimizerRules.OptimizerRule<Enrich> {
        @Override
        protected LogicalPlan rule(Enrich re) {
            LogicalPlan child = re.child();

            if (child instanceof OrderBy orderBy) {
                return orderBy.replaceChild(re.replaceChild(orderBy.child()));
            } else if (child instanceof Project) {
                var projectWithChild = pushDownPastProject(re);
                var attrs = asAttributes(re.enrichFields());
                return projectWithChild.withProjections(mergeOutputExpressions(attrs, projectWithChild.projections()));
            }

            return re;
        }
    }

    protected static class PushDownAndCombineOrderBy extends OptimizerRules.OptimizerRule<OrderBy> {

        @Override
        protected LogicalPlan rule(OrderBy orderBy) {
            LogicalPlan child = orderBy.child();

            if (child instanceof OrderBy childOrder) {
                // combine orders
                return new OrderBy(orderBy.source(), childOrder.child(), CollectionUtils.combine(orderBy.order(), childOrder.order()));
            } else if (child instanceof Project) {
                return pushDownPastProject(orderBy);
            }

            return orderBy;
        }
    }

    /**
     * Remove unused columns created in the plan, in fields inside eval or aggregations inside stats.
     */
    static class PruneColumns extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            var used = new AttributeSet();
            // don't remove Evals without any Project/Aggregate (which might not occur as the last node in the plan)
            var seenProjection = new Holder<>(Boolean.FALSE);

            // start top-to-bottom
            // and track used references
            var pl = plan.transformDown(p -> {
                // skip nodes that simply pass the input through
                if (p instanceof Limit) {
                    return p;
                }

                // remember used
                boolean recheck;
                // analyze the unused items against dedicated 'producer' nodes such as Eval and Aggregate
                // perform a loop to retry checking if the current node is completely eliminated
                do {
                    recheck = false;
                    if (p instanceof Aggregate aggregate) {
                        var remaining = seenProjection.get() ? removeUnused(aggregate.aggregates(), used) : null;
                        // no aggregates, no need
                        if (remaining != null) {
                            if (remaining.isEmpty()) {
                                recheck = true;
                                p = aggregate.child();
                            } else {
                                p = new Aggregate(aggregate.source(), aggregate.child(), aggregate.groupings(), remaining);
                            }
                        }

                        seenProjection.set(Boolean.TRUE);
                    } else if (p instanceof Eval eval) {
                        var remaining = seenProjection.get() ? removeUnused(eval.fields(), used) : null;
                        // no fields, no eval
                        if (remaining != null) {
                            if (remaining.isEmpty()) {
                                p = eval.child();
                                recheck = true;
                            } else {
                                p = new Eval(eval.source(), eval.child(), remaining);
                            }
                        }
                    } else if (p instanceof Project) {
                        seenProjection.set(Boolean.TRUE);
                    }
                } while (recheck);

                used.addAll(p.references());

                // preserve the state before going to the next node
                return p;
            });

            return pl;
        }

        /**
         * Prunes attributes from the list not found in the given set.
         * Returns null if no changed occurred.
         */
        private static <N extends NamedExpression> List<N> removeUnused(List<N> named, AttributeSet used) {
            var clone = new ArrayList<>(named);
            var it = clone.listIterator(clone.size());

            // due to Eval, go in reverse
            while (it.hasPrevious()) {
                N prev = it.previous();
                if (used.contains(prev.toAttribute()) == false) {
                    it.remove();
                } else {
                    used.addAll(prev.references());
                }
            }
            return clone.size() != named.size() ? clone : null;
        }
    }

    static class PruneOrderByBeforeStats extends OptimizerRules.OptimizerRule<Aggregate> {

        @Override
        protected LogicalPlan rule(Aggregate agg) {
            OrderBy order = findPullableOrderBy(agg.child());

            LogicalPlan p = agg;
            if (order != null) {
                p = agg.transformDown(OrderBy.class, o -> o == order ? order.child() : o);
            }
            return p;
        }

        private static OrderBy findPullableOrderBy(LogicalPlan plan) {
            OrderBy pullable = null;
            if (plan instanceof OrderBy o) {
                pullable = o;
            } else if (plan instanceof Eval
                || plan instanceof Filter
                || plan instanceof Project
                || plan instanceof RegexExtract
                || plan instanceof Enrich) {
                    pullable = findPullableOrderBy(((UnaryPlan) plan).child());
                }
            return pullable;
        }

    }

    static class PruneRedundantSortClauses extends OptimizerRules.OptimizerRule<OrderBy> {

        @Override
        protected LogicalPlan rule(OrderBy plan) {
            var referencedAttributes = new ExpressionSet<Order>();
            var order = new ArrayList<Order>();
            for (Order o : plan.order()) {
                if (referencedAttributes.add(o)) {
                    order.add(o);
                }
            }

            return plan.order().size() == order.size() ? plan : new OrderBy(plan.source(), plan.child(), order);
        }
    }

    private static Project pushDownPastProject(UnaryPlan parent) {
        if (parent.child() instanceof Project project) {
            AttributeMap.Builder<Expression> aliasBuilder = AttributeMap.builder();
            project.forEachExpression(Alias.class, a -> aliasBuilder.put(a.toAttribute(), a.child()));
            var aliases = aliasBuilder.build();

            var expressionsWithResolvedAliases = (UnaryPlan) parent.transformExpressionsOnly(
                ReferenceAttribute.class,
                r -> aliases.resolve(r, r)
            );

            return project.replaceChild(expressionsWithResolvedAliases.replaceChild(project.child()));
        } else {
            throw new EsqlIllegalArgumentException("Expected child to be instance of Project");
        }
    }

    /**
     * Combine disjunctions on the same field into an In expression.
     * This rule looks for both simple equalities:
     * 1. a == 1 OR a == 2 becomes a IN (1, 2)
     * and combinations of In
     * 2. a == 1 OR a IN (2) becomes a IN (1, 2)
     * 3. a IN (1) OR a IN (2) becomes a IN (1, 2)
     *
     * This rule does NOT check for type compatibility as that phase has been
     * already be verified in the analyzer.
     */
    public static class CombineDisjunctionsToIn extends OptimizerRules.CombineDisjunctionsToIn {

        protected In createIn(Expression key, List<Expression> values, ZoneId zoneId) {
            return new In(key.source(), key, values);
        }

        protected Equals createEquals(Expression k, Set<Expression> v, ZoneId finalZoneId) {
            return new Equals(k.source(), k, v.iterator().next(), finalZoneId);
        }
    }

    static class ReplaceLimitAndSortAsTopN extends OptimizerRules.OptimizerRule<Limit> {

        @Override
        protected LogicalPlan rule(Limit plan) {
            LogicalPlan p = plan;
            if (plan.child() instanceof OrderBy o) {
                p = new TopN(plan.source(), o.child(), o.order(), plan.limit());
            }
            return p;
        }
    }

    public static class ReplaceRegexMatch extends OptimizerRules.ReplaceRegexMatch {

        protected Expression regexToEquals(RegexMatch<?> regexMatch, Literal literal) {
            return new Equals(regexMatch.source(), regexMatch.field(), literal);
        }
    }

    private static class ReplaceFieldAttributesWithExactSubfield extends OptimizerRules.OptimizerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (plan instanceof Filter || plan instanceof OrderBy || plan instanceof Aggregate) {
                return plan.transformExpressionsOnly(FieldAttribute.class, ReplaceFieldAttributesWithExactSubfield::toExact);
            }
            return plan;
        }

        private static FieldAttribute toExact(FieldAttribute fa) {
            if (fa.getExactInfo().hasExact() && fa.exactAttribute() != fa) {
                return fa.exactAttribute();
            }
            return fa;
        }
    }
}
