/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.common.Failures;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.ExpressionSet;
import org.elasticsearch.xpack.ql.expression.Expressions;
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
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.rule.ParameterizedRule;
import org.elasticsearch.xpack.ql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputExpressions;
import static org.elasticsearch.xpack.ql.expression.Expressions.asAttributes;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.FoldNull;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PropagateEquals;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PropagateNullable;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.TransformDirection;

public class LogicalPlanOptimizer extends ParameterizedRuleExecutor<LogicalPlan, LogicalOptimizerContext> {

    private final LogicalVerifier verifier = LogicalVerifier.INSTANCE;

    public LogicalPlanOptimizer(LogicalOptimizerContext optimizerContext) {
        super(optimizerContext);
    }

    public LogicalPlan optimize(LogicalPlan verified) {
        var optimized = execute(verified);

        Failures failures = verifier.verify(optimized);
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
        return optimized;
    }

    @Override
    protected List<Batch<LogicalPlan>> batches() {
        return rules();
    }

    protected static Batch<LogicalPlan> operators() {
        return new Batch<>(
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
            new DuplicateLimitAfterMvExpand(),
            new PushDownAndCombineFilters(),
            new PushDownEval(),
            new PushDownRegexExtract(),
            new PushDownEnrich(),
            new PushDownAndCombineOrderBy(),
            new PruneOrderByBeforeStats(),
            new PruneRedundantSortClauses()
        );
    }

    protected static Batch<LogicalPlan> cleanup() {
        return new Batch<>("Clean Up", new ReplaceLimitAndSortAsTopN());
    }

    protected static List<Batch<LogicalPlan>> rules() {
        var substitutions = new Batch<>(
            "Substitutions",
            Limiter.ONCE,
            // first extract nested aggs top-level - this simplifies the rest of the rules
            new ReplaceStatsAggExpressionWithEval(),
            // second extract nested aggs inside of them
            new ReplaceStatsNestedExpressionWithEval(),
            // lastly replace surrogate functions
            new SubstituteSurrogates(),
            new ReplaceRegexMatch(),
            new ReplaceAliasingEvalWithProject(),
            new SkipQueryOnEmptyMappings()
            // new NormalizeAggregate(), - waits on https://github.com/elastic/elasticsearch/issues/100634
        );

        var skip = new Batch<>("Skip Compute", new SkipQueryOnLimitZero());
        var defaultTopN = new Batch<>("Add default TopN", new AddDefaultTopN());
        var label = new Batch<>("Set as Optimized", Limiter.ONCE, new SetAsOptimized());

        return asList(substitutions, operators(), skip, cleanup(), defaultTopN, label);
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
                if (Alias.unwrap(agg) instanceof AggregateFunction af && af instanceof SurrogateExpression == false) {
                    aggFuncToAttr.put(af, agg.toAttribute());
                }
            }

            int[] counter = new int[] { 0 };
            // 0. check list of surrogate expressions
            for (NamedExpression agg : aggs) {
                Expression e = Alias.unwrap(agg);
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
                                var temporaryName = temporaryName(af, agg, counter[0]++);
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
                    plan = new Project(source, plan, Expressions.asAttributes(aggs));
                }
            }

            return plan;
        }

        static String temporaryName(Expression inner, Expression outer, int suffix) {
            String in = toString(inner);
            String out = toString(outer);
            return rawTemporaryName(in, out, String.valueOf(suffix));
        }

        static String rawTemporaryName(String inner, String outer, String suffix) {
            return "$$" + inner + "$" + outer + "$" + suffix;
        }

        static int TO_STRING_LIMIT = 16;

        static String toString(Expression ex) {
            return ex instanceof AggregateFunction af ? af.functionName() : extractString(ex);
        }

        static String extractString(Expression ex) {
            return ex instanceof NamedExpression ne ? ne.name() : limitToString(ex.sourceText()).replace(' ', '_');
        }

        static String limitToString(String string) {
            return string.length() > 16 ? string.substring(0, TO_STRING_LIMIT - 1) + ">" : string;
        }
    }

    static class ConvertStringToByteRef extends OptimizerRules.OptimizerExpressionRule<Literal> {

        ConvertStringToByteRef() {
            super(TransformDirection.UP);
        }

        @Override
        protected Expression rule(Literal lit) {
            Object value = lit.value();

            if (value == null) {
                return lit;
            }
            if (value instanceof String s) {
                return Literal.of(lit, new BytesRef(s));
            }
            if (value instanceof List<?> l) {
                if (l.isEmpty() || false == l.get(0) instanceof String) {
                    return lit;
                }
                List<BytesRef> byteRefs = new ArrayList<>(l.size());
                for (Object v : l) {
                    byteRefs.add(new BytesRef(v.toString()));
                }
                return Literal.of(lit, byteRefs);
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
                    project = p.withProjections(combineProjections(project.projections(), p.projections()));
                    child = project.child();
                    plan = project;
                    // don't return the plan since the grandchild (now child) might be an aggregate that could not be folded on the way up
                    // e.g. stats c = count(x) | project c, c as x | project x
                    // try to apply the rule again opportunistically as another node might be pushed in (a limit might be pushed in)
                }
                // check if the projection eliminates certain aggregates
                // but be mindful of aliases to existing aggregates that we don't want to duplicate to avoid redundant work
                if (child instanceof Aggregate a) {
                    var aggs = a.aggregates();
                    var newAggs = projectAggregations(project.projections(), aggs);
                    // project can be fully removed
                    if (newAggs != null) {
                        var newGroups = replacePrunedAliasesUsedInGroupBy(a.groupings(), aggs, newAggs);
                        plan = new Aggregate(a.source(), a.child(), newGroups, newAggs);
                    }
                }
                return plan;
            }

            // Agg with underlying Project (group by on sub-queries)
            if (plan instanceof Aggregate a) {
                if (child instanceof Project p) {
                    plan = new Aggregate(a.source(), p.child(), a.groupings(), combineProjections(a.aggregates(), p.projections()));
                }
            }

            return plan;
        }

        // variant of #combineProjections specialized for project followed by agg due to the rewrite rules applied on aggregations
        // this method tries to combine the projections by paying attention to:
        // - aggregations that are projected away - remove them
        // - aliases in the project that point to aggregates - keep them in place (to avoid duplicating the aggs)
        private static List<? extends NamedExpression> projectAggregations(
            List<? extends NamedExpression> upperProjection,
            List<? extends NamedExpression> lowerAggregations
        ) {
            AttributeMap<Expression> lowerAliases = new AttributeMap<>();
            for (NamedExpression ne : lowerAggregations) {
                lowerAliases.put(ne.toAttribute(), Alias.unwrap(ne));
            }

            AttributeSet seen = new AttributeSet();
            for (NamedExpression upper : upperProjection) {
                Expression unwrapped = Alias.unwrap(upper);
                // projection contains an inner alias (point to an existing fields inside the projection)
                if (seen.contains(unwrapped)) {
                    return null;
                }
                seen.add(Expressions.attribute(unwrapped));
            }

            lowerAggregations = combineProjections(upperProjection, lowerAggregations);

            return lowerAggregations;
        }

        // normally only the upper projections should survive but since the lower list might have aliases definitions
        // that might be reused by the upper one, these need to be replaced.
        // for example an alias defined in the lower list might be referred in the upper - without replacing it the alias becomes invalid
        private static List<NamedExpression> combineProjections(
            List<? extends NamedExpression> upper,
            List<? extends NamedExpression> lower
        ) {

            // collect aliases in the lower list
            AttributeMap<NamedExpression> aliases = new AttributeMap<>();
            for (NamedExpression ne : lower) {
                if ((ne instanceof Attribute) == false) {
                    aliases.put(ne.toAttribute(), ne);
                }
            }
            List<NamedExpression> replaced = new ArrayList<>();

            // replace any matching attribute with a lower alias (if there's a match)
            // but clean-up non-top aliases at the end
            for (NamedExpression ne : upper) {
                NamedExpression replacedExp = (NamedExpression) ne.transformUp(Attribute.class, a -> aliases.resolve(a, a));
                replaced.add((NamedExpression) trimNonTopLevelAliases(replacedExp));
            }
            return replaced;
        }

        /**
         * Replace grouping alias previously contained in the aggregations that might have been projected away.
         */
        private List<Expression> replacePrunedAliasesUsedInGroupBy(
            List<Expression> groupings,
            List<? extends NamedExpression> oldAggs,
            List<? extends NamedExpression> newAggs
        ) {
            AttributeMap<Expression> removedAliases = new AttributeMap<>();
            AttributeSet currentAliases = new AttributeSet(Expressions.asAttributes(newAggs));

            // record only removed aliases
            for (NamedExpression ne : oldAggs) {
                if (ne instanceof Alias alias) {
                    var attr = ne.toAttribute();
                    if (currentAliases.contains(attr) == false) {
                        removedAliases.put(attr, alias.child());
                    }
                }
            }

            if (removedAliases.isEmpty()) {
                return groupings;
            }

            var newGroupings = new ArrayList<Expression>(groupings.size());
            for (Expression group : groupings) {
                newGroupings.add(group.transformUp(Attribute.class, a -> removedAliases.resolve(a, a)));
            }

            return newGroupings;
        }

        public static Expression trimNonTopLevelAliases(Expression e) {
            return e instanceof Alias a ? a.replaceChild(trimAliases(a.child())) : trimAliases(e);
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

            java.util.function.Function<ReferenceAttribute, Expression> replaceReference = r -> collectRefs.resolve(r, r);

            // collect aliases bottom-up
            plan.forEachExpressionUp(Alias.class, a -> {
                var c = a.child();
                boolean shouldCollect = c.foldable();
                // try to resolve the expression based on an existing foldables
                if (shouldCollect == false) {
                    c = c.transformUp(ReferenceAttribute.class, replaceReference);
                    shouldCollect = c.foldable();
                }
                if (shouldCollect) {
                    collectRefs.put(a.toAttribute(), Literal.of(c));
                }
            });
            if (collectRefs.isEmpty()) {
                return plan;
            }

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
                } else if (plan instanceof MvExpand) {
                    // the limit that applies to mv_expand shouldn't be changed
                    // ie "| limit 1 | mv_expand x | limit 20" where we want that last "limit" to apply on expand results
                    return null;
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

    static class DuplicateLimitAfterMvExpand extends OptimizerRules.OptimizerRule<Limit> {

        @Override
        protected LogicalPlan rule(Limit limit) {
            var child = limit.child();
            var shouldSkip = child instanceof Eval
                || child instanceof Project
                || child instanceof RegexExtract
                || child instanceof Enrich
                || child instanceof Limit;

            if (shouldSkip == false && child instanceof UnaryPlan unary) {
                MvExpand mvExpand = descendantMvExpand(unary);
                if (mvExpand != null) {
                    Limit limitBeforeMvExpand = limitBeforeMvExpand(mvExpand);
                    // if there is no "appropriate" limit before mv_expand, then push down a copy of the one after it so that:
                    // - a possible TopN is properly built as low as possible in the tree (closed to Lucene)
                    // - the input of mv_expand is as small as possible before it is expanded (less rows to inflate and occupy memory)
                    if (limitBeforeMvExpand == null) {
                        var duplicateLimit = new Limit(limit.source(), limit.limit(), mvExpand.child());
                        return limit.replaceChild(propagateDuplicateLimitUntilMvExpand(duplicateLimit, mvExpand, unary));
                    }
                }
            }
            return limit;
        }

        private static MvExpand descendantMvExpand(UnaryPlan unary) {
            UnaryPlan plan = unary;
            AttributeSet filterReferences = new AttributeSet();
            while (plan instanceof Aggregate == false) {
                if (plan instanceof MvExpand mve) {
                    // don't return the mv_expand that has a filter after it which uses the expanded values
                    // since this will trigger the use of a potentially incorrect (too restrictive) limit further down in the tree
                    if (filterReferences.isEmpty() == false) {
                        if (filterReferences.contains(mve.target()) // the same field or reference attribute is used in mv_expand AND filter
                            || mve.target() instanceof ReferenceAttribute // or the mv_expand attr hasn't yet been resolved to a field attr
                            // or not all filter references have been resolved to field attributes
                            || filterReferences.stream().anyMatch(ref -> ref instanceof ReferenceAttribute)) {
                            return null;
                        }
                    }
                    return mve;
                } else if (plan instanceof Filter filter) {
                    // gather all the filters' references to be checked later when a mv_expand is found
                    filterReferences.addAll(filter.references());
                } else if (plan instanceof OrderBy) {
                    // ordering after mv_expand COULD break the order of the results, so the limit shouldn't be copied past mv_expand
                    // something like from test | sort emp_no | mv_expand job_positions | sort first_name | limit 5
                    // (the sort first_name likely changes the order of the docs after sort emp_no, so "limit 5" shouldn't be copied down
                    return null;
                }

                if (plan.child() instanceof UnaryPlan unaryPlan) {
                    plan = unaryPlan;
                } else {
                    break;
                }
            }
            return null;
        }

        private static Limit limitBeforeMvExpand(MvExpand mvExpand) {
            UnaryPlan plan = mvExpand;
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

        private LogicalPlan propagateDuplicateLimitUntilMvExpand(Limit duplicateLimit, MvExpand mvExpand, UnaryPlan child) {
            if (child == mvExpand) {
                return mvExpand.replaceChild(duplicateLimit);
            } else {
                return child.replaceChild(propagateDuplicateLimitUntilMvExpand(duplicateLimit, mvExpand, (UnaryPlan) child.child()));
            }
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

    static class SkipQueryOnEmptyMappings extends OptimizerRules.OptimizerRule<EsRelation> {

        @Override
        protected LogicalPlan rule(EsRelation plan) {
            return plan.index().concreteIndices().isEmpty() ? new LocalRelation(plan.source(), plan.output(), LocalSupplier.EMPTY) : plan;
        }
    }

    @SuppressWarnings("removal")
    static class PropagateEmptyRelation extends OptimizerRules.OptimizerRule<UnaryPlan> {

        @Override
        protected LogicalPlan rule(UnaryPlan plan) {
            LogicalPlan p = plan;
            if (plan.child() instanceof LocalRelation local && local.supplier() == LocalSupplier.EMPTY) {
                // only care about non-grouped aggs might return something (count)
                if (plan instanceof Aggregate agg && agg.groupings().isEmpty()) {
                    List<Block> emptyBlocks = aggsFromEmpty(agg.aggregates());
                    p = skipPlan(plan, LocalSupplier.of(emptyBlocks.toArray(Block[]::new)));
                } else {
                    p = skipPlan(plan);
                }
            }
            return p;
        }

        private List<Block> aggsFromEmpty(List<? extends NamedExpression> aggs) {
            List<Block> blocks = new ArrayList<>();
            var blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
            int i = 0;
            for (var agg : aggs) {
                // there needs to be an alias
                if (Alias.unwrap(agg) instanceof AggregateFunction aggFunc) {
                    aggOutput(agg, aggFunc, blockFactory, blocks);
                } else {
                    throw new EsqlIllegalArgumentException("Did not expect a non-aliased aggregation {}", agg);
                }
            }
            return blocks;
        }

        /**
         * The folded aggregation output - this variant is for the coordinator/final.
         */
        protected void aggOutput(NamedExpression agg, AggregateFunction aggFunc, BlockFactory blockFactory, List<Block> blocks) {
            // look for count(literal) with literal != null
            Object value = aggFunc instanceof Count count && (count.foldable() == false || count.fold() != null) ? 0L : null;
            var wrapper = BlockUtils.wrapperFor(blockFactory, PlannerUtils.toElementType(aggFunc.dataType()), 1);
            wrapper.accept(value);
            blocks.add(wrapper.builder().build());
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

    protected static class PushDownEval extends OptimizerRules.OptimizerRule<Eval> {
        @Override
        protected LogicalPlan rule(Eval eval) {
            return pushGeneratingPlanPastProjectAndOrderBy(eval, asAttributes(eval.fields()));
        }
    }

    protected static class PushDownRegexExtract extends OptimizerRules.OptimizerRule<RegexExtract> {
        @Override
        protected LogicalPlan rule(RegexExtract re) {
            return pushGeneratingPlanPastProjectAndOrderBy(re, re.extractedFields());
        }
    }

    protected static class PushDownEnrich extends OptimizerRules.OptimizerRule<Enrich> {
        @Override
        protected LogicalPlan rule(Enrich en) {
            return pushGeneratingPlanPastProjectAndOrderBy(en, asAttributes(en.enrichFields()));
        }
    }

    /**
     * Pushes LogicalPlans which generate new attributes (Eval, Grok/Dissect, Enrich), past OrderBys and Projections.
     * Although it seems arbitrary whether the OrderBy or the Eval is executed first, this transformation ensures that OrderBys only
     * separated by an eval can be combined by PushDownAndCombineOrderBy.
     *
     * E.g.:
     *
     * ... | sort a | eval x = b + 1 | sort x
     *
     * becomes
     *
     * ... | eval x = b + 1 | sort a | sort x
     *
     * Ordering the Evals before the OrderBys has the advantage that it's always possible to order the plans like this.
     * E.g., in the example above it would not be possible to put the eval after the two orderBys.
     *
     * In case one of the Eval's fields would shadow the orderBy's attributes, we rename the attribute first.
     *
     * E.g.
     *
     * ... | sort a | eval a = b + 1 | ...
     *
     * becomes
     *
     * ... | eval $$a = a | eval a = b + 1 | sort $$a | drop $$a
     */
    private static LogicalPlan pushGeneratingPlanPastProjectAndOrderBy(UnaryPlan generatingPlan, List<Attribute> generatedAttributes) {
        LogicalPlan child = generatingPlan.child();

        if (child instanceof OrderBy orderBy) {
            Set<String> evalFieldNames = new LinkedHashSet<>(Expressions.names(generatedAttributes));

            // Look for attributes in the OrderBy's expressions and create aliases with temporary names for them.
            AttributeReplacement nonShadowedOrders = renameAttributesInExpressions(evalFieldNames, orderBy.order());

            AttributeMap<Alias> aliasesForShadowedOrderByAttrs = nonShadowedOrders.replacedAttributes;
            @SuppressWarnings("unchecked")
            List<Order> newOrder = (List<Order>) (List<?>) nonShadowedOrders.rewrittenExpressions;

            if (aliasesForShadowedOrderByAttrs.isEmpty() == false) {
                List<Alias> newAliases = new ArrayList<>(aliasesForShadowedOrderByAttrs.values());

                LogicalPlan plan = new Eval(orderBy.source(), orderBy.child(), newAliases);
                plan = generatingPlan.replaceChild(plan);
                plan = new OrderBy(orderBy.source(), plan, newOrder);
                plan = new Project(generatingPlan.source(), plan, generatingPlan.output());

                return plan;
            }

            return orderBy.replaceChild(generatingPlan.replaceChild(orderBy.child()));
        } else if (child instanceof Project) {
            var projectWithEvalChild = pushDownPastProject(generatingPlan);
            return projectWithEvalChild.withProjections(mergeOutputExpressions(generatedAttributes, projectWithEvalChild.projections()));
        }

        return generatingPlan;
    }

    private record AttributeReplacement(List<Expression> rewrittenExpressions, AttributeMap<Alias> replacedAttributes) {};

    /**
     * Replace attributes in the given expressions by assigning them temporary names.
     * Returns the rewritten expressions and a map with an alias for each replaced attribute; the rewritten expressions reference
     * these aliases.
     */
    private static AttributeReplacement renameAttributesInExpressions(
        Set<String> attributeNamesToRename,
        List<? extends Expression> expressions
    ) {
        AttributeMap<Alias> aliasesForReplacedAttributes = new AttributeMap<>();
        List<Expression> rewrittenExpressions = new ArrayList<>();

        for (Expression expr : expressions) {
            rewrittenExpressions.add(expr.transformUp(Attribute.class, attr -> {
                if (attributeNamesToRename.contains(attr.name())) {
                    Alias renamedAttribute = aliasesForReplacedAttributes.computeIfAbsent(attr, a -> {
                        String tempName = SubstituteSurrogates.rawTemporaryName(a.name(), "temp_name", a.id().toString());
                        // TODO: this should be synthetic
                        return new Alias(a.source(), tempName, null, a, null, false);
                    });
                    return renamedAttribute.toAttribute();
                }

                return attr;
            }));
        }

        return new AttributeReplacement(rewrittenExpressions, aliasesForReplacedAttributes);
    }

    protected static class PushDownAndCombineOrderBy extends OptimizerRules.OptimizerRule<OrderBy> {
        @Override
        protected LogicalPlan rule(OrderBy orderBy) {
            LogicalPlan child = orderBy.child();

            if (child instanceof OrderBy childOrder) {
                // combine orders
                return new OrderBy(orderBy.source(), childOrder.child(), orderBy.order());
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

    /**
     * This adds an explicit TopN node to a plan that only has an OrderBy right before Lucene.
     * To date, the only known use case that "needs" this is a query of the form
     * from test
     * | sort emp_no
     * | mv_expand first_name
     * | rename first_name AS x
     * | where x LIKE "*a*"
     * | limit 15
     *
     * or
     *
     * from test
     * | sort emp_no
     * | mv_expand first_name
     * | sort first_name
     * | limit 15
     *
     * PushDownAndCombineLimits rule will copy the "limit 15" after "sort emp_no" if there is no filter on the expanded values
     * OR if there is no sort between "limit" and "mv_expand".
     * But, since this type of query has such a filter, the "sort emp_no" will have no limit when it reaches the current rule.
     */
    static class AddDefaultTopN extends ParameterizedOptimizerRule<LogicalPlan, LogicalOptimizerContext> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan, LogicalOptimizerContext context) {
            if (plan instanceof UnaryPlan unary && unary.child() instanceof OrderBy order && order.child() instanceof EsRelation relation) {
                var limit = new Literal(plan.source(), context.configuration().resultTruncationMaxSize(), DataTypes.INTEGER);
                return unary.replaceChild(new TopN(plan.source(), relation, order.order(), limit));
            }
            return plan;
        }
    }

    public static class ReplaceRegexMatch extends OptimizerRules.ReplaceRegexMatch {

        protected Expression regexToEquals(RegexMatch<?> regexMatch, Literal literal) {
            return new Equals(regexMatch.source(), regexMatch.field(), literal);
        }
    }

    /**
     * Replace nested expressions inside an aggregate with synthetic eval (which end up being projected away by the aggregate).
     * stats sum(a + 1) by x % 2
     * becomes
     * eval `a + 1` = a + 1, `x % 2` = x % 2 | stats sum(`a+1`_ref) by `x % 2`_ref
     */
    static class ReplaceStatsNestedExpressionWithEval extends OptimizerRules.OptimizerRule<Aggregate> {

        @Override
        protected LogicalPlan rule(Aggregate aggregate) {
            List<Alias> evals = new ArrayList<>();
            Map<String, Attribute> evalNames = new HashMap<>();
            List<Expression> newGroupings = new ArrayList<>(aggregate.groupings());
            boolean groupingChanged = false;

            // start with the groupings since the aggs might duplicate it
            for (int i = 0, s = newGroupings.size(); i < s; i++) {
                Expression g = newGroupings.get(i);
                // move the alias into an eval and replace it with its attribute
                if (g instanceof Alias as) {
                    groupingChanged = true;
                    var attr = as.toAttribute();
                    evals.add(as);
                    evalNames.put(as.name(), attr);
                    newGroupings.set(i, attr);
                }
            }

            Holder<Boolean> aggsChanged = new Holder<>(false);
            List<? extends NamedExpression> aggs = aggregate.aggregates();
            List<NamedExpression> newAggs = new ArrayList<>(aggs.size());

            // map to track common expressions
            Map<Expression, Attribute> expToAttribute = new HashMap<>();
            for (Alias a : evals) {
                expToAttribute.put(a.child().canonical(), a.toAttribute());
            }

            int[] counter = new int[] { 0 };
            // for the aggs make sure to unwrap the agg function and check the existing groupings
            for (NamedExpression agg : aggs) {
                NamedExpression a = (NamedExpression) agg.transformDown(Alias.class, as -> {
                    // if the child is a nested expression
                    Expression child = as.child();

                    // shortcut for common scenario
                    if (child instanceof AggregateFunction af && af.field() instanceof Attribute) {
                        return as;
                    }

                    // check if the alias matches any from grouping otherwise unwrap it
                    Attribute ref = evalNames.get(as.name());
                    if (ref != null) {
                        aggsChanged.set(true);
                        return ref;
                    }

                    // 1. look for the aggregate function
                    var replaced = child.transformUp(AggregateFunction.class, af -> {
                        Expression result = af;

                        Expression field = af.field();
                        // 2. if the field is a nested expression (not attribute or literal), replace it
                        if (field instanceof Attribute == false && field.foldable() == false) {
                            // 3. create a new alias if one doesn't exist yet no reference
                            Attribute attr = expToAttribute.computeIfAbsent(field.canonical(), k -> {
                                Alias newAlias = new Alias(k.source(), syntheticName(k, af, counter[0]++), null, k, null, true);
                                evals.add(newAlias);
                                aggsChanged.set(true);
                                return newAlias.toAttribute();
                            });
                            // replace field with attribute
                            List<Expression> newChildren = new ArrayList<>(af.children());
                            newChildren.set(0, attr);
                            result = af.replaceChildren(newChildren);
                        }
                        return result;
                    });

                    return as.replaceChild(replaced);
                });

                newAggs.add(a);
            }

            if (evals.size() > 0) {
                var groupings = groupingChanged ? newGroupings : aggregate.groupings();
                var aggregates = aggsChanged.get() ? newAggs : aggregate.aggregates();

                var newEval = new Eval(aggregate.source(), aggregate.child(), evals);
                aggregate = new Aggregate(aggregate.source(), newEval, groupings, aggregates);
            }

            return aggregate;
        }

        static String syntheticName(Expression expression, AggregateFunction af, int counter) {
            return SubstituteSurrogates.temporaryName(expression, af, counter);
        }
    }

    /**
     * Replace nested expressions over aggregates with synthetic eval post the aggregation
     * stats a = sum(a) + min(b) by x
     * becomes
     * stats a1 = sum(a), a2 = min(b) by x | eval a = a1 + a2 | keep a, x
     *
     * Since the logic is very similar, this rule also handles duplicate aggregate functions to avoid duplicate compute
     * stats a = min(x), b = min(x), c = count(*), d = count() by g
     * becomes
     * stats a = min(x), c = count(*) by g | eval b = a, d = c | keep a, b, c, d, g
     */
    static class ReplaceStatsAggExpressionWithEval extends OptimizerRules.OptimizerRule<Aggregate> {
        ReplaceStatsAggExpressionWithEval() {
            super(TransformDirection.UP);
        }

        @Override
        protected LogicalPlan rule(Aggregate aggregate) {
            // build alias map
            AttributeMap<Expression> aliases = new AttributeMap<>();
            aggregate.forEachExpressionUp(Alias.class, a -> aliases.put(a.toAttribute(), a.child()));

            // break down each aggregate into AggregateFunction
            // preserve the projection at the end
            List<? extends NamedExpression> aggs = aggregate.aggregates();

            // root/naked aggs
            Map<AggregateFunction, Alias> rootAggs = Maps.newLinkedHashMapWithExpectedSize(aggs.size());
            // evals (original expression relying on multiple aggs)
            List<Alias> newEvals = new ArrayList<>();
            List<NamedExpression> newProjections = new ArrayList<>();
            // track the aggregate aggs (including grouping which is not an AggregateFunction)
            List<NamedExpression> newAggs = new ArrayList<>();

            Holder<Boolean> changed = new Holder<>(false);
            int[] counter = new int[] { 0 };

            for (NamedExpression agg : aggs) {
                if (agg instanceof Alias as) {
                    // if the child a nested expression
                    Expression child = as.child();

                    // common case - handle duplicates
                    if (child instanceof AggregateFunction af) {
                        AggregateFunction canonical = (AggregateFunction) af.canonical();
                        Expression field = canonical.field().transformUp(e -> aliases.resolve(e, e));
                        canonical = (AggregateFunction) canonical.replaceChildren(
                            CollectionUtils.combine(singleton(field), canonical.parameters())
                        );

                        Alias found = rootAggs.get(canonical);
                        // aggregate is new
                        if (found == null) {
                            rootAggs.put(canonical, as);
                            newAggs.add(as);
                            newProjections.add(as.toAttribute());
                        }
                        // agg already exists - preserve the current alias but point it to the existing agg
                        // thus don't add it to the list of aggs as we don't want duplicated compute
                        else {
                            changed.set(true);
                            newProjections.add(as.replaceChild(found.toAttribute()));
                        }
                    }
                    // nested expression over aggregate function - replace them with reference and move the expression into a
                    // follow-up eval
                    else {
                        Holder<Boolean> transformed = new Holder<>(false);
                        Expression aggExpression = child.transformUp(AggregateFunction.class, af -> {
                            transformed.set(true);
                            changed.set(true);

                            AggregateFunction canonical = (AggregateFunction) af.canonical();
                            Alias alias = rootAggs.get(canonical);
                            if (alias == null) {
                                // create synthetic alias ove the found agg function
                                alias = new Alias(
                                    af.source(),
                                    syntheticName(canonical, child, counter[0]++),
                                    as.qualifier(),
                                    canonical,
                                    null,
                                    true
                                );
                                // and remember it to remove duplicates
                                rootAggs.put(canonical, alias);
                                // add it to the list of aggregates and continue
                                newAggs.add(alias);
                            }
                            // (even when found) return a reference to it
                            return alias.toAttribute();
                        });

                        Alias alias = as;
                        if (transformed.get()) {
                            // if at least a change occurred, update the alias and add it to the eval
                            alias = as.replaceChild(aggExpression);
                            newEvals.add(alias);
                        }
                        // aliased grouping
                        else {
                            newAggs.add(alias);
                        }

                        newProjections.add(alias.toAttribute());
                    }
                }
                // not an alias (e.g. grouping field)
                else {
                    newAggs.add(agg);
                    newProjections.add(agg.toAttribute());
                }
            }

            LogicalPlan plan = aggregate;
            if (changed.get()) {
                Source source = aggregate.source();
                plan = new Aggregate(source, aggregate.child(), aggregate.groupings(), newAggs);
                if (newEvals.size() > 0) {
                    plan = new Eval(source, plan, newEvals);
                }
                // preserve initial projection
                plan = new Project(source, plan, newProjections);
            }

            return plan;
        }

        static String syntheticName(Expression expression, Expression af, int counter) {
            return SubstituteSurrogates.temporaryName(expression, af, counter);
        }
    }

    /**
     * Replace aliasing evals (eval x=a) with a projection which can be further combined / simplified.
     * The rule gets applied only if there's another project (Project/Stats) above it.
     *
     * Needs to take into account shadowing of potentially intermediate fields:
     * eval x = a + 1, y = x, z = y + 1, y = z, w = y + 1
     * The output should be
     * eval x = a + 1, z = a + 1 + 1, w = a + 1 + 1
     * project x, z, z as y, w
     */
    static class ReplaceAliasingEvalWithProject extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan logicalPlan) {
            Holder<Boolean> enabled = new Holder<>(false);

            return logicalPlan.transformDown(p -> {
                // found projection, turn enable flag on
                if (p instanceof Aggregate || p instanceof Project) {
                    enabled.set(true);
                } else if (enabled.get() && p instanceof Eval eval) {
                    p = rule(eval);
                }

                return p;
            });
        }

        private LogicalPlan rule(Eval eval) {
            LogicalPlan plan = eval;

            // holds simple aliases such as b = a, c = b, d = c
            AttributeMap<Expression> basicAliases = new AttributeMap<>();
            // same as above but keeps the original expression
            AttributeMap<NamedExpression> basicAliasSources = new AttributeMap<>();

            List<Alias> keptFields = new ArrayList<>();

            var fields = eval.fields();
            for (int i = 0, size = fields.size(); i < size; i++) {
                Alias field = fields.get(i);
                Expression child = field.child();
                var attribute = field.toAttribute();
                // put the aliases in a separate map to separate the underlying resolve from other aliases
                if (child instanceof Attribute) {
                    basicAliases.put(attribute, child);
                    basicAliasSources.put(attribute, field);
                } else {
                    // be lazy and start replacing name aliases only if needed
                    if (basicAliases.size() > 0) {
                        // update the child through the field
                        field = (Alias) field.transformUp(e -> basicAliases.resolve(e, e));
                    }
                    keptFields.add(field);
                }
            }

            // at least one alias encountered, move it into a project
            if (basicAliases.size() > 0) {
                // preserve the eval output (takes care of shadowing and order) but replace the basic aliases
                List<NamedExpression> projections = new ArrayList<>(eval.output());
                // replace the removed aliases with their initial definition - however use the output to preserve the shadowing
                for (int i = projections.size() - 1; i >= 0; i--) {
                    NamedExpression project = projections.get(i);
                    projections.set(i, basicAliasSources.getOrDefault(project, project));
                }

                LogicalPlan child = eval.child();
                if (keptFields.size() > 0) {
                    // replace the eval with just the kept fields
                    child = new Eval(eval.source(), eval.child(), keptFields);
                }
                // put the projection in place
                plan = new Project(eval.source(), child, projections);
            }

            return plan;
        }
    }

    private abstract static class ParameterizedOptimizerRule<SubPlan extends LogicalPlan, P> extends ParameterizedRule<
        SubPlan,
        LogicalPlan,
        P> {

        public final LogicalPlan apply(LogicalPlan plan, P context) {
            return plan.transformDown(typeToken(), t -> rule(t, context));
        }

        protected abstract LogicalPlan rule(SubPlan plan, P context);
    }

    /**
     * Normalize aggregation functions by:
     * 1. replaces reference to field attributes with their source
     * 2. in case of Count, aligns the various forms (Count(1), Count(0), Count(), Count(*)) to Count(*)
     */
    // TODO waiting on https://github.com/elastic/elasticsearch/issues/100634
    static class NormalizeAggregate extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            AttributeMap<Expression> aliases = new AttributeMap<>();

            // traverse the tree bottom-up
            // 1. if it's Aggregate, normalize the aggregates
            // regardless, collect the attributes but only if they refer to an attribute or literal
            plan = plan.transformUp(p -> {
                if (p instanceof Aggregate agg) {
                    p = normalize(agg, aliases);
                }
                p.forEachExpression(Alias.class, a -> {
                    var child = a.child();
                    if (child.foldable() || child instanceof NamedExpression) {
                        aliases.putIfAbsent(a.toAttribute(), child);
                    }
                });

                return p;
            });
            return plan;
        }

        private static LogicalPlan normalize(Aggregate aggregate, AttributeMap<Expression> aliases) {
            var aggs = aggregate.aggregates();
            List<NamedExpression> newAggs = new ArrayList<>(aggs.size());
            final Holder<Boolean> changed = new Holder<>(false);

            for (NamedExpression agg : aggs) {
                var newAgg = (NamedExpression) agg.transformDown(AggregateFunction.class, af -> {
                    // replace field reference
                    if (af.field() instanceof NamedExpression ne) {
                        Attribute attr = ne.toAttribute();
                        var resolved = aliases.resolve(attr, attr);
                        if (resolved != attr) {
                            changed.set(true);
                            var newChildren = CollectionUtils.combine(Collections.singletonList(resolved), af.parameters());
                            // update the reference so Count can pick it up
                            af = (AggregateFunction) af.replaceChildren(newChildren);
                        }
                    }
                    // handle Count(*)
                    if (af instanceof Count count) {
                        var field = af.field();
                        if (field.foldable()) {
                            var fold = field.fold();
                            if (fold != null && StringUtils.WILDCARD.equals(fold) == false) {
                                changed.set(true);
                                var source = count.source();
                                af = new Count(source, new Literal(source, StringUtils.WILDCARD, DataTypes.KEYWORD));
                            }
                        }
                    }
                    return af;
                });
                newAggs.add(newAgg);
            }
            return changed.get() ? new Aggregate(aggregate.source(), aggregate.child(), aggregate.groupings(), newAggs) : aggregate;
        }
    }
}
