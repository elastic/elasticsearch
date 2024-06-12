/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.analyzer.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Order;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.StringPattern;
import org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules;
import org.elasticsearch.xpack.esql.core.plan.logical.Limit;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.core.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.optimizer.rules.AddDefaultTopN;
import org.elasticsearch.xpack.esql.optimizer.rules.BooleanFunctionEqualsElimination;
import org.elasticsearch.xpack.esql.optimizer.rules.BooleanSimplification;
import org.elasticsearch.xpack.esql.optimizer.rules.CombineDisjunctionsToIn;
import org.elasticsearch.xpack.esql.optimizer.rules.CombineEvals;
import org.elasticsearch.xpack.esql.optimizer.rules.CombineProjections;
import org.elasticsearch.xpack.esql.optimizer.rules.ConstantFolding;
import org.elasticsearch.xpack.esql.optimizer.rules.ConvertStringToByteRef;
import org.elasticsearch.xpack.esql.optimizer.rules.DuplicateLimitAfterMvExpand;
import org.elasticsearch.xpack.esql.optimizer.rules.FoldNull;
import org.elasticsearch.xpack.esql.optimizer.rules.LiteralsOnTheRight;
import org.elasticsearch.xpack.esql.optimizer.rules.PartiallyFoldCase;
import org.elasticsearch.xpack.esql.optimizer.rules.PropagateEmptyRelation;
import org.elasticsearch.xpack.esql.optimizer.rules.PropagateEquals;
import org.elasticsearch.xpack.esql.optimizer.rules.PropagateEvalFoldables;
import org.elasticsearch.xpack.esql.optimizer.rules.PropagateNullable;
import org.elasticsearch.xpack.esql.optimizer.rules.PruneColumns;
import org.elasticsearch.xpack.esql.optimizer.rules.PruneEmptyPlans;
import org.elasticsearch.xpack.esql.optimizer.rules.PruneFilters;
import org.elasticsearch.xpack.esql.optimizer.rules.PruneLiteralsInOrderBy;
import org.elasticsearch.xpack.esql.optimizer.rules.PruneOrderByBeforeStats;
import org.elasticsearch.xpack.esql.optimizer.rules.PruneRedundantSortClauses;
import org.elasticsearch.xpack.esql.optimizer.rules.PushDownAndCombineFilters;
import org.elasticsearch.xpack.esql.optimizer.rules.PushDownAndCombineLimits;
import org.elasticsearch.xpack.esql.optimizer.rules.PushDownAndCombineOrderBy;
import org.elasticsearch.xpack.esql.optimizer.rules.PushDownEnrich;
import org.elasticsearch.xpack.esql.optimizer.rules.SetAsOptimized;
import org.elasticsearch.xpack.esql.optimizer.rules.SimplifyComparisonsArithmetics;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.asAttributes;
import static org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.TransformDirection;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputExpressions;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.SubstituteSurrogates.rawTemporaryName;

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

    protected static Batch<LogicalPlan> substitutions() {
        return new Batch<>(
            "Substitutions",
            Limiter.ONCE,
            new ReplaceLookupWithJoin(),
            new RemoveStatsOverride(),
            // first extract nested expressions inside aggs
            new ReplaceStatsNestedExpressionWithEval(),
            // then extract nested aggs top-level
            new ReplaceStatsAggExpressionWithEval(),
            // lastly replace surrogate functions
            new SubstituteSurrogates(),
            new ReplaceRegexMatch(),
            new ReplaceTrivialTypeConversions(),
            new ReplaceAliasingEvalWithProject(),
            new SkipQueryOnEmptyMappings(),
            new SubstituteSpatialSurrogates(),
            new ReplaceOrderByExpressionWithEval()
            // new NormalizeAggregate(), - waits on https://github.com/elastic/elasticsearch/issues/100634
        );
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
            new PropagateEvalFoldables(),
            new ConstantFolding(),
            new PartiallyFoldCase(),
            // boolean
            new BooleanSimplification(),
            new LiteralsOnTheRight(),
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
        var skip = new Batch<>("Skip Compute", new SkipQueryOnLimitZero());
        var defaultTopN = new Batch<>("Add default TopN", new AddDefaultTopN());
        var label = new Batch<>("Set as Optimized", Limiter.ONCE, new SetAsOptimized());

        return asList(substitutions(), operators(), skip, cleanup(), defaultTopN, label);
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
                if (Alias.unwrap(agg) instanceof AggregateFunction af) {
                    if ((af instanceof SurrogateExpression se && se.surrogate() != null) == false) {
                        aggFuncToAttr.put(af, agg.toAttribute());
                    }
                }
            }

            int[] counter = new int[] { 0 };
            // 0. check list of surrogate expressions
            for (NamedExpression agg : aggs) {
                Expression e = Alias.unwrap(agg);
                if (e instanceof SurrogateExpression sf && sf.surrogate() != null) {
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
                if (newAggs.isEmpty() == false) {
                    plan = new Aggregate(source, aggregate.child(), aggregate.groupings(), newAggs);
                } else {
                    // All aggs actually have been surrogates for (foldable) expressions, e.g.
                    // \_Aggregate[[],[AVG([1, 2][INTEGER]) AS s]]
                    // Replace by a local relation with one row, followed by an eval, e.g.
                    // \_Eval[[MVAVG([1, 2][INTEGER]) AS s]]
                    // \_LocalRelation[[{e}#21],[ConstantNullBlock[positions=1]]]
                    plan = new LocalRelation(
                        source,
                        List.of(new EmptyAttribute(source)),
                        LocalSupplier.of(new Block[] { BlockUtils.constantBlock(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, null, 1) })
                    );
                }
                // 5. force the initial projection in place
                if (transientEval.isEmpty() == false) {
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

    /**
     * Currently this works similarly to SurrogateExpression, leaving the logic inside the expressions,
     * so each can decide for itself whether or not to change to a surrogate expression.
     * But what is actually being done is similar to LiteralsOnTheRight. We can consider in the future moving
     * this in either direction, reducing the number of rules, but for now,
     * it's a separate rule to reduce the risk of unintended interactions with other rules.
     */
    static class SubstituteSpatialSurrogates extends OptimizerRules.OptimizerExpressionRule<SpatialRelatesFunction> {

        SubstituteSpatialSurrogates() {
            super(TransformDirection.UP);
        }

        @Override
        protected SpatialRelatesFunction rule(SpatialRelatesFunction function) {
            return function.surrogate();
        }
    }

    static class ReplaceOrderByExpressionWithEval extends OptimizerRules.OptimizerRule<OrderBy> {
        private static int counter = 0;

        @Override
        protected LogicalPlan rule(OrderBy orderBy) {
            int size = orderBy.order().size();
            List<Alias> evals = new ArrayList<>(size);
            List<Order> newOrders = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                var order = orderBy.order().get(i);
                if (order.child() instanceof Attribute == false) {
                    var name = rawTemporaryName("order_by", String.valueOf(i), String.valueOf(counter++));
                    var eval = new Alias(order.child().source(), name, order.child());
                    newOrders.add(order.replaceChildren(List.of(eval.toAttribute())));
                    evals.add(eval);
                } else {
                    newOrders.add(order);
                }
            }
            if (evals.isEmpty()) {
                return orderBy;
            } else {
                var newOrderBy = new OrderBy(orderBy.source(), new Eval(orderBy.source(), orderBy.child(), evals), newOrders);
                return new Project(orderBy.source(), newOrderBy, orderBy.output());
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

    static class SkipQueryOnLimitZero extends OptimizerRules.SkipQueryOnLimitZero {

        @Override
        protected LogicalPlan skipPlan(Limit limit) {
            return LogicalPlanOptimizer.skipPlan(limit);
        }
    }

    static class SkipQueryOnEmptyMappings extends OptimizerRules.OptimizerRule<EsRelation> {

        @Override
        protected LogicalPlan rule(EsRelation plan) {
            return plan.index().concreteIndices().isEmpty() ? new LocalRelation(plan.source(), plan.output(), LocalSupplier.EMPTY) : plan;
        }
    }

    public static LogicalPlan skipPlan(UnaryPlan plan) {
        return new LocalRelation(plan.source(), plan.output(), LocalSupplier.EMPTY);
    }

    public static LogicalPlan skipPlan(UnaryPlan plan, LocalSupplier supplier) {
        return new LocalRelation(plan.source(), plan.output(), supplier);
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
    public static LogicalPlan pushGeneratingPlanPastProjectAndOrderBy(UnaryPlan generatingPlan, List<Attribute> generatedAttributes) {
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

    public static Project pushDownPastProject(UnaryPlan parent) {
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

    private static class ReplaceLookupWithJoin extends OptimizerRules.OptimizerRule<Lookup> {

        ReplaceLookupWithJoin() {
            super(TransformDirection.UP);
        }

        @Override
        protected LogicalPlan rule(Lookup lookup) {
            // left join between the main relation and the local, lookup relation
            return new Join(lookup.source(), lookup.child(), lookup.localRelation(), lookup.joinConfig());
        }
    }

    public static class ReplaceRegexMatch extends org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.OptimizerExpressionRule<
        RegexMatch<?>> {

        ReplaceRegexMatch() {
            super(org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.TransformDirection.DOWN);
        }

        @Override
        public Expression rule(RegexMatch<?> regexMatch) {
            Expression e = regexMatch;
            StringPattern pattern = regexMatch.pattern();
            if (pattern.matchesAll()) {
                e = new IsNotNull(e.source(), regexMatch.field());
            } else {
                String match = pattern.exactMatch();
                if (match != null) {
                    Literal literal = new Literal(regexMatch.source(), match, DataType.KEYWORD);
                    e = regexToEquals(regexMatch, literal);
                }
            }
            return e;
        }

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
            Map<GroupingFunction, Attribute> groupingAttributes = new HashMap<>();
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
                    if (as.child() instanceof GroupingFunction gf) {
                        groupingAttributes.put(gf, attr);
                    }
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
                                return newAlias.toAttribute();
                            });
                            aggsChanged.set(true);
                            // replace field with attribute
                            List<Expression> newChildren = new ArrayList<>(af.children());
                            newChildren.set(0, attr);
                            result = af.replaceChildren(newChildren);
                        }
                        return result;
                    });
                    // replace any grouping functions with their references pointing to the added synthetic eval
                    replaced = replaced.transformDown(GroupingFunction.class, gf -> {
                        aggsChanged.set(true);
                        // should never return null, as it's verified.
                        // but even if broken, the transform will fail safely; otoh, returning `gf` will fail later due to incorrect plan.
                        return groupingAttributes.get(gf);
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
     * The rule also considers expressions applied over groups:
     * stats a = x + 1 by x becomes stats by x | eval a = x + 1 | keep a, x
     * And to combine the two:
     * stats a = x + count(*) by x
     * becomes
     * stats a1 = count(*) by x | eval a = x + a1 | keep a1, x
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

            // break down each aggregate into AggregateFunction and/or grouping key
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
                    // nested expression over aggregate function or groups
                    // replace them with reference and move the expression into a follow-up eval
                    else {
                        changed.set(true);
                        Expression aggExpression = child.transformUp(AggregateFunction.class, af -> {
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

                        Alias alias = as.replaceChild(aggExpression);
                        newEvals.add(alias);
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

    /**
     * Replace type converting eval with aliasing eval when type change does not occur.
     * A following {@link ReplaceAliasingEvalWithProject} will effectively convert {@link ReferenceAttribute} into {@link FieldAttribute},
     * something very useful in local physical planning.
     */
    static class ReplaceTrivialTypeConversions extends OptimizerRules.OptimizerRule<Eval> {
        @Override
        protected LogicalPlan rule(Eval eval) {
            return eval.transformExpressionsOnly(AbstractConvertFunction.class, convert -> {
                if (convert.field() instanceof FieldAttribute fa && fa.dataType() == convert.dataType()) {
                    return fa;
                }
                return convert;
            });
        }
    }

    /**
     * Rule that removes Aggregate overrides in grouping, aggregates and across them inside.
     * The overrides appear when the same alias is used multiple times in aggregations and/or groupings:
     * STATS x = COUNT(*), x = MIN(a) BY x = b + 1, x = c + 10
     * becomes
     * STATS BY x = c + 10
     * That is the last declaration for a given alias, overrides all the other declarations, with
     * groups having priority vs aggregates.
     * Separately, it replaces expressions used as group keys inside the aggregates with references:
     * STATS max(a + b + 1) BY a + b
     * becomes
     * STATS max($x + 1) BY $x = a + b
     */
    private static class RemoveStatsOverride extends AnalyzerRules.AnalyzerRule<Aggregate> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan rule(Aggregate agg) {
            return agg.resolved() ? removeAggDuplicates(agg) : agg;
        }

        private static Aggregate removeAggDuplicates(Aggregate agg) {
            var groupings = agg.groupings();
            var aggregates = agg.aggregates();

            groupings = removeDuplicateNames(groupings);
            aggregates = removeDuplicateNames(aggregates);

            // replace EsqlAggregate with Aggregate
            return new Aggregate(agg.source(), agg.child(), groupings, aggregates);
        }

        private static <T extends Expression> List<T> removeDuplicateNames(List<T> list) {
            var newList = new ArrayList<>(list);
            var nameSet = Sets.newHashSetWithExpectedSize(list.size());

            // remove duplicates
            for (int i = list.size() - 1; i >= 0; i--) {
                var element = list.get(i);
                var name = Expressions.name(element);
                if (nameSet.add(name) == false) {
                    newList.remove(i);
                }
            }
            return newList.size() == list.size() ? list : newList;
        }
    }

    public abstract static class ParameterizedOptimizerRule<SubPlan extends LogicalPlan, P> extends ParameterizedRule<
        SubPlan,
        LogicalPlan,
        P> {

        public final LogicalPlan apply(LogicalPlan plan, P context) {
            return plan.transformDown(typeToken(), t -> rule(t, context));
        }

        protected abstract LogicalPlan rule(SubPlan plan, P context);
    }

}
