/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Order;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.core.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
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
import org.elasticsearch.xpack.esql.optimizer.rules.PushDownEval;
import org.elasticsearch.xpack.esql.optimizer.rules.PushDownRegexExtract;
import org.elasticsearch.xpack.esql.optimizer.rules.RemoveStatsOverride;
import org.elasticsearch.xpack.esql.optimizer.rules.ReplaceAliasingEvalWithProject;
import org.elasticsearch.xpack.esql.optimizer.rules.ReplaceLimitAndSortAsTopN;
import org.elasticsearch.xpack.esql.optimizer.rules.ReplaceLookupWithJoin;
import org.elasticsearch.xpack.esql.optimizer.rules.ReplaceOrderByExpressionWithEval;
import org.elasticsearch.xpack.esql.optimizer.rules.ReplaceRegexMatch;
import org.elasticsearch.xpack.esql.optimizer.rules.ReplaceStatsAggExpressionWithEval;
import org.elasticsearch.xpack.esql.optimizer.rules.ReplaceStatsNestedExpressionWithEval;
import org.elasticsearch.xpack.esql.optimizer.rules.ReplaceTrivialTypeConversions;
import org.elasticsearch.xpack.esql.optimizer.rules.SetAsOptimized;
import org.elasticsearch.xpack.esql.optimizer.rules.SimplifyComparisonsArithmetics;
import org.elasticsearch.xpack.esql.optimizer.rules.SkipQueryOnEmptyMappings;
import org.elasticsearch.xpack.esql.optimizer.rules.SkipQueryOnLimitZero;
import org.elasticsearch.xpack.esql.optimizer.rules.SplitInWithFoldableValue;
import org.elasticsearch.xpack.esql.optimizer.rules.SubstituteSpatialSurrogates;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Project;
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
import static org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules.TransformDirection;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputExpressions;

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
    public static class SubstituteSurrogates extends OptimizerRules.OptimizerRule<Aggregate> {

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

        public static String temporaryName(Expression inner, Expression outer, int suffix) {
            String in = toString(inner);
            String out = toString(outer);
            return rawTemporaryName(in, out, String.valueOf(suffix));
        }

        public static String rawTemporaryName(String inner, String outer, String suffix) {
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

    public static LogicalPlan skipPlan(UnaryPlan plan) {
        return new LocalRelation(plan.source(), plan.output(), LocalSupplier.EMPTY);
    }

    public static LogicalPlan skipPlan(UnaryPlan plan, LocalSupplier supplier) {
        return new LocalRelation(plan.source(), plan.output(), supplier);
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
