/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.rules.AddDefaultTopN;
import org.elasticsearch.xpack.esql.optimizer.rules.BooleanFunctionEqualsElimination;
import org.elasticsearch.xpack.esql.optimizer.rules.BooleanSimplification;
import org.elasticsearch.xpack.esql.optimizer.rules.CombineBinaryComparisons;
import org.elasticsearch.xpack.esql.optimizer.rules.CombineDisjunctions;
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
import org.elasticsearch.xpack.esql.optimizer.rules.ReplaceAggregatesWithNull;
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
import org.elasticsearch.xpack.esql.optimizer.rules.SubstituteSurrogates;
import org.elasticsearch.xpack.esql.optimizer.rules.TranslateMetricsAggregate;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputExpressions;

/**
 * <p>This class is part of the planner</p>
 * <p>Global optimizations based strictly on the structure of the query (i.e. not factoring in information about the backing indices).
 * The bulk of query transformations happen in this step. </p>
 *
 * <p>Global optimizations based strictly on the structure of the query (i.e. not factoring in information about the backing indices).  The
 * bulk of query transformations happen in this step. This has three important sub-phases:</p>
 * <ul>
 *     <li>The {@link LogicalPlanOptimizer#substitutions()} phase rewrites things to expand out shorthand in the syntax.  For example,
 *     a nested expression embedded in a stats gets replaced with an eval followed by a stats, followed by another eval.  This phase
 *     also applies surrogates, such as replacing an average with a sum divided by a count.</li>
 *     <li>{@link LogicalPlanOptimizer#operators()} (NB: The word "operator" is extremely overloaded and referrers to many different
 *     things.) transform the tree in various different ways.  This includes folding (i.e. computing constant expressions at parse
 *     time), combining expressions, dropping redundant clauses, and some normalization such as putting literals on the right whenever
 *     possible.  These rules are run in a loop until none of the rules make any changes to the plan (there is also a safety shut off
 *     after many iterations, although hitting that is considered a bug)</li>
 *     <li>{@link LogicalPlanOptimizer#cleanup()}  Which can replace sorts+limit with a TopN</li>
 * </ul>
 *
 * <p>Note that the {@link LogicalPlanOptimizer#operators()} and {@link LogicalPlanOptimizer#cleanup()} steps are reapplied at the
 * {@link LocalLogicalPlanOptimizer} layer.</p>
 */
public class LogicalPlanOptimizer extends ParameterizedRuleExecutor<LogicalPlan, LogicalOptimizerContext> {

    private final LogicalVerifier verifier = LogicalVerifier.INSTANCE;

    public LogicalPlanOptimizer(LogicalOptimizerContext optimizerContext) {
        super(optimizerContext);
    }

    public static String temporaryName(Expression inner, Expression outer, int suffix) {
        String in = toString(inner);
        String out = toString(outer);
        return rawTemporaryName(in, out, String.valueOf(suffix));
    }

    public static String locallyUniqueTemporaryName(String inner, String outer) {
        return FieldAttribute.SYNTHETIC_ATTRIBUTE_NAME_PREFIX + inner + "$" + outer + "$" + new NameId();
    }

    public static String rawTemporaryName(String inner, String outer, String suffix) {
        return FieldAttribute.SYNTHETIC_ATTRIBUTE_NAME_PREFIX + inner + "$" + outer + "$" + suffix;
    }

    static String toString(Expression ex) {
        return ex instanceof AggregateFunction af ? af.functionName() : extractString(ex);
    }

    static String extractString(Expression ex) {
        return ex instanceof NamedExpression ne ? ne.name() : limitToString(ex.sourceText()).replace(' ', '_');
    }

    static int TO_STRING_LIMIT = 16;

    static String limitToString(String string) {
        return string.length() > TO_STRING_LIMIT ? string.substring(0, TO_STRING_LIMIT - 1) + ">" : string;
    }

    public LogicalPlan optimize(LogicalPlan verified) {
        var optimized = execute(verified);

        Failures failures = verifier.verify(optimized);
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
        optimized.setOptimized();
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
            // translate metric aggregates after surrogate substitution and replace nested expressions with eval (again)
            new TranslateMetricsAggregate(),
            new ReplaceStatsNestedExpressionWithEval(),
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
            new ReplaceAggregatesWithNull(),
            new SplitInWithFoldableValue(),
            new PropagateEvalFoldables(),
            new ConstantFolding(),
            new SubstituteSurrogates(),
            new PartiallyFoldCase(),
            // boolean
            new BooleanSimplification(),
            new LiteralsOnTheRight(),
            // needs to occur before BinaryComparison combinations (see class)
            new PropagateEquals(),
            new PropagateNullable(),
            new BooleanFunctionEqualsElimination(),
            new CombineBinaryComparisons(),
            new CombineDisjunctions(),
            new SimplifyComparisonsArithmetics(DataType::areCompatible),
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

    public static LogicalPlan skipPlan(UnaryPlan plan) {
        return new LocalRelation(plan.source(), plan.output(), LocalSupplier.EMPTY);
    }

    public static LogicalPlan skipPlan(UnaryPlan plan, LocalSupplier supplier) {
        return new LocalRelation(plan.source(), plan.output(), supplier);
    }

    /**
     * Pushes LogicalPlans which generate new attributes (Eval, Grok/Dissect, Enrich), past OrderBys and Projections.
     * Although it seems arbitrary whether the OrderBy or the generating plan is executed first, this transformation ensures that OrderBys
     * only separated by e.g. an Eval can be combined by {@link PushDownAndCombineOrderBy}.
     * <p>
     * E.g. {@code ... | sort a | eval x = b + 1 | sort x} becomes {@code ... | eval x = b + 1 | sort a | sort x}
     * <p>
     * Ordering the generating plans before the OrderBys has the advantage that it's always possible to order the plans like this.
     * E.g., in the example above it would not be possible to put the eval after the two orderBys.
     * <p>
     * In case one of the generating plan's attributes would shadow the OrderBy's attributes, we alias the generated attribute first.
     * <p>
     * E.g. {@code ... | sort a | eval a = b + 1 | ...} becomes {@code ... | eval $$a = a | eval a = b + 1 | sort $$a | drop $$a ...}
     * <p>
     * In case the generating plan's attributes would shadow the Project's attributes, we rename the generated attributes in place.
     * <p>
     * E.g. {@code ... | rename a as z | eval a = b + 1 | ...} becomes {@code ... eval $$a = b + 1 | rename a as z, $$a as a ...}
     */
    public static <Plan extends UnaryPlan & GeneratingPlan<Plan>> LogicalPlan pushGeneratingPlanPastProjectAndOrderBy(Plan generatingPlan) {
        LogicalPlan child = generatingPlan.child();
        if (child instanceof OrderBy orderBy) {
            Set<String> evalFieldNames = new LinkedHashSet<>(Expressions.names(generatingPlan.generatedAttributes()));

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
        } else if (child instanceof Project project) {
            // We need to account for attribute shadowing: a rename might rely on a name generated in an Eval/Grok/Dissect/Enrich.
            // E.g. in:
            //
            // Eval[[2 * x{f}#1 AS y]]
            // \_Project[[x{f}#1, y{f}#2, y{f}#2 AS z]]
            //
            // Just moving the Eval down breaks z because we shadow y{f}#2.
            // Instead, we use a different alias in the Eval, eventually renaming back to y:
            //
            // Project[[x{f}#1, y{f}#2 as z, $$y{r}#3 as y]]
            // \_Eval[[2 * x{f}#1 as $$y]]

            List<Attribute> generatedAttributes = generatingPlan.generatedAttributes();

            @SuppressWarnings("unchecked")
            Plan generatingPlanWithResolvedExpressions = (Plan) resolveRenamesFromProject(generatingPlan, project);

            Set<String> namesReferencedInRenames = new HashSet<>();
            for (NamedExpression ne : project.projections()) {
                if (ne instanceof Alias as) {
                    namesReferencedInRenames.addAll(as.child().references().names());
                }
            }
            Map<String, String> renameGeneratedAttributeTo = newNamesForConflictingAttributes(
                generatingPlan.generatedAttributes(),
                namesReferencedInRenames
            );
            List<String> newNames = generatedAttributes.stream()
                .map(attr -> renameGeneratedAttributeTo.getOrDefault(attr.name(), attr.name()))
                .toList();
            Plan generatingPlanWithRenamedAttributes = generatingPlanWithResolvedExpressions.withGeneratedNames(newNames);

            // Put the project at the top, but include the generated attributes.
            // Any generated attributes that had to be renamed need to be re-renamed to their original names.
            List<NamedExpression> generatedAttributesRenamedToOriginal = new ArrayList<>(generatedAttributes.size());
            List<Attribute> renamedGeneratedAttributes = generatingPlanWithRenamedAttributes.generatedAttributes();
            for (int i = 0; i < generatedAttributes.size(); i++) {
                Attribute originalAttribute = generatedAttributes.get(i);
                Attribute renamedAttribute = renamedGeneratedAttributes.get(i);
                if (originalAttribute.name().equals(renamedAttribute.name())) {
                    generatedAttributesRenamedToOriginal.add(renamedAttribute);
                } else {
                    generatedAttributesRenamedToOriginal.add(
                        new Alias(
                            originalAttribute.source(),
                            originalAttribute.name(),
                            renamedAttribute,
                            originalAttribute.id(),
                            originalAttribute.synthetic()
                        )
                    );
                }
            }

            Project projectWithGeneratingChild = project.replaceChild(generatingPlanWithRenamedAttributes.replaceChild(project.child()));
            return projectWithGeneratingChild.withProjections(
                mergeOutputExpressions(generatedAttributesRenamedToOriginal, projectWithGeneratingChild.projections())
            );
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
                        String tempName = locallyUniqueTemporaryName(a.name(), "temp_name");
                        // TODO: this should be synthetic
                        // blocked on https://github.com/elastic/elasticsearch/issues/98703
                        return new Alias(a.source(), tempName, a, null, false);
                    });
                    return renamedAttribute.toAttribute();
                }

                return attr;
            }));
        }

        return new AttributeReplacement(rewrittenExpressions, aliasesForReplacedAttributes);
    }

    private static Map<String, String> newNamesForConflictingAttributes(
        List<Attribute> potentiallyConflictingAttributes,
        Set<String> reservedNames
    ) {
        if (reservedNames.isEmpty()) {
            return Map.of();
        }

        Map<String, String> renameAttributeTo = new HashMap<>();
        for (Attribute attr : potentiallyConflictingAttributes) {
            String name = attr.name();
            if (reservedNames.contains(name)) {
                renameAttributeTo.putIfAbsent(name, locallyUniqueTemporaryName(name, "temp_name"));
            }
        }

        return renameAttributeTo;
    }

    public static Project pushDownPastProject(UnaryPlan parent) {
        if (parent.child() instanceof Project project) {
            UnaryPlan expressionsWithResolvedAliases = resolveRenamesFromProject(parent, project);

            return project.replaceChild(expressionsWithResolvedAliases.replaceChild(project.child()));
        } else {
            throw new EsqlIllegalArgumentException("Expected child to be instance of Project");
        }
    }

    private static UnaryPlan resolveRenamesFromProject(UnaryPlan plan, Project project) {
        AttributeMap.Builder<Expression> aliasBuilder = AttributeMap.builder();
        project.forEachExpression(Alias.class, a -> aliasBuilder.put(a.toAttribute(), a.child()));
        var aliases = aliasBuilder.build();

        return (UnaryPlan) plan.transformExpressionsOnly(ReferenceAttribute.class, r -> aliases.resolve(r, r));
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
