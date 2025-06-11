/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ApplySampleCorrections;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.BooleanFunctionEqualsElimination;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.BooleanSimplification;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.CombineBinaryComparisons;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.CombineDisjunctions;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.CombineEvals;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.CombineProjections;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ConstantFolding;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ConvertStringToByteRef;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ExtractAggregateCommonFilter;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.FoldNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.LiteralsOnTheRight;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PartiallyFoldCase;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PropagateEmptyRelation;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PropagateEquals;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PropagateEvalFoldables;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PropagateInlineEvals;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PropagateNullable;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PropgateUnmappedFields;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneEmptyPlans;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneFilters;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneLiteralsInOrderBy;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneRedundantOrderBy;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneRedundantSortClauses;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneUnusedIndexMode;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineFilters;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineLimits;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineOrderBy;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineSample;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownCompletion;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownEnrich;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownEval;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownRegexExtract;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushLimitToKnn;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.RemoveStatsOverride;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceAggregateAggExpressionWithEval;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceAggregateNestedExpressionWithEval;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceAliasingEvalWithProject;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceLimitAndSortAsTopN;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceOrderByExpressionWithEval;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceRegexMatch;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceRowAsLocalRelation;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceStatsFilteredAggWithEval;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceStringCasingWithInsensitiveEquals;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceTrivialTypeConversions;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SetAsOptimized;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SimplifyComparisonsArithmetics;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SkipQueryOnEmptyMappings;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SkipQueryOnLimitZero;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SplitInWithFoldableValue;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteFilteredExpression;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogateAggregations;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogateExpressions;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogatePlans;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.PruneLeftJoinOnNullMatchingField;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;

import java.util.List;

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

    private static final List<RuleExecutor.Batch<LogicalPlan>> RULES = List.of(
        substitutions(),
        operators(),
        new Batch<>("Skip Compute", new SkipQueryOnLimitZero()),
        cleanup(),
        new Batch<>("Set as Optimized", Limiter.ONCE, new SetAsOptimized())
    );

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
        optimized.setOptimized();
        return optimized;
    }

    @Override
    protected List<Batch<LogicalPlan>> batches() {
        return RULES;
    }

    protected static Batch<LogicalPlan> substitutions() {
        return new Batch<>(
            "Substitutions",
            Limiter.ONCE,
            new ApplySampleCorrections(),
            new SubstituteSurrogatePlans(),
            // Translate filtered expressions into aggregate with filters - can't use surrogate expressions because it was
            // retrofitted for constant folding - this needs to be fixed.
            // Needs to occur before ReplaceAggregateAggExpressionWithEval, which will update the functions, losing the filter.
            new SubstituteFilteredExpression(),
            new RemoveStatsOverride(),
            // first extract nested expressions inside aggs
            new ReplaceAggregateNestedExpressionWithEval(),
            // then extract nested aggs top-level
            new ReplaceAggregateAggExpressionWithEval(),
            // lastly replace surrogate functions
            new SubstituteSurrogateAggregations(),
            // translate metric aggregates after surrogate substitution and replace nested expressions with eval (again)
            new TranslateTimeSeriesAggregate(),
            new PruneUnusedIndexMode(),
            // after translating metric aggregates, we need to replace surrogate substitutions and nested expressions again.
            new SubstituteSurrogateAggregations(),
            new ReplaceAggregateNestedExpressionWithEval(),
            // this one needs to be placed before ReplaceAliasingEvalWithProject, so that any potential aliasing eval (eval x = y)
            // is not replaced with a Project before the eval to be copied on the left hand side of an InlineJoin
            new PropagateInlineEvals(),
            new ReplaceRegexMatch(),
            new ReplaceTrivialTypeConversions(),
            new ReplaceAliasingEvalWithProject(),
            new SkipQueryOnEmptyMappings(),
            new SubstituteSurrogateExpressions(),
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
            new CombineBinaryComparisons(),
            new CombineDisjunctions(),
            // TODO: bifunction can now (since we now have just one data types set) be pushed into the rule
            new SimplifyComparisonsArithmetics(DataType::areCompatible),
            new ReplaceStringCasingWithInsensitiveEquals(),
            new ReplaceStatsFilteredAggWithEval(),
            new ExtractAggregateCommonFilter(),
            // prune/elimination
            new PruneFilters(),
            new PruneColumns(),
            new PruneLiteralsInOrderBy(),
            new PushLimitToKnn(),
            new PushDownAndCombineLimits(),
            new PushDownAndCombineFilters(),
            new PushDownAndCombineSample(),
            new PushDownCompletion(),
            new PushDownEval(),
            new PushDownRegexExtract(),
            new PushDownEnrich(),
            new PushDownAndCombineOrderBy(),
            new PruneRedundantOrderBy(),
            new PruneRedundantSortClauses(),
            new PruneLeftJoinOnNullMatchingField()
        );
    }

    protected static Batch<LogicalPlan> cleanup() {
        return new Batch<>("Clean Up", new ReplaceLimitAndSortAsTopN(), new ReplaceRowAsLocalRelation(), new PropgateUnmappedFields());
    }
}
