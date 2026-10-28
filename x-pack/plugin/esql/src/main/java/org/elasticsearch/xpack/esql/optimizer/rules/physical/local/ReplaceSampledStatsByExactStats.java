/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.approximation.ApproximationPlan;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampledAggregateExec;

import java.util.ArrayList;
import java.util.List;

/**
 * If the original aggregate wrapped by the sampled aggregate can be
 * pushed down to Lucene (so that it will execute exact and fast), sampling
 * should be skipped and the original aggregate should be executed.
 * <p>
 * In that case, this rule replaces the sampled aggregate by a regular
 * aggregate and replicates the exact intermediate values to all bucket
 * intermediates. The plan:
 * <pre>
 * {@code FROM data | EVAL bucket_id=... | SAMPLED_STATS original_aggs, bucket_aggs}
 * </pre>
 * is (loosely) transformed into:
 * <pre>
 * {@code FROM data | ES_STATS_QUERY original_aggs | EVAL bucket_aggs=original_aggs}
 * </pre>
 * Replicating the exact value to all buckets makes exact data appear as
 * zero-variance sampled data, so confidence intervals remain correct in
 * mixed exact/sampled scenarios (where some nodes push down exact stats and
 * others use sampling).
 * <p>
 * Two shapes are handled:
 * <ul>
 * <li>An ungrouped {@code COUNT} that {@link PushStatsToSource} can push down
 *     to a {@code LuceneCountOperator}.</li>
 * <li>A grouped {@code COUNT(*) BY BUCKET(date, ...)} whose grouping has been
 *     rewritten to a {@code RoundTo} and can be pushed down as query-and-tags
 *     (see {@link ReplaceRoundToWithQueryAndTags} and
 *     {@link PushCountQueryAndTagsToSource}). Here the sampled aggregate is
 *     turned back into a regular aggregate on top of the {@code RoundTo} eval,
 *     so that the later query-and-tags push down rewrites it into an exact
 *     {@code EsStatsQueryExec}.</li>
 * </ul>
 */
public class ReplaceSampledStatsByExactStats extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    SampledAggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(SampledAggregateExec plan, LocalPhysicalOptimizerContext context) {
        // Make sure that the plan is a SampledAggregate, preceded by an Eval that produces the bucket ID,
        // preceded by EsQueryExec.
        if (plan.getMode() == AggregatorMode.INITIAL
            && plan.child() instanceof EvalExec eval
            && eval.fields().stream().anyMatch(alias -> alias.name().equals(ApproximationPlan.BUCKET_ID_COLUMN_NAME))
            && eval.child() instanceof EsQueryExec queryExec) {

            // COUNT with any grouping, that can be pushed down by PushStatsToSource.
            if (plan.groupings().isEmpty() && eval.fields().size() == 1  // the bucket ID (checked above)
            ) {
                var tuple = PushStatsToSource.pushableStats(plan.groupings(), plan.originalAggregates(), context);

                // for the moment support pushing count just for one field
                List<EsStatsQueryExec.Stat> stats = tuple.v2();
                if (stats.size() != 1 || stats.size() != plan.originalAggregates().size()) {
                    return plan;
                }

                AggregateExec aggregate = new AggregateExec(
                    plan.source(),
                    queryExec,
                    plan.groupings(),
                    plan.originalAggregates(),
                    plan.getMode(),
                    plan.originalIntermediateAttributes(),
                    plan.estimatedRowSize()
                );
                return replicateExactBuckets(plan, aggregate);
            }

            // Grouped COUNT(*) BY BUCKET(date, ...) that can be pushed down as query-and-tags.
            // The grouping has already been rewritten to a RoundTo by ReplaceDateTruncBucketWithRoundTo,
            // and the Eval below the SampledAggregate should hold that RoundTo and the bucket ID.
            // Reuses logic from ReplaceRoundToWithQueryAndTags and PushCountQueryAndTagsToSource.
            RoundTo roundTo;
            List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags;
            if (plan.groupings().size() == 1
                && eval.fields().size() == 2  // the bucket ID (checked above) and the RoundTo (checked below)
                && queryExec.canSubstituteRoundToWithQueryBuilderAndTags()
                && PushCountQueryAndTagsToSource.isPushableGroupedCount(plan.originalAggregates(), plan.groupings())
                && (roundTo = ReplaceRoundToWithQueryAndTags.pushableRoundTo(eval, queryExec, context)) != null
                && (queryBuilderAndTags = ReplaceRoundToWithQueryAndTags.queryBuilderAndTags(roundTo, queryExec, context)) != null
                && PushCountQueryAndTagsToSource.pushableCountQueries(queryBuilderAndTags) != null) {

                // Rebuild the eval keeping only the RoundTo grouping; the random bucket ID is no longer needed without sampling.
                Alias roundToAlias = eval.fields().stream().filter(a -> a.child() == roundTo).findFirst().orElseThrow();
                EvalExec groupingEval = new EvalExec(eval.source(), queryExec, List.of(roundToAlias));

                AggregateExec aggregate = new AggregateExec(
                    plan.source(),
                    groupingEval,
                    plan.groupings(),
                    plan.originalAggregates(),
                    plan.getMode(),
                    plan.originalIntermediateAttributes(),
                    plan.estimatedRowSize()
                );
                return replicateExactBuckets(plan, aggregate);
            }
        }

        return plan;
    }

    /**
     * Wraps the exact aggregate in an {@code EvalExec} that replicates the exact aggregate's intermediate state to every bucket's
     * intermediate state. This makes the exact result appear as zero-variance sampled data, so confidence intervals come out as the exact
     * value with a certified, zero-width interval.
     */
    private static PhysicalPlan replicateExactBuckets(SampledAggregateExec plan, AggregateExec aggregate) {
        int groupingCount = plan.groupings().size();
        int aggregateStateCount = plan.originalIntermediateAttributes().size() - groupingCount;

        // The first intermediate attributes of the SampledAggregate are the grouping attributes.
        // Next follow intermediate attributes of the SampledAggregate's original aggregations.
        // Next follow the bucket aggregations. Each bucket has the same intermediate attributes
        // as the original aggregate.
        List<Alias> exactBuckets = new ArrayList<>();
        for (int i = plan.originalIntermediateAttributes().size(); i < plan.intermediateAttributes().size(); i++) {
            Attribute attribute = plan.intermediateAttributes().get(i);
            int stateIndex = (i - plan.originalIntermediateAttributes().size()) % aggregateStateCount;
            Attribute originalAttribute = plan.originalIntermediateAttributes().get(groupingCount + stateIndex);
            exactBuckets.add(new Alias(Source.EMPTY, attribute.name(), originalAttribute, attribute.id(), attribute.synthetic()));
        }
        return new EvalExec(Source.EMPTY, aggregate, exactBuckets);
    }
}
