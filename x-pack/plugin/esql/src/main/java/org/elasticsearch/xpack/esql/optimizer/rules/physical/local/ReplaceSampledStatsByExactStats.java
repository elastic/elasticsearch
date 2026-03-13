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
 */
public class ReplaceSampledStatsByExactStats extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    SampledAggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(SampledAggregateExec plan, LocalPhysicalOptimizerContext context) {
        if (plan.getMode() == AggregatorMode.INITIAL
            && plan.child() instanceof EvalExec eval
            && eval.expressions().size() == 1
            && eval.expressions().getFirst() instanceof Alias alias
            && alias.name().equals(ApproximationPlan.BUCKET_ID_COLUMN_NAME)
            && eval.child() instanceof EsQueryExec queryExec) {

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

            // The first intermediate attributes of the SampledAggregate are the original aggregations.
            // Next follow the bucket aggregations. Each bucket has the same intermediate attributes
            // as the original aggregate.
            List<Alias> exactBuckets = new ArrayList<>();
            for (int i = plan.originalIntermediateAttributes().size(); i < plan.intermediateAttributes().size(); i++) {
                Attribute attribute = plan.intermediateAttributes().get(i);
                Attribute originalAttribute = plan.originalIntermediateAttributes().get(i % plan.originalIntermediateAttributes().size());
                exactBuckets.add(new Alias(Source.EMPTY, attribute.name(), originalAttribute, attribute.id()));
            }
            return new EvalExec(Source.EMPTY, aggregate, exactBuckets);
        } else {
            return plan;
        }
    }
}
