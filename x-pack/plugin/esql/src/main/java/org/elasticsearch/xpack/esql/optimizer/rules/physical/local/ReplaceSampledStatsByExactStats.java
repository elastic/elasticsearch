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
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampledAggregateExec;

import java.util.List;

/**
 * If the original aggregate wrapped by the sampled aggregate can be
 * pushed down to Lucene (so that it will execute exact and fast), sampling
 * should be skipped and the original aggregate should be executed.
 * <p>
 * In that case, this rule replaces the sampled aggregate by a regular
 * aggregate and nullifies all buckets. The plan:
 * <pre>
 * {@code FROM data | EVAL bucket_id=... | SAMPLED_STATS original_aggs, bucket_aggs}
 * </pre>
 * is transformed into:
 * <pre>
 * {@code FROM data | EVAL bucket_id=... | STATS original_aggs | EVAL bucket_aggs=NULL}
 * </pre>
 * All buckets being NULL indicates to the coordinator that the stats are exact.
 * <p>
 * The aggregate created by this rule is pushed down to Lucene by the
 * {@link PushStatsToSource} rule, whose logic is reused here.
 */
public class ReplaceSampledStatsByExactStats extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    SampledAggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(SampledAggregateExec sampledAggregateExec, LocalPhysicalOptimizerContext context) {
        if (sampledAggregateExec.getMode() == AggregatorMode.INITIAL
            && sampledAggregateExec.child() instanceof EvalExec evalExec
            && evalExec.expressions().size() == 1
            && evalExec.expressions().getFirst() instanceof Alias alias
            && alias.name().equals(ApproximationPlan.BUCKET_ID_COLUMN_NAME)
            && evalExec.child() instanceof EsQueryExec queryExec) {

            var tuple = PushStatsToSource.pushableStats(
                sampledAggregateExec.groupings(),
                sampledAggregateExec.originalAggregates(),
                context
            );

            // for the moment support pushing count just for one field
            List<EsStatsQueryExec.Stat> stats = tuple.v2();
            if (stats.size() != 1 || stats.size() != sampledAggregateExec.originalAggregates().size()) {
                return sampledAggregateExec;
            }

            List<Alias> nullBuckets = sampledAggregateExec.outputSet()
                .subtract(AttributeSet.of(sampledAggregateExec.originalIntermediateAttributes()))
                .stream()
                .map(attr -> new Alias(Source.EMPTY, attr.name(), Literal.NULL, attr.id()))
                .toList();

            PhysicalPlan plan = new AggregateExec(
                sampledAggregateExec.source(),
                queryExec,
                sampledAggregateExec.groupings(),
                sampledAggregateExec.originalAggregates(),
                sampledAggregateExec.getMode(),
                sampledAggregateExec.originalIntermediateAttributes(),
                sampledAggregateExec.estimatedRowSize()
            );

            return new EvalExec(Source.EMPTY, plan, nullBuckets);
        } else {
            return sampledAggregateExec;
        }
    }
}
