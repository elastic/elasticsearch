/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.LeafExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
import org.elasticsearch.xpack.esql.plan.physical.SampledAggregateExec;

/**
 * If the original aggregate wrapped by the sampled aggregate cannot be
 * pushed down to Lucene (which would execute exact and fast), sampling
 * should be used to speed up the aggregation.
 * <p>
 * In that case, this rule replaces the sampled aggregate by a regular
 * aggregate on top of a sample. The plan:
 * <pre>
 * {@code FROM data | commands | SAMPLED_STATS[prob] aggs}
 * </pre>
 * is transformed into:
 * <pre>
 * {@code FROM data | SAMPLE prob | commands | STATS aggs}
 * </pre>
 */
public class ReplaceSampledStatsBySampleAndStats extends PhysicalOptimizerRules.OptimizerRule<SampledAggregateExec> {

    @Override
    protected PhysicalPlan rule(SampledAggregateExec plan) {
        double sampleProbability = (double) Foldables.literalValueOf(plan.sampleProbability());
        return new AggregateExec(
            plan.source(),
            sampleProbability == 1.0
                ? plan.child()
                : plan.child().transformUp(LeafExec.class, leaf -> new SampleExec(Source.EMPTY, leaf, plan.sampleProbability())),
            plan.groupings(),
            plan.aggregates(),
            plan.getMode(),
            plan.intermediateAttributes(),
            plan.estimatedRowSize()
        );
    }
}
