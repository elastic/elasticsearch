/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
import org.elasticsearch.xpack.esql.plan.physical.SampledAggregateExec;

public class ReplaceSampledStats extends PhysicalOptimizerRules.OptimizerRule<SampledAggregateExec> {

    @Override
    protected PhysicalPlan rule(SampledAggregateExec plan) {
        // TODO: push sample to the source command
        return new AggregateExec(
            plan.source(),
            new SampleExec(plan.source(), plan.child(), plan.sampleProbability()),
            plan.groupings(),
            plan.aggregates(),
            plan.getMode(),
            plan.intermediateAttributes(),
            plan.estimatedRowSize()
        );
    }
}
