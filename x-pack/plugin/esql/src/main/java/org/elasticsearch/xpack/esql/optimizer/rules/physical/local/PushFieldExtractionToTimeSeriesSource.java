/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesSourceExec;

/**
 * A rule that moves field extractions to occur before the time-series aggregation in the time-series source plan.
 */
public class PushFieldExtractionToTimeSeriesSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    EsQueryExec,
    LocalPhysicalOptimizerContext> {

    public PushFieldExtractionToTimeSeriesSource() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    public PhysicalPlan rule(EsQueryExec plan, LocalPhysicalOptimizerContext context) {
        if (plan.indexMode() == IndexMode.TIME_SERIES) {
            return new TimeSeriesSourceExec(plan.source(), plan.output(), plan.query(), plan.limit(), plan.estimatedRowSize());
        } else {
            return plan;
        }
    }
}
