/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.datasources.spi.SatelliteFragment;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

/**
 * Pushes a LIMIT into {@link ExternalSourceExec} so the source can stop reading early.
 * The {@link LimitExec} is kept as a safety net; only the hint is propagated.
 */
public class PushLimitToExternalSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<LimitExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(LimitExec limitExec, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan child = limitExec.child();
        if (child instanceof ExternalSourceExec ext) {
            // The satellite enforces the limit inside its fragment. Handing it to the planner as well would size the drivers as if the
            // first rows read were enough, while a filter in the fragment may need every split to find them.
            if (ext.pushedFilter() instanceof SatelliteFragment fragment && fragment.filtersRows()) {
                return limitExec;
            }
            int limit = (int) limitExec.limit().fold(ctx.foldCtx());
            return limitExec.replaceChild(ext.withPushedLimit(limit));
        }
        return limitExec;
    }
}
