/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.Streaming;

import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Locate any LIMIT that is "visible" under remote ENRICH, and make a copy of it above the ENRICH. The original limit is marked as local.
 * This allows the correct semantics of the remote application of limit. Enrich itself does not change the cardinality,
 * but the limit needs to be taken twice, locally on the node and again on the coordinator.
 * This runs only on LogicalPlanOptimizer not on local one.
 */
public final class HoistRemoteEnrichLimit extends OptimizerRules.ParameterizedOptimizerRule<Enrich, LogicalOptimizerContext>
    implements
        OptimizerRules.CoordinatorOnly {

    public HoistRemoteEnrichLimit() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Enrich en, LogicalOptimizerContext ctx) {
        if (en.mode() == Enrich.Mode.REMOTE) {
            // Since limits are combinable, we will just assemble the set of candidates and create one combined lowest limit above the
            // Enrich.
            Set<Limit> seenLimits = Collections.newSetFromMap(new IdentityHashMap<>());
            en.child().forEachDownMayReturnEarly((p, stop) -> {
                if (p instanceof Limit l && l.local() == false) {
                    // Local limits can be ignored here as they are always duplicates that have another limit upstairs
                    seenLimits.add(l);
                    return;
                }
                if ((p instanceof Streaming) == false // can change the number of rows, so we can't just pull a limit from
                                                      // under it
                    // this will fail the verifier anyway, so no need to continue
                    || (p instanceof ExecutesOn ex && ex.executesOn() == ExecutesOn.ExecuteLocation.COORDINATOR)
                // This is essentially another remote enrich - let it take care of its own limits
                    || (p instanceof Enrich e && e.mode() == Enrich.Mode.REMOTE)
                // If it's a pipeline breaker, this part will be on the coordinator anyway, which likely will fail the verifier
                // In any case, duplicating anything below there is pointless.
                    || p instanceof PipelineBreaker) {
                    stop.set(true);
                }
            });

            if (seenLimits.isEmpty()) {
                return en;
            }
            // Mark original limits as local
            LogicalPlan transformLimits = en.transformDown(Limit.class, l -> seenLimits.contains(l) ? l.withLocal(true) : l);
            // Shouldn't actually throw because we checked seenLimits is not empty
            Limit lowestLimit = seenLimits.stream().min(Comparator.comparing(l -> (int) l.limit().fold(ctx.foldCtx()))).orElseThrow();
            // Insert new lowest limit on top of the Enrich, and mark it as duplicated since we don't want it to be pushed down
            return new Limit(lowestLimit.source(), lowestLimit.limit(), transformLimits, true, false);
        }
        return en;
    }
}
