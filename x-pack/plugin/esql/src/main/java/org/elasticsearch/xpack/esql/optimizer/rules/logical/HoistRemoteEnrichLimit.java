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
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

/**
 * Locate any LIMIT that is "visible" under remote ENRICH, and make a copy of it above the ENRICH.
 * This allows the correct semantics of the remote application of limit. Enrich itself does not change the cardinality,
 * but the limit needs to be taken twice, locally on the node and again on the coordinator.
 */
public final class HoistRemoteEnrichLimit extends OptimizerRules.ParameterizedOptimizerRule<Enrich, LogicalOptimizerContext> {
    // Local plans don't really need the duplication
    private final boolean local;

    public HoistRemoteEnrichLimit(boolean local) {
        super(OptimizerRules.TransformDirection.UP);
        this.local = local;
    }

    @Override
    protected LogicalPlan rule(Enrich en, LogicalOptimizerContext ctx) {
        if (en.mode() == Enrich.Mode.REMOTE) {
            Set<Limit> seenLimits = new HashSet<>();
            en.child().forEachDownMayReturnEarly((p, stop) -> {
                if (p instanceof Limit l && l.isLocal() == false) {
                    seenLimits.add(l);
                    return;
                }
                // if we hit a plan that can generate more rows, or a pipeline breaker, we cannot look further down
                if (p instanceof MvExpand || (p instanceof Join join && join.config().type() == JoinTypes.LEFT)
                // this will fail the verifier anyway, so no need to continue
                    || (p instanceof ExecutesOn ex && ex.executesOn() == ExecutesOn.ExecuteLocation.COORDINATOR)
                    || p instanceof Enrich
                    || p instanceof PipelineBreaker) {
                    stop.set(true);
                }
            });

            if (seenLimits.isEmpty()) {
                return en;
            }
            // Mark original limits as local
            LogicalPlan transformLimits = en.transformDown(Limit.class, l -> seenLimits.contains(l) ? l.withLocal(true) : l);
            if (local) {
                // For local plan, we just mark the limits so that the verifier is content
                // TODO: alternatively, we could just not make the Enrich verifier know where the plan is running
                return transformLimits;
            }
            // Shouldn't actually throw because we checked seenLimits is not empty
            Limit lowestLimit = seenLimits.stream().min(Comparator.comparing(l -> (int) l.limit().fold(ctx.foldCtx()))).orElseThrow();
            return new Limit(en.source(), lowestLimit.limit(), transformLimits, true, false);
        }
        return en;
    }
}
