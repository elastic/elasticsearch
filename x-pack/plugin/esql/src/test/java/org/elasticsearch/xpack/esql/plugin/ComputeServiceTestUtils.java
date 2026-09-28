/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchBoundaryExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * Test access to reduction planning that supplies deterministic remote-fetch runtime identity only when a boundary requires it.
 */
public final class ComputeServiceTestUtils {
    private ComputeServiceTestUtils() {}

    /**
     * Plans reduction through the same entry point used by data-node compute while keeping ordinary golden plans free of runtime state.
     */
    public static ReductionPlan reductionPlan(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        ExchangeSinkExec originalPlan,
        boolean runNodeLevelReduction,
        boolean reduceNodeLateMaterialization,
        PlanTimeProfile planTimeProfile
    ) {
        if (originalPlan.child() instanceof RemoteFetchBoundaryExec) {
            return ComputeService.reductionPlan(
                plannerSettings,
                flags,
                configuration,
                foldCtx,
                originalPlan,
                runNodeLevelReduction,
                reduceNodeLateMaterialization,
                "golden-node",
                "golden-session",
                planTimeProfile
            );
        }
        return ComputeService.reductionPlan(
            plannerSettings,
            flags,
            configuration,
            foldCtx,
            originalPlan,
            runNodeLevelReduction,
            reduceNodeLateMaterialization,
            planTimeProfile
        );
    }
}
