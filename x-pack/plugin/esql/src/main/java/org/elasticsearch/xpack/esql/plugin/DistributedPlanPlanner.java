/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Map;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

/**
 * Completes planning that depends on the coordinator/data-node split and the current cluster topology.
 */
final class DistributedPlanPlanner {

    record DistributedPlan(
        PhysicalPlan coordinatorPlan,
        @Nullable PhysicalPlan dataNodePlan,
        boolean hasConcreteIndices,
        boolean retainSearchContexts
    ) {}

    static DistributedPlan plan(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        PhysicalPlan resolvedPlan,
        Map<String, OriginalIndices> clusterToConcreteIndices,
        TransportVersion minimumTransportVersion
    ) {
        var splitPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(resolvedPlan, configuration);
        PhysicalPlan coordinatorPlan = splitPlan.v1();
        PhysicalPlan dataNodePlan = splitPlan.v2();
        boolean hasConcreteIndices = clusterToConcreteIndices.values().stream().anyMatch(indices -> indices.indices().length > 0);

        boolean retainSearchContexts = false;
        if (configuration.pragmas().remoteFetchTopN()
            && hasConcreteIndices
            && clusterToConcreteIndices.size() == 1
            && clusterToConcreteIndices.containsKey(LOCAL_CLUSTER_GROUP_KEY)
            && minimumTransportVersion.supports(DataNodeRequest.ESQL_REMOTE_FETCH_TOPN_REDUCTION)
            && dataNodePlan instanceof ExchangeSinkExec exchangeSink) {
            var remoteFetchPlan = RemoteFetchReductionPlanner.planCoordinatorTopN(
                stats -> new LocalPhysicalOptimizerContext(plannerSettings, flags, configuration, foldContext, stats),
                exchangeSink,
                coordinatorPlan
            );
            if (remoteFetchPlan.isPresent()) {
                var rewrittenPlan = remoteFetchPlan.get();
                coordinatorPlan = rewrittenPlan.coordinatorPlan();
                dataNodePlan = rewrittenPlan.dataNodePlan();
                // The rewrite is the only source of remote-fetch handles, so it alone decides whether contexts are retained.
                retainSearchContexts = true;
            }
        }

        return new DistributedPlan(coordinatorPlan, dataNodePlan, hasConcreteIndices, retainSearchContexts);
    }

    private DistributedPlanPlanner() {}
}
