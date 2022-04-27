/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Collections;

public abstract class HealthIndicatorServiceBase implements HealthIndicatorService {

    static final String COULD_NOT_DETERMINE_HEALTH = "Health could not be determined";

    private final ClusterService clusterService;

    public HealthIndicatorServiceBase(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public HealthIndicatorResult calculate(boolean calculateDetails) {
        ClusterState clusterState = clusterService.state();
        if (stateIsKnown(clusterState)) {
            return doCalculate(clusterState, calculateDetails);
        } else {
            return doCalculateUnknown(clusterState, calculateDetails);
        }
    }

    protected abstract HealthIndicatorResult doCalculate(ClusterState clusterState, boolean calculateDetails);

    protected HealthIndicatorResult doCalculateUnknown(ClusterState clusterState, boolean calculateDetails) {
        return createIndicator(
            HealthStatus.UNKNOWN,
            COULD_NOT_DETERMINE_HEALTH,
            calculateDetails ? unknownHealthDetails(clusterState) : HealthIndicatorDetails.EMPTY,
            Collections.emptyList()
        );
    }

    private HealthIndicatorDetails unknownHealthDetails(ClusterState clusterState) {
        boolean hasMaster = clusterState.getBlocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID) == false;
        boolean clusterStateRecovered = clusterState.getBlocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false;
        return (XContentBuilder builder, ToXContent.Params params) -> {
            builder.startObject();
            builder.object("reason", xContentBuilder -> {
                builder.field("has_master", hasMaster);
                builder.field("cluster_state_recovered", clusterStateRecovered);
            });
            return builder.endObject();
        };
    }

    private static boolean stateIsKnown(ClusterState clusterState) {
        return clusterState.getBlocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false
            && clusterState.getBlocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID) == false;
    }
}
