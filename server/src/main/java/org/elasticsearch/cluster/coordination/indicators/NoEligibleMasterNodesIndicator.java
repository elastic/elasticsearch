/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.indicators;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;

public class NoEligibleMasterNodesIndicator implements HealthIndicatorService {

    public static final String NO_ELIGIBLE_MASTERS = "no_eligible_masters";
    public static final String CLUSTER_COORDINATION = "cluster_coordination";

    private final ClusterState clusterState;

    public NoEligibleMasterNodesIndicator(ClusterState clusterState) {
        this.clusterState = clusterState;
    }

    @Override
    public String name() {
        return NO_ELIGIBLE_MASTERS;
    }

    @Override
    public String component() {
        return CLUSTER_COORDINATION;
    }

    @Override
    public HealthIndicatorResult calculate() {
        for (DiscoveryNode node : clusterState.nodes()) {
            if (node.getRoles().contains(DiscoveryNodeRole.MASTER_ROLE)) {
                return HealthIndicatorResult.of(
                    NO_ELIGIBLE_MASTERS,
                    CLUSTER_COORDINATION,
                    HealthStatus.GREEN,
                    "There are eligible master nodes."
                );
            }
        }
        return HealthIndicatorResult.of(NO_ELIGIBLE_MASTERS, CLUSTER_COORDINATION, HealthStatus.RED, "No eligible master nodes.");
    }
}
