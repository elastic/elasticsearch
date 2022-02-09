/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.indicators;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;

import java.util.stream.Collectors;

import static org.elasticsearch.health.ServerHealthComponents.CLUSTER_COORDINATION;

public class CanFormQuorumIndicator implements HealthIndicatorService {

    public static final String CAN_FORM_QUORUM = "can_form_quorum";

    private final ClusterService clusterService;

    public CanFormQuorumIndicator(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return CAN_FORM_QUORUM;
    }

    @Override
    public String component() {
        return CLUSTER_COORDINATION;
    }

    @Override
    public HealthIndicatorResult calculate() {
        var clusterState = clusterService.state();
        boolean hasQuorum = clusterState.getLastCommittedConfiguration()
            .hasQuorum(clusterState.getNodes().getMasterNodes().values().stream().map(DiscoveryNode::getId).collect(Collectors.toSet()));
        return hasQuorum
            ? HealthIndicatorResult.of(CAN_FORM_QUORUM, CLUSTER_COORDINATION, HealthStatus.GREEN, "Instances can form a quorum.")
            : HealthIndicatorResult.of(
                CAN_FORM_QUORUM,
                CLUSTER_COORDINATION,
                HealthStatus.RED,
                "Not enough master-eligible instances to form a quorum."
            );
    }
}
