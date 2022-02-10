/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.indicators;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;

import static org.elasticsearch.health.ServerHealthComponents.CLUSTER_COORDINATION;

public class HasEligibleMasterNodeIndicator implements HealthIndicatorService {

    public static final String HAS_ELIGIBLE_MASTER = "has_eligible_master";

    private final ClusterService clusterService;

    public HasEligibleMasterNodeIndicator(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return HAS_ELIGIBLE_MASTER;
    }

    @Override
    public String component() {
        return CLUSTER_COORDINATION;
    }

    @Override
    public HealthIndicatorResult calculate() {
        for (DiscoveryNode node : clusterService.state().nodes()) {
            if (node.getRoles().contains(DiscoveryNodeRole.MASTER_ROLE)) {
                return HealthIndicatorResult.of(
                    HAS_ELIGIBLE_MASTER,
                    CLUSTER_COORDINATION,
                    HealthStatus.GREEN,
                    "There is a master-eligible node."
                );
            }
        }
        return HealthIndicatorResult.of(HAS_ELIGIBLE_MASTER, CLUSTER_COORDINATION, HealthStatus.RED, "No master-eligible nodes.");
    }
}
