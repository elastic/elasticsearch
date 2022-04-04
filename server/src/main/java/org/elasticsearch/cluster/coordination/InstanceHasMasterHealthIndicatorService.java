/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;

import java.util.Collections;

import static org.elasticsearch.health.ServerHealthComponents.CLUSTER_COORDINATION;

public class InstanceHasMasterHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "instance_has_master";

    private static final String INSTANCE_HAS_MASTER_GREEN_SUMMARY = "Health coordinating instance has a master node.";
    private static final String INSTANCE_HAS_MASTER_RED_SUMMARY = "Health coordinating instance does not have a master node.";

    private final ClusterService clusterService;

    public InstanceHasMasterHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String component() {
        return CLUSTER_COORDINATION;
    }

    @Override
    public HealthIndicatorResult calculate() {

        DiscoveryNode coordinatingNode = clusterService.localNode();
        ClusterState clusterState = clusterService.state();
        DiscoveryNodes nodes = clusterState.nodes();
        DiscoveryNode masterNode = nodes.getMasterNode();

        HealthStatus instanceHasMasterStatus = masterNode == null ? HealthStatus.RED : HealthStatus.GREEN;
        String instanceHasMasterSummary = masterNode == null ? INSTANCE_HAS_MASTER_RED_SUMMARY : INSTANCE_HAS_MASTER_GREEN_SUMMARY;

        return createIndicator(instanceHasMasterStatus, instanceHasMasterSummary, (builder, params) -> {
            builder.startObject();
            builder.object("coordinating_node", xContentBuilder -> {
                builder.field("node_id", coordinatingNode.getId());
                builder.field("name", coordinatingNode.getName());
            });
            builder.object("master_node", xContentBuilder -> {
                if (masterNode != null) {
                    builder.field("node_id", masterNode.getId());
                    builder.field("name", masterNode.getName());
                } else {
                    builder.nullField("node_id");
                    builder.nullField("name");
                }
            });
            return builder.endObject();
        }, Collections.emptyList());
    }
}
