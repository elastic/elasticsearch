/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.components.controller;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.health.HealthComponentResult;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;

import java.util.Collections;

public final class ClusterCoordination {

    public static final String NAME = "cluster_coordination";

    public static final String INSTANCE_HAS_MASTER_NAME = "instance_has_master";
    public static final String INSTANCE_HAS_MASTER_GREEN_SUMMARY = "Health coordinating instance has a master node.";
    public static final String INSTANCE_HAS_MASTER_RED_SUMMARY = "Health coordinating instance does not have a master node.";

    private ClusterCoordination() {}

    public static HealthComponentResult createClusterCoordinationComponent(
        final DiscoveryNode coordinatingNode,
        final ClusterState clusterState
    ) {
        final DiscoveryNodes nodes = clusterState.nodes();
        final DiscoveryNode masterNode = nodes.getMasterNode();

        HealthStatus instanceHasMasterStatus = masterNode == null ? HealthStatus.RED : HealthStatus.GREEN;
        String instanceHasMasterSummary = masterNode == null ? INSTANCE_HAS_MASTER_RED_SUMMARY : INSTANCE_HAS_MASTER_GREEN_SUMMARY;
        HealthIndicatorResult instanceHasMaster = new HealthIndicatorResult(
            INSTANCE_HAS_MASTER_NAME,
            NAME,
            instanceHasMasterStatus,
            instanceHasMasterSummary,
            (builder, params) -> {
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
            }
        );

        // Only a single indicator currently so it determines the status
        final HealthStatus status = instanceHasMaster.status();
        return new HealthComponentResult(NAME, status, Collections.singletonMap(INSTANCE_HAS_MASTER_NAME, instanceHasMaster));
    }
}
