/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.nodesinfo;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;

public class ComponentVersionsNodesInfoIT extends ESIntegTestCase {

    public void testNodesInfoComponentVersions() {
        final String node_1 = internalCluster().startNode();

        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForGreenStatus()
            .setWaitForNodes("1")
            .get();
        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());

        String server1NodeId = getNodeId(node_1);
        logger.info("--> started nodes: {}", server1NodeId);

        NodesInfoResponse response = clusterAdmin().prepareNodesInfo().get();
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertThat(
            response.getNodesMap().get(server1NodeId).getComponentVersions().keySet(),
            containsInAnyOrder("transform_config_version", "ml_config_version", "api_key_version")
        );
    }
}
