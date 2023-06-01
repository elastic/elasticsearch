/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class HealthInfoCacheTests extends ESTestCase {

    private static final DiskHealthInfo GREEN = new DiskHealthInfo(HealthStatus.GREEN, null);
    private static final DiskHealthInfo RED = new DiskHealthInfo(
        HealthStatus.RED,
        DiskHealthInfo.Cause.FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD
    );
    private final ClusterService clusterService = mock(ClusterService.class);
    private final DiscoveryNode node1 = DiscoveryNodeUtils.builder("node_1")
        .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
        .build();
    private final DiscoveryNode node2 = DiscoveryNodeUtils.builder("node_2")
        .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
        .build();
    private final DiscoveryNode[] allNodes = new DiscoveryNode[] { node1, node2 };

    public void testAddHealthInfo() {
        HealthInfoCache healthInfoCache = HealthInfoCache.create(clusterService);
        healthInfoCache.updateNodeHealth(node1.getId(), GREEN);
        healthInfoCache.updateNodeHealth(node2.getId(), RED);

        Map<String, DiskHealthInfo> diskHealthInfo = healthInfoCache.getHealthInfo().diskInfoByNode();
        healthInfoCache.updateNodeHealth(node1.getId(), RED);

        assertThat(diskHealthInfo.get(node1.getId()), equalTo(GREEN));
        assertThat(diskHealthInfo.get(node2.getId()), equalTo(RED));
    }

    public void testRemoveNodeFromTheCluster() {
        HealthInfoCache healthInfoCache = HealthInfoCache.create(clusterService);
        healthInfoCache.updateNodeHealth(node1.getId(), GREEN);
        healthInfoCache.updateNodeHealth(node2.getId(), RED);

        ClusterState previous = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        ClusterState current = ClusterStateCreationUtils.state(node1, node1, node1, new DiscoveryNode[] { node1 });
        healthInfoCache.clusterChanged(new ClusterChangedEvent("test", current, previous));

        Map<String, DiskHealthInfo> diskHealthInfo = healthInfoCache.getHealthInfo().diskInfoByNode();
        assertThat(diskHealthInfo.get(node1.getId()), equalTo(GREEN));
        assertThat(diskHealthInfo.get(node2.getId()), nullValue());
    }

    public void testNotAHealthNode() {
        HealthInfoCache healthInfoCache = HealthInfoCache.create(clusterService);
        healthInfoCache.updateNodeHealth(node1.getId(), GREEN);
        healthInfoCache.updateNodeHealth(node2.getId(), RED);

        ClusterState previous = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        ClusterState current = ClusterStateCreationUtils.state(node1, node1, node2, allNodes);
        healthInfoCache.clusterChanged(new ClusterChangedEvent("test", current, previous));

        Map<String, DiskHealthInfo> diskHealthInfo = healthInfoCache.getHealthInfo().diskInfoByNode();
        assertThat(diskHealthInfo.isEmpty(), equalTo(true));
    }
}
