/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.health.node.HealthInfoTests.randomDslHealthInfo;
import static org.elasticsearch.health.node.HealthInfoTests.randomRepoHealthInfo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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
        DataStreamLifecycleHealthInfo latestDslHealthInfo = randomDslHealthInfo();
        var repoHealthInfo = randomRepoHealthInfo();
        healthInfoCache.updateNodeHealth(node1.getId(), GREEN, latestDslHealthInfo, repoHealthInfo, FileSettingsHealthInfo.INDETERMINATE);
        healthInfoCache.updateNodeHealth(node2.getId(), RED, null, null, FileSettingsHealthInfo.INDETERMINATE);

        Map<String, DiskHealthInfo> diskHealthInfo = healthInfoCache.getHealthInfo().diskInfoByNode();
        // Ensure that HealthInfoCache#getHealthInfo() returns a copy of the health info.
        healthInfoCache.updateNodeHealth(node1.getId(), RED, null, null, FileSettingsHealthInfo.INDETERMINATE);

        assertThat(diskHealthInfo.get(node1.getId()), equalTo(GREEN));
        assertThat(diskHealthInfo.get(node2.getId()), equalTo(RED));
        // dsl health info has not changed as a new value has not been reported
        assertThat(healthInfoCache.getHealthInfo().dslHealthInfo(), is(latestDslHealthInfo));
    }

    public void testRemoveNodeFromTheCluster() {
        HealthInfoCache healthInfoCache = HealthInfoCache.create(clusterService);
        healthInfoCache.updateNodeHealth(node1.getId(), GREEN, null, null, FileSettingsHealthInfo.INDETERMINATE);
        DataStreamLifecycleHealthInfo latestDslHealthInfo = randomDslHealthInfo();
        var repoHealthInfo = randomRepoHealthInfo();
        healthInfoCache.updateNodeHealth(node2.getId(), RED, latestDslHealthInfo, repoHealthInfo, FileSettingsHealthInfo.INDETERMINATE);

        ClusterState previous = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        ClusterState current = ClusterStateCreationUtils.state(node1, node1, node1, new DiscoveryNode[] { node1 });
        healthInfoCache.clusterChanged(new ClusterChangedEvent("test", current, previous));

        Map<String, DiskHealthInfo> diskHealthInfo = healthInfoCache.getHealthInfo().diskInfoByNode();
        assertThat(diskHealthInfo.get(node1.getId()), equalTo(GREEN));
        assertThat(diskHealthInfo.get(node2.getId()), nullValue());
        // the dsl info is not removed when the node that reported it leaves the cluster as the next DSL run will report it and
        // override it (if the health node stops being the designated health node the health cache nullifies the existing DSL info)
        assertThat(healthInfoCache.getHealthInfo().dslHealthInfo(), is(latestDslHealthInfo));
    }

    public void testNotAHealthNode() {
        HealthInfoCache healthInfoCache = HealthInfoCache.create(clusterService);
        healthInfoCache.updateNodeHealth(
            node1.getId(),
            GREEN,
            randomDslHealthInfo(),
            randomRepoHealthInfo(),
            FileSettingsHealthInfo.INDETERMINATE
        );
        healthInfoCache.updateNodeHealth(node2.getId(), RED, null, null, FileSettingsHealthInfo.INDETERMINATE);

        ClusterState previous = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        ClusterState current = ClusterStateCreationUtils.state(node1, node1, node2, allNodes);
        healthInfoCache.clusterChanged(new ClusterChangedEvent("test", current, previous));

        Map<String, DiskHealthInfo> diskHealthInfo = healthInfoCache.getHealthInfo().diskInfoByNode();
        assertThat(diskHealthInfo.isEmpty(), equalTo(true));
        assertThat(healthInfoCache.getHealthInfo().dslHealthInfo(), is(nullValue()));
        Map<String, RepositoriesHealthInfo> repoHealthInfo = healthInfoCache.getHealthInfo().repositoriesInfoByNode();
        assertThat(repoHealthInfo.isEmpty(), equalTo(true));
    }
}
