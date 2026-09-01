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
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.health.node.HealthInfoTests.randomDlmFrozenTransitionsHealthInfo;
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
        // node1 is local node, master, and health node
        ClusterState state = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        healthInfoCache.clusterChanged(new ClusterChangedEvent("test", state, state));
        DataStreamLifecycleHealthInfo latestDslHealthInfo = randomDslHealthInfo();
        var repoHealthInfo = randomRepoHealthInfo();
        DlmFrozenTransitionsHealthInfo latestDlmFrozenTransitionsHealthInfo = randomDlmFrozenTransitionsHealthInfo();
        healthInfoCache.updateNodeHealth(
            node1.getId(),
            GREEN,
            latestDslHealthInfo,
            repoHealthInfo,
            FileSettingsHealthInfo.INDETERMINATE,
            latestDlmFrozenTransitionsHealthInfo
        );
        healthInfoCache.updateNodeHealth(node2.getId(), RED, null, null, FileSettingsHealthInfo.INDETERMINATE);

        Map<String, DiskHealthInfo> diskHealthInfo = healthInfoCache.getHealthInfo().diskInfoByNode();
        // Ensure that HealthInfoCache#getHealthInfo() returns a copy of the health info.
        healthInfoCache.updateNodeHealth(node1.getId(), RED, null, null, FileSettingsHealthInfo.INDETERMINATE);

        assertThat(diskHealthInfo.get(node1.getId()), equalTo(GREEN));
        assertThat(diskHealthInfo.get(node2.getId()), equalTo(RED));
        // dsl health info has not changed as a new value has not been reported
        assertThat(healthInfoCache.getHealthInfo().dslHealthInfo(), is(latestDslHealthInfo));
        // same for the DLM frozen transitions health info
        assertThat(healthInfoCache.getHealthInfo().dlmFrozenTransitionsHealthInfo(), is(latestDlmFrozenTransitionsHealthInfo));
    }

    public void testRemoveNodeFromTheCluster() {
        HealthInfoCache healthInfoCache = HealthInfoCache.create(clusterService);
        // node1 is local node, master, and health node
        ClusterState previous = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        healthInfoCache.clusterChanged(new ClusterChangedEvent("test", previous, previous));
        DataStreamLifecycleHealthInfo latestDslHealthInfo = randomDslHealthInfo();
        var repoHealthInfo = randomRepoHealthInfo();
        // DSL health info is published by the master (node1), disk/repo health from node2
        healthInfoCache.updateNodeHealth(node1.getId(), GREEN, latestDslHealthInfo, repoHealthInfo, FileSettingsHealthInfo.INDETERMINATE);
        healthInfoCache.updateNodeHealth(node2.getId(), RED, null, null, FileSettingsHealthInfo.INDETERMINATE);

        ClusterState current = ClusterStateCreationUtils.state(node1, node1, node1, new DiscoveryNode[] { node1 });
        healthInfoCache.clusterChanged(new ClusterChangedEvent("test", current, previous));

        Map<String, DiskHealthInfo> diskHealthInfo = healthInfoCache.getHealthInfo().diskInfoByNode();
        assertThat(diskHealthInfo.get(node1.getId()), equalTo(GREEN));
        assertThat(diskHealthInfo.get(node2.getId()), nullValue());
        // the dsl info is not removed when a non-master node leaves the cluster; it is only reset when the health node changes
        assertThat(healthInfoCache.getHealthInfo().dslHealthInfo(), is(latestDslHealthInfo));
    }

    public void testFileSettingsHealthInfoOnlyAcceptedFromMaster() {
        HealthInfoCache healthInfoCache = HealthInfoCache.create(clusterService);
        // node1 is local node, master, and health node
        ClusterState state = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        healthInfoCache.clusterChanged(new ClusterChangedEvent("test", state, state));

        var failing = new FileSettingsHealthInfo(true, 1L, 1, "some error");
        var green = FileSettingsHealthInfo.INITIAL_ACTIVE.successful();

        // update from master (node1) is accepted
        healthInfoCache.updateNodeHealth(node1.getId(), GREEN, null, null, failing);
        assertThat(healthInfoCache.getHealthInfo().fileSettingsHealthInfo(), equalTo(failing));

        // update from non-master (node2) is rejected
        healthInfoCache.updateNodeHealth(node2.getId(), RED, null, null, green);
        assertThat(healthInfoCache.getHealthInfo().fileSettingsHealthInfo(), equalTo(failing));
    }

    public void testStaleFileSettingsHealthInfoRejectedAfterMasterChange() {
        HealthInfoCache healthInfoCache = HealthInfoCache.create(clusterService);
        // node1 is local/health node, node2 is master
        ClusterState withNode2AsMaster = ClusterStateCreationUtils.state(node1, node2, node1, allNodes);
        healthInfoCache.clusterChanged(new ClusterChangedEvent("test", withNode2AsMaster, withNode2AsMaster));

        var failing = new FileSettingsHealthInfo(true, 1L, 1, "some error");
        // old master (node2) publishes a failure
        healthInfoCache.updateNodeHealth(node2.getId(), RED, null, null, failing);
        assertThat(healthInfoCache.getHealthInfo().fileSettingsHealthInfo(), equalTo(failing));

        // master election in progress (no master): old value is preserved and old master's late arrivals are still accepted
        ClusterState noMaster = ClusterStateCreationUtils.state(node1, null, node1, allNodes);
        healthInfoCache.clusterChanged(new ClusterChangedEvent("test", noMaster, withNode2AsMaster));
        assertThat(healthInfoCache.getHealthInfo().fileSettingsHealthInfo(), equalTo(failing));
        healthInfoCache.updateNodeHealth(node2.getId(), RED, null, null, failing);
        assertThat(healthInfoCache.getHealthInfo().fileSettingsHealthInfo(), equalTo(failing));

        // new master (node1) elected: old value is kept until new master reports
        ClusterState withNode1AsMaster = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        healthInfoCache.clusterChanged(new ClusterChangedEvent("test", withNode1AsMaster, noMaster));
        assertThat(healthInfoCache.getHealthInfo().fileSettingsHealthInfo(), equalTo(failing));

        // delayed update from old master (node2) is rejected; old value is still kept
        healthInfoCache.updateNodeHealth(node2.getId(), RED, null, null, failing);
        assertThat(healthInfoCache.getHealthInfo().fileSettingsHealthInfo(), equalTo(failing));

        // new master (node1) publishes green — accepted
        var green = FileSettingsHealthInfo.INITIAL_ACTIVE.successful();
        healthInfoCache.updateNodeHealth(node1.getId(), GREEN, null, null, green);
        assertThat(healthInfoCache.getHealthInfo().fileSettingsHealthInfo(), equalTo(green));
    }

    public void testNotAHealthNode() {
        HealthInfoCache healthInfoCache = HealthInfoCache.create(clusterService);
        healthInfoCache.updateNodeHealth(
            node1.getId(),
            GREEN,
            randomDslHealthInfo(),
            randomRepoHealthInfo(),
            FileSettingsHealthInfo.INDETERMINATE,
            randomDlmFrozenTransitionsHealthInfo()
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
        assertThat(healthInfoCache.getHealthInfo().dlmFrozenTransitionsHealthInfo(), is(nullValue()));
    }

}
