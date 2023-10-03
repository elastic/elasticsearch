/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import static org.elasticsearch.gateway.GatewayService.RECOVER_AFTER_DATA_NODES_SETTING;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.test.NodeRoles.masterNode;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasItem;

public class GatewayServiceTests extends ESTestCase {

    private GatewayService createService(final Settings.Builder settings) {
        final ClusterService clusterService = new ClusterService(
            Settings.builder().put("cluster.name", "GatewayServiceTests").build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            (TaskManager) null
        );
        return new GatewayService(
            settings.build(),
            (reason, priority, listener) -> fail("should not reroute"),
            clusterService,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            null
        );
    }

    public void testDefaultRecoverAfterTime() {
        // check that the default is not set
        GatewayService service = createService(Settings.builder());
        assertNull(service.recoverAfterTime());

        // ensure default is set when setting expected_data_nodes
        service = createService(Settings.builder().put("gateway.expected_data_nodes", 1));
        assertThat(service.recoverAfterTime(), Matchers.equalTo(GatewayService.DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET));

        // ensure settings override default
        final TimeValue timeValue = TimeValue.timeValueHours(3);

        // ensure default is set when setting expected_nodes
        service = createService(Settings.builder().put("gateway.recover_after_time", timeValue.toString()));
        assertThat(service.recoverAfterTime().millis(), Matchers.equalTo(timeValue.millis()));
    }

    public void testRecoverStateUpdateTask() throws Exception {
        GatewayService service = createService(Settings.builder());
        final long expectedTerm = randomLongBetween(1, 42);
        ClusterStateUpdateTask clusterStateUpdateTask = service.new RecoverStateUpdateTask(expectedTerm, () -> {});

        ClusterState stateWithBlock = buildClusterState(1, expectedTerm);

        ClusterState recoveredState = clusterStateUpdateTask.execute(stateWithBlock);
        assertNotEquals(recoveredState, stateWithBlock);
        assertThat(recoveredState.blocks().global(ClusterBlockLevel.METADATA_WRITE), not(hasItem(STATE_NOT_RECOVERED_BLOCK)));

        ClusterState clusterState = clusterStateUpdateTask.execute(recoveredState);
        assertSame(recoveredState, clusterState);
    }

    public void testRecoverWillAbortIfExpectedTermDoesNotMatch() throws Exception {
        GatewayService service = createService(Settings.builder());
        final long expectedTerm = randomLongBetween(1, 42);
        ClusterStateUpdateTask clusterStateUpdateTask = service.new RecoverStateUpdateTask(expectedTerm, () -> {});

        ClusterState stateWithBlock = buildClusterState(1, randomLongBetween(43, 99));

        ClusterState recoveredState = clusterStateUpdateTask.execute(stateWithBlock);
        assertSame(recoveredState, stateWithBlock);
    }

    public void testRecoverWillAbortIfRequiredNumberOfDataNodesNotMet() throws Exception {
        GatewayService service = createService(Settings.builder().put(RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 5));
        final long expectedTerm = randomLongBetween(1, 42);
        ClusterStateUpdateTask clusterStateUpdateTask = service.new RecoverStateUpdateTask(expectedTerm, () -> {});

        ClusterState stateWithBlock = buildClusterState(randomIntBetween(1, 4), expectedTerm);

        ClusterState recoveredState = clusterStateUpdateTask.execute(stateWithBlock);
        assertSame(recoveredState, stateWithBlock);
    }

    private ClusterState buildClusterState(int numberOfNodes, long expectedTerm) {
        assert numberOfNodes >= 1;
        final String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(
            settings(IndexVersion.current()).put(masterNode()).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            nodeId
        );
        final DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder()
            .localNodeId(nodeId)
            .masterNodeId(nodeId)
            .add(masterNode);
        for (int i = 1; i < numberOfNodes; i++) {
            discoveryNodesBuilder.add(DiscoveryNodeUtils.create("node-" + i, randomAlphaOfLength(10)));
        }

        ClusterState stateWithBlock = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder.build())
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(expectedTerm).build()).build())
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        return stateWithBlock;
    }
}
