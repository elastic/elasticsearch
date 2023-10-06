/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterChangedEvent;
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
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.test.NodeRoles.masterNode;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class GatewayServiceTests extends ESTestCase {

    private GatewayService createService(final Settings.Builder settings) {
        final ClusterService clusterService = new ClusterService(
            Settings.builder().put("cluster.name", "GatewayServiceTests").build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            (TaskManager) null
        );
        return createService(settings, clusterService, null);
    }

    private GatewayService createService(
        final Settings.Builder settings,
        final ClusterService clusterService,
        final ThreadPool threadPool
    ) {
        return new GatewayService(
            settings.build(),
            (reason, priority, listener) -> fail("should not reroute"),
            clusterService,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            threadPool
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
        ClusterStateUpdateTask clusterStateUpdateTask = service.new RecoverStateUpdateTask(expectedTerm);

        ClusterState stateWithBlock = buildClusterState(1, expectedTerm);

        ClusterState recoveredState = clusterStateUpdateTask.execute(stateWithBlock);
        assertNotEquals(recoveredState, stateWithBlock);
        assertThat(recoveredState.blocks().global(ClusterBlockLevel.METADATA_WRITE), not(hasItem(STATE_NOT_RECOVERED_BLOCK)));

        ClusterState clusterState = clusterStateUpdateTask.execute(recoveredState);
        assertSame(recoveredState, clusterState);
    }

    public void testRecoveryWillAbortIfExpectedTermDoesNotMatch() throws Exception {
        final GatewayService service = createService(Settings.builder());
        final long expectedTerm = randomLongBetween(1, 42);
        final ClusterStateUpdateTask clusterStateUpdateTask = service.new RecoverStateUpdateTask(expectedTerm);

        final ClusterState stateWithBlock = buildClusterState(1, randomLongBetween(43, 99));

        final ClusterState recoveredState = clusterStateUpdateTask.execute(stateWithBlock);
        assertSame(recoveredState, stateWithBlock);
    }

    public void testNoActionWhenNodeIsNotMaster() {
        final String localNodeId = randomAlphaOfLength(10);
        final DiscoveryNode localNode = DiscoveryNodeUtils.builder(localNodeId)
            .applySettings(settings(IndexVersion.current()).put(masterNode()).build())
            .address(new TransportAddress(TransportAddress.META_ADDRESS, 9300))
            .build();

        final DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder().localNodeId(localNodeId).add(localNode);
        if (randomBoolean()) {
            final String masterNodeId = randomAlphaOfLength(11);
            final DiscoveryNode masterNode = DiscoveryNodeUtils.create(masterNodeId);
            discoveryNodesBuilder.masterNodeId(masterNodeId).add(masterNode);
        }

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder.build())
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
            .build();

        final ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        when(clusterChangedEvent.state()).thenReturn(clusterState);

        final GatewayService service = createService(Settings.builder());
        service.clusterChanged(clusterChangedEvent);
        assertThat(service.currentPendingStateRecovery, nullValue());
    }

    public void testNoActionWhenStateIsAlreadyRecovered() {
        final String localNodeId = randomAlphaOfLength(10);
        final DiscoveryNode localNode = DiscoveryNodeUtils.builder(localNodeId)
            .applySettings(settings(IndexVersion.current()).put(masterNode()).build())
            .address(new TransportAddress(TransportAddress.META_ADDRESS, 9300))
            .build();
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().masterNodeId(localNodeId).localNodeId(localNodeId).add(localNode).build())
            .build();

        final ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        when(clusterChangedEvent.state()).thenReturn(clusterState);

        final GatewayService service = createService(Settings.builder());
        service.clusterChanged(clusterChangedEvent);
        assertThat(service.currentPendingStateRecovery, nullValue());
    }

    public void testImmediateRecovery() {
        final Settings.Builder settingsBuilder = Settings.builder();
        final int expectedNumberOfDataNodes = randomIntBetween(1, 3);
        if (randomBoolean()) {
            settingsBuilder.put("gateway.expected_data_nodes", expectedNumberOfDataNodes);
        }
        final ClusterService clusterService = mock(ClusterService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        final GatewayService service = createService(settingsBuilder, clusterService, threadPool);

        final ClusterState clusterState = buildClusterState(expectedNumberOfDataNodes, randomLongBetween(1, 42));
        final ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        when(clusterChangedEvent.state()).thenReturn(clusterState);
        service.clusterChanged(clusterChangedEvent);
        final GatewayService.PendingStateRecovery currentPendingStateRecovery = service.currentPendingStateRecovery;
        assertThat(currentPendingStateRecovery, notNullValue());
        verify(clusterService).submitUnbatchedStateUpdateTask(
            eq("local-gateway-elected-state"),
            any(GatewayService.RecoverStateUpdateTask.class)
        );

        // Will *not* run recover again for the same term
        Mockito.clearInvocations(clusterService);
        service.clusterChanged(clusterChangedEvent);
        assertSame(currentPendingStateRecovery, service.currentPendingStateRecovery);
        verifyNoInteractions(clusterService);

        // Will run recover again for a newer term
        when(clusterChangedEvent.state()).thenReturn(buildClusterState(expectedNumberOfDataNodes, clusterState.term() + 1));
        service.clusterChanged(clusterChangedEvent);
        verify(clusterService).submitUnbatchedStateUpdateTask(
            eq("local-gateway-elected-state"),
            any(GatewayService.RecoverStateUpdateTask.class)
        );

        // Nothing is ever scheduled for immediate recover
        verifyNoInteractions(threadPool);
    }

    public void testScheduledRecovery() {
        final Settings.Builder settingsBuilder = Settings.builder();
        final int expectedNumberOfDataNodes = randomIntBetween(3, 5);
        settingsBuilder.put("gateway.expected_data_nodes", expectedNumberOfDataNodes);
        if (randomBoolean()) {
            settingsBuilder.put("gateway.recover_after_time", TimeValue.timeValueMinutes(10));
        }

        final ClusterService clusterService = mock(ClusterService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        final GatewayService service = createService(settingsBuilder, clusterService, threadPool);

        // Schedule recovery for the initial term
        final long initialTerm = randomLongBetween(1, 42);
        final ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        when(clusterChangedEvent.state()).thenReturn(buildClusterState(expectedNumberOfDataNodes - 1, initialTerm));
        final Scheduler.ScheduledCancellable scheduledCancellable = mock(Scheduler.ScheduledCancellable.class);
        final ArgumentCaptor<AbstractRunnable> runnableCaptor = ArgumentCaptor.forClass(AbstractRunnable.class);
        when(threadPool.schedule(runnableCaptor.capture(), any(), any())).thenReturn(scheduledCancellable);
        service.clusterChanged(clusterChangedEvent);
        final GatewayService.PendingStateRecovery pendingStateRecoveryOfInitialTerm = service.currentPendingStateRecovery;
        final AbstractRunnable runnableOfInitialTerm = runnableCaptor.getValue();
        assertThat(runnableOfInitialTerm, notNullValue());
        verifyNoInteractions(clusterService);

        // Schedule will be cancelled if the cluster can recover immediately due to expected data nodes is reached
        when(clusterChangedEvent.state()).thenReturn(buildClusterState(expectedNumberOfDataNodes, initialTerm));
        service.clusterChanged(clusterChangedEvent);
        verify(scheduledCancellable).cancel();
        verify(clusterService).submitUnbatchedStateUpdateTask(
            eq("local-gateway-elected-state"),
            any(GatewayService.RecoverStateUpdateTask.class)
        );

        // Re-schedule for term+1
        Mockito.clearInvocations(clusterService);
        final long termPlus1 = initialTerm + 1;
        when(clusterChangedEvent.state()).thenReturn(buildClusterState(expectedNumberOfDataNodes - 1, termPlus1));
        service.clusterChanged(clusterChangedEvent);
        final GatewayService.PendingStateRecovery pendingRecoveryOfTermPlus1 = service.currentPendingStateRecovery;
        assertNotSame(pendingRecoveryOfTermPlus1, pendingStateRecoveryOfInitialTerm);
        final AbstractRunnable runnableOfTermPlus1 = runnableCaptor.getValue();
        assertNotSame(runnableOfTermPlus1, runnableOfInitialTerm);

        // A newer schedule for term+2 will make schedule of term+1 a no-op when it runs
        final long termPlus2 = initialTerm + 2;
        when(clusterChangedEvent.state()).thenReturn(buildClusterState(expectedNumberOfDataNodes - 1, termPlus2));
        service.clusterChanged(clusterChangedEvent);
        assertNotSame(service.currentPendingStateRecovery, pendingRecoveryOfTermPlus1);
        final AbstractRunnable runnableOfTermPlus2 = runnableCaptor.getValue();
        assertNotSame(runnableOfTermPlus2, runnableOfTermPlus1);
        runnableOfTermPlus1.run();
        verifyNoInteractions(clusterService);

        // Runnable of term+2 will complete the recovery
        runnableOfTermPlus2.run();
        verify(clusterService).submitUnbatchedStateUpdateTask(
            eq("local-gateway-elected-state"),
            any(GatewayService.RecoverStateUpdateTask.class)
        );
    }

    public void testScheduledRecoveryWithRecoverAfterNodes() {
        final Settings.Builder settingsBuilder = Settings.builder();
        final int expectedNumberOfDataNodes = randomIntBetween(3, 5);
        if (randomBoolean()) {
            settingsBuilder.put("gateway.expected_data_nodes", expectedNumberOfDataNodes);
        } else {
            settingsBuilder.put("gateway.recover_after_time", TimeValue.timeValueMinutes(10));
        }
        final int recoverAfterNodes = expectedNumberOfDataNodes - 1;
        settingsBuilder.put("gateway.recover_after_data_nodes", recoverAfterNodes);

        final ClusterService clusterService = mock(ClusterService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        final GatewayService service = createService(settingsBuilder, clusterService, threadPool);

        // Not recover because recover_after_data_nodes is not met
        final long initialTerm = randomLongBetween(1, 42);
        final ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        when(clusterChangedEvent.state()).thenReturn(buildClusterState(recoverAfterNodes - 1, initialTerm));
        service.clusterChanged(clusterChangedEvent);
        verifyNoInteractions(threadPool);
        verifyNoInteractions(clusterService);

        // Schedule recover when recover_after_data_nodes is met
        when(clusterChangedEvent.state()).thenReturn(buildClusterState(recoverAfterNodes, initialTerm));
        final Scheduler.ScheduledCancellable scheduledCancellable = mock(Scheduler.ScheduledCancellable.class);
        final ArgumentCaptor<AbstractRunnable> runnableCaptor = ArgumentCaptor.forClass(AbstractRunnable.class);
        when(threadPool.schedule(runnableCaptor.capture(), any(), any())).thenReturn(scheduledCancellable);
        service.clusterChanged(clusterChangedEvent);
        final AbstractRunnable runnableOfInitialTerm = runnableCaptor.getValue();
        assertThat(runnableOfInitialTerm, notNullValue());

        // Schedule will be cancelled when recover_after_data_nodes drops below required number
        when(clusterChangedEvent.state()).thenReturn(buildClusterState(recoverAfterNodes - 1, initialTerm));
        service.clusterChanged(clusterChangedEvent);
        verify(scheduledCancellable).cancel();

        // Reschedule when recover_after_data_nodes is reached again
        when(clusterChangedEvent.state()).thenReturn(buildClusterState(recoverAfterNodes, initialTerm));
        service.clusterChanged(clusterChangedEvent);
        final AbstractRunnable anotherRunnableOfInitialTerm = runnableCaptor.getValue();
        assertNotSame(anotherRunnableOfInitialTerm, runnableOfInitialTerm);
    }

    private ClusterState buildClusterState(int numberOfNodes, long expectedTerm) {
        assert numberOfNodes >= 1;
        final String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNodeUtils.builder(nodeId)
            .applySettings(settings(IndexVersion.current()).put(masterNode()).build())
            .address(new TransportAddress(TransportAddress.META_ADDRESS, 9300))
            .build();
        final DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder()
            .localNodeId(nodeId)
            .masterNodeId(nodeId)
            .add(masterNode);
        for (int i = 1; i < numberOfNodes; i++) {
            discoveryNodesBuilder.add(DiscoveryNodeUtils.create("node-" + i, randomAlphaOfLength(10) + i));
        }

        ClusterState stateWithBlock = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder.build())
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(expectedTerm).build()).build())
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        return stateWithBlock;
    }
}
