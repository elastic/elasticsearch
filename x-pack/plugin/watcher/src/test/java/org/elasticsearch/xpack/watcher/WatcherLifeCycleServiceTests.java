/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.NotMultiProjectCapable;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.WatcherMetadata;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WatcherLifeCycleServiceTests extends ESTestCase {

    private WatcherService watcherService;
    private WatcherLifeCycleService lifeCycleService;
    @NotMultiProjectCapable(description = "Watcher is not available in serverless")
    private ProjectId projectId = ProjectId.DEFAULT;

    @Before
    public void prepareServices() {
        ClusterService clusterService = mock(ClusterService.class);
        Answer<Object> answer = invocationOnMock -> {
            AckedClusterStateUpdateTask updateTask = (AckedClusterStateUpdateTask) invocationOnMock.getArguments()[1];
            updateTask.onAllNodesAcked();
            return null;
        };
        doAnswer(answer).when(clusterService).submitUnbatchedStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        watcherService = mock(WatcherService.class);
        lifeCycleService = new WatcherLifeCycleService(clusterService, watcherService);
    }

    public void testNoRestartWithoutAllocationIdsConfigured() {
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(new Index("anything", "foo")).build();
        ClusterState previousClusterState = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")))
            .putRoutingTable(projectId, RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        IndexRoutingTable watchRoutingTable = IndexRoutingTable.builder(new Index(Watch.INDEX, "foo")).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
            .putProjectMetadata(
                ProjectMetadata.builder(projectId)
                    .put(IndexTemplateMetadata.builder(HISTORY_TEMPLATE_NAME).patterns(randomIndexPatterns()))
                    .build()
            )
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")))
            .putRoutingTable(projectId, RoutingTable.builder().add(watchRoutingTable).build())
            .build();

        when(watcherService.validate(clusterState)).thenReturn(true);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, previousClusterState));
        verify(watcherService).setDesiredPaused("no watcher index found");

        // Re-publishing the same intent is harmless; WatcherService coalesces it.
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, previousClusterState));
        verify(watcherService, times(2)).setDesiredPaused("no watcher index found");
    }

    public void testStartWithStateNotRecoveredBlock() {
        DiscoveryNodes.Builder nodes = new DiscoveryNodes.Builder().add(
            DiscoveryNodeUtils.builder("id1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build()
        ).masterNodeId("id1").localNodeId("id1");
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .nodes(nodes)
            .build();
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, clusterState));
        verifyNoMoreInteractions(watcherService);
    }

    public void testShutdown() {
        IndexRoutingTable watchRoutingTable = IndexRoutingTable.builder(new Index(Watch.INDEX, "foo")).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")))
            .putRoutingTable(projectId, RoutingTable.builder().add(watchRoutingTable).build())
            .putProjectMetadata(
                ProjectMetadata.builder(projectId)
                    .put(IndexTemplateMetadata.builder(HISTORY_TEMPLATE_NAME).patterns(randomIndexPatterns()))
                    .build()
            )
            .build();

        when(watcherService.validate(clusterState)).thenReturn(true);

        lifeCycleService.shutDown();
        verify(watcherService, never()).setDesiredStopped(anyString());
        verify(watcherService).setDesiredShutdown();

        reset(watcherService);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, clusterState));
        verifyNoMoreInteractions(watcherService);
    }

    public void testManualStartStop() {
        Index index = new Index(Watch.INDEX, "uuid");
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addShard(
            TestShardRouting.newShardRouting(new ShardId(index, 0), "node_1", true, ShardRoutingState.STARTED)
        );
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(Watch.INDEX)
            .settings(settings(IndexVersion.current()).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 6)) // the internal index format,
                                                                                                            // required
            .numberOfShards(1)
            .numberOfReplicas(0);
        ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(projectId)
            .put(indexMetadataBuilder)
            .put(IndexTemplateMetadata.builder(HISTORY_TEMPLATE_NAME).patterns(randomIndexPatterns()));
        if (randomBoolean()) {
            metadataBuilder.putCustom(WatcherMetadata.TYPE, new WatcherMetadata(false));
        }
        ProjectMetadata metadata = metadataBuilder.build();
        IndexRoutingTable indexRoutingTable = indexRoutingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")))
            .putRoutingTable(projectId, RoutingTable.builder().add(indexRoutingTable).build())
            .putProjectMetadata(metadata)
            .build();

        when(watcherService.validate(clusterState)).thenReturn(true);

        // mark watcher manually as stopped
        ClusterState stoppedClusterState = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")))
            .putRoutingTable(projectId, RoutingTable.builder().add(indexRoutingTable).build())
            .putProjectMetadata(ProjectMetadata.builder(metadata).putCustom(WatcherMetadata.TYPE, new WatcherMetadata(true)).build())
            .build();

        lifeCycleService.clusterChanged(new ClusterChangedEvent("foo", stoppedClusterState, clusterState));
        verify(watcherService).setDesiredStopped("watcher manually marked to shutdown by cluster state update");

        // Starting via cluster state update, as the watcher metadata block is removed/set to true
        reset(watcherService);
        when(watcherService.validate(clusterState)).thenReturn(true);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, stoppedClusterState));
        verify(watcherService).setDesiredRunning(eq(clusterState), anyList(), anyString());

        // The lifecycle service republishes intent; WatcherService deduplicates the unchanged routing fingerprint.
        reset(watcherService);
        when(watcherService.validate(clusterState)).thenReturn(true);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService).setDesiredRunning(eq(clusterState), anyList(), anyString());
    }

    public void testRoutingChangeWhileStartingRequestsLatestState() {
        /*
         * Regression test for a race where a replica shard transitions to STARTED while watcher
         * is mid-start (state=STARTING, reloadInner in flight). Without handling this case the
         * stale reloadInner completes with shardCount=1, causing every node to schedule all
         * watches and producing an alternating throttled/executed history pattern.
         *
         * The STARTING state is reached here via validate() returning false on the first
         * cluster-changed event (e.g. index not yet ready), which drives state to STOPPED; the
         * next event with the same routing but validate()=true then transitions STOPPED→STARTING.
         */
        Index watchIndex = new Index(Watch.INDEX, "uuid");
        ShardId shardId = new ShardId(watchIndex, 0);
        DiscoveryNodes nodes = new DiscoveryNodes.Builder().masterNodeId("node_1")
            .localNodeId("node_1")
            .add(newNode("node_1"))
            .add(newNode("node_2"))
            .build();
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(Watch.INDEX)
            .settings(settings(IndexVersion.current()).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 6))
            .numberOfShards(1)
            .numberOfReplicas(1);
        ProjectMetadata metadata = ProjectMetadata.builder(projectId)
            .put(IndexTemplateMetadata.builder(HISTORY_TEMPLATE_NAME).patterns(randomIndexPatterns()))
            .put(indexMetadataBuilder)
            .build();

        // CS_S: only the primary on the local node (replica not yet allocated)
        IndexRoutingTable primaryOnly = IndexRoutingTable.builder(watchIndex)
            .addShard(TestShardRouting.newShardRouting(shardId, "node_1", true, STARTED))
            .build();
        ClusterState csWithPrimary = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(nodes)
            .putRoutingTable(projectId, RoutingTable.builder().add(primaryOnly).build())
            .putProjectMetadata(metadata)
            .build();

        // CS_3: primary on node_1, replica now STARTED on node_2
        IndexRoutingTable primaryAndReplica = IndexRoutingTable.builder(watchIndex)
            .addShard(TestShardRouting.newShardRouting(shardId, "node_1", true, STARTED))
            .addShard(TestShardRouting.newShardRouting(shardId, "node_2", false, STARTED))
            .build();
        ClusterState csWithReplica = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(nodes)
            .putRoutingTable(projectId, RoutingTable.builder().add(primaryAndReplica).build())
            .putProjectMetadata(metadata)
            .build();

        ClusterState emptyState = ClusterState.builder(new ClusterName("my-cluster")).nodes(nodes).putProjectMetadata(metadata).build();

        // Step 1: validation rejects the initial routing.
        when(watcherService.validate(csWithPrimary)).thenReturn(false);
        lifeCycleService.clusterChanged(new ClusterChangedEvent(randomIdentifier(), csWithPrimary, emptyState));
        verify(watcherService).setDesiredStopped("watcher failed validation");

        // Step 2: the same routing becomes valid and is published as the desired running state.
        reset(watcherService);
        when(watcherService.validate(csWithPrimary)).thenReturn(true);
        lifeCycleService.clusterChanged(new ClusterChangedEvent(randomIdentifier(), csWithPrimary, emptyState));
        verify(watcherService).setDesiredRunning(eq(csWithPrimary), anyList(), anyString());

        // Step 3: replica becomes STARTED while startup for CS_S is still in flight. Publish the
        // latest state; WatcherService's reconciler is responsible for coalescing both requests.
        reset(watcherService);
        when(watcherService.validate(csWithReplica)).thenReturn(true);
        lifeCycleService.clusterChanged(new ClusterChangedEvent(randomIdentifier(), csWithReplica, csWithPrimary));
        verify(watcherService).setDesiredRunning(eq(csWithReplica), anyList(), eq("watcher shard allocation changed"));
    }

    public void testPublishesRunningIntentWithIdenticalRoutingTable() {
        /*
         * WatcherService owns routing-fingerprint deduplication, so the lifecycle service publishes every valid intent.
         */
        startWatcher();

        ClusterChangedEvent[] events = masterChangeScenario();
        assertThat(events[1].previousState(), equalTo(events[0].state()));
        assertFalse(events[1].routingTableChanged());

        for (ClusterChangedEvent event : events) {
            when(watcherService.validate(event.state())).thenReturn(true);
            lifeCycleService.clusterChanged(event);
        }
        verify(watcherService).setDesiredRunning(eq(events[0].state()), anyList(), anyString());
        verify(watcherService).setDesiredRunning(eq(events[1].state()), anyList(), anyString());
    }

    private ClusterChangedEvent[] masterChangeScenario() {
        DiscoveryNodes nodes = new DiscoveryNodes.Builder().localNodeId("node_1").add(newNode("node_1")).add(newNode("node_2")).build();

        Index index = new Index(Watch.INDEX, "uuid");
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addShard(
            TestShardRouting.newShardRouting(new ShardId(index, 0), "node_1", true, ShardRoutingState.STARTED)
        );
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTableBuilder.build()).build();

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(Watch.INDEX)
            .settings(settings(IndexVersion.current()).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 6)) // the internal index format,
            // required
            .numberOfShards(1)
            .numberOfReplicas(0);
        ProjectMetadata metadata = ProjectMetadata.builder(projectId)
            .put(IndexTemplateMetadata.builder(HISTORY_TEMPLATE_NAME).patterns(randomIndexPatterns()))
            .put(indexMetadataBuilder)
            .build();

        GlobalRoutingTable globalRoutingTable = GlobalRoutingTable.builder().put(metadata.id(), routingTable).build();

        ClusterState emptyState = ClusterState.builder(new ClusterName("my-cluster")).nodes(nodes).putProjectMetadata(metadata).build();
        ClusterState stateWithMasterNode1 = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(nodes.withMasterNodeId("node_1"))
            .putProjectMetadata(metadata)
            .routingTable(globalRoutingTable)
            .build();
        ClusterState stateWithMasterNode2 = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(nodes.withMasterNodeId("node_2"))
            .putProjectMetadata(metadata)
            .routingTable(globalRoutingTable)
            .build();

        return new ClusterChangedEvent[] {
            new ClusterChangedEvent("any", stateWithMasterNode1, emptyState),
            new ClusterChangedEvent("any", stateWithMasterNode2, stateWithMasterNode1) };
    }

    public void testNoLocalShards() {
        Index watchIndex = new Index(Watch.INDEX, "foo");
        ShardId shardId = new ShardId(watchIndex, 0);
        DiscoveryNodes nodes = new DiscoveryNodes.Builder().masterNodeId("node_1")
            .localNodeId("node_1")
            .add(newNode("node_1"))
            .add(newNode("node_2"))
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder(Watch.INDEX)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 6))
            .build();

        IndexRoutingTable watchRoutingTable = IndexRoutingTable.builder(watchIndex)
            .addShard(
                randomBoolean()
                    ? TestShardRouting.newShardRouting(shardId, "node_1", true, STARTED)
                    : TestShardRouting.newShardRouting(shardId, "node_1", "node_2", true, RELOCATING)
            )
            .build();
        ClusterState clusterStateWithLocalShards = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(nodes)
            .putRoutingTable(projectId, RoutingTable.builder().add(watchRoutingTable).build())
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();

        // shard moved over to node 2
        IndexRoutingTable watchRoutingTableNode2 = IndexRoutingTable.builder(watchIndex)
            .addShard(
                randomBoolean()
                    ? TestShardRouting.newShardRouting(shardId, "node_2", true, STARTED)
                    : TestShardRouting.newShardRouting(shardId, "node_2", "node_1", true, RELOCATING)
            )
            .build();
        ClusterState clusterStateWithoutLocalShards = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(nodes)
            .putRoutingTable(projectId, RoutingTable.builder().add(watchRoutingTableNode2).build())
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();

        // Publish the routing fingerprint for the local shard.
        when(watcherService.validate(eq(clusterStateWithLocalShards))).thenReturn(true);
        when(watcherService.validate(eq(clusterStateWithoutLocalShards))).thenReturn(false);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterStateWithLocalShards, clusterStateWithoutLocalShards));
        verify(watcherService).setDesiredRunning(eq(clusterStateWithLocalShards), anyList(), eq("watcher shard allocation changed"));
        verify(watcherService, times(1)).validate(eq(clusterStateWithLocalShards));
        verifyNoMoreInteractions(watcherService);

        // no more local shards, lets pause execution
        reset(watcherService);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterStateWithoutLocalShards, clusterStateWithLocalShards));
        verify(watcherService).setDesiredPaused("no local watcher shards found");
        verifyNoMoreInteractions(watcherService);

        // The lifecycle service publishes intent for every relevant cluster state; WatcherService coalesces duplicates.
        reset(watcherService);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterStateWithoutLocalShards, clusterStateWithoutLocalShards));
        verify(watcherService).setDesiredPaused("no local watcher shards found");
    }

    public void testReplicaWasAddedOrRemoved() {
        Index watchIndex = new Index(Watch.INDEX, "foo");
        ShardId shardId = new ShardId(watchIndex, 0);
        ShardId secondShardId = new ShardId(watchIndex, 1);
        DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder().masterNodeId("node_1")
            .localNodeId("node_1")
            .add(newNode("node_1"))
            .add(newNode("node_2"))
            .build();

        ShardRouting firstShardOnSecondNode = TestShardRouting.newShardRouting(shardId, "node_2", true, STARTED);
        ShardRouting secondShardOnFirstNode = TestShardRouting.newShardRouting(secondShardId, "node_1", true, STARTED);

        IndexRoutingTable previousWatchRoutingTable = IndexRoutingTable.builder(watchIndex)
            .addShard(secondShardOnFirstNode)
            .addShard(firstShardOnSecondNode)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(Watch.INDEX)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 6))
            .build();

        ClusterState stateWithPrimaryShard = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(discoveryNodes)
            .putRoutingTable(projectId, RoutingTable.builder().add(previousWatchRoutingTable).build())
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();

        // add a replica in the local node
        boolean addShardOnLocalNode = randomBoolean();
        final ShardRouting addedShardRouting;
        if (addShardOnLocalNode) {
            addedShardRouting = TestShardRouting.newShardRouting(shardId, "node_1", false, STARTED);
        } else {
            addedShardRouting = TestShardRouting.newShardRouting(secondShardId, "node_2", false, STARTED);
        }

        IndexRoutingTable currentWatchRoutingTable = IndexRoutingTable.builder(watchIndex)
            .addShard(secondShardOnFirstNode)
            .addShard(firstShardOnSecondNode)
            .addShard(addedShardRouting)
            .build();

        ClusterState stateWithReplicaAdded = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(discoveryNodes)
            .putRoutingTable(projectId, RoutingTable.builder().add(currentWatchRoutingTable).build())
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();

        // randomize between addition or removal of a replica
        boolean replicaAdded = randomBoolean();
        ClusterChangedEvent firstEvent;
        ClusterChangedEvent secondEvent;
        if (replicaAdded) {
            firstEvent = new ClusterChangedEvent("any", stateWithPrimaryShard, stateWithReplicaAdded);
            secondEvent = new ClusterChangedEvent("any", stateWithReplicaAdded, stateWithPrimaryShard);
        } else {
            firstEvent = new ClusterChangedEvent("any", stateWithReplicaAdded, stateWithPrimaryShard);
            secondEvent = new ClusterChangedEvent("any", stateWithPrimaryShard, stateWithReplicaAdded);
        }

        when(watcherService.validate(eq(firstEvent.state()))).thenReturn(true);
        lifeCycleService.clusterChanged(firstEvent);
        verify(watcherService).setDesiredRunning(eq(firstEvent.state()), anyList(), anyString());

        reset(watcherService);
        when(watcherService.validate(eq(secondEvent.state()))).thenReturn(true);
        lifeCycleService.clusterChanged(secondEvent);
        verify(watcherService).setDesiredRunning(eq(secondEvent.state()), anyList(), anyString());
    }

    // make sure that cluster state changes can be processed on nodes that do not hold data
    public void testNonDataNode() {
        Index index = new Index(Watch.INDEX, "foo");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, "node2", true, STARTED);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting);

        DiscoveryNode node1 = DiscoveryNodeUtils.builder("node_1")
            .roles(new HashSet<>(asList(randomFrom(DiscoveryNodeRole.INGEST_ROLE, DiscoveryNodeRole.MASTER_ROLE))))
            .build();

        DiscoveryNode node2 = DiscoveryNodeUtils.builder("node_2").roles(new HashSet<>(asList(DiscoveryNodeRole.DATA_ROLE))).build();

        DiscoveryNode node3 = DiscoveryNodeUtils.builder("node_3").roles(new HashSet<>(asList(DiscoveryNodeRole.DATA_ROLE))).build();

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(Watch.INDEX)
            .settings(indexSettings(IndexVersion.current(), 1, 0));

        ClusterState previousState = ClusterState.builder(new ClusterName("my-cluster"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadataBuilder))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(node1).add(node2).add(node3))
            .putRoutingTable(projectId, RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        IndexMetadata.Builder newIndexMetadataBuilder = IndexMetadata.builder(Watch.INDEX)
            .settings(indexSettings(IndexVersion.current(), 1, 1));

        ShardRouting replicaShardRouting = TestShardRouting.newShardRouting(shardId, "node3", false, STARTED);
        IndexRoutingTable.Builder newRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting).addShard(replicaShardRouting);
        ClusterState currentState = ClusterState.builder(new ClusterName("my-cluster"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(newIndexMetadataBuilder))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(node1).add(node2).add(node3))
            .putRoutingTable(projectId, RoutingTable.builder().add(newRoutingTable).build())
            .build();

        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", currentState, previousState));
        verify(watcherService).setDesiredRunning(currentState, List.of(), "starting");
    }

    public void testMissingWatcherIndexPublishesPausedIntent() {
        Index watchIndex = new Index(Watch.INDEX, "foo");
        ShardId shardId = new ShardId(watchIndex, 0);
        IndexRoutingTable watchRoutingTable = IndexRoutingTable.builder(watchIndex)
            .addShard(TestShardRouting.newShardRouting(shardId, "node_1", true, STARTED))
            .build();
        DiscoveryNodes nodes = new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")).build();

        IndexMetadata.Builder newIndexMetadataBuilder = IndexMetadata.builder(Watch.INDEX)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 6));

        ClusterState clusterStateWithWatcherIndex = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(nodes)
            .putRoutingTable(projectId, RoutingTable.builder().add(watchRoutingTable).build())
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(newIndexMetadataBuilder))
            .build();

        ClusterState clusterStateWithoutWatcherIndex = ClusterState.builder(new ClusterName("my-cluster")).nodes(nodes).build();

        when(watcherService.validate(eq(clusterStateWithWatcherIndex))).thenReturn(true);
        when(watcherService.validate(eq(clusterStateWithoutWatcherIndex))).thenReturn(false);

        // First publish a running intent by going from an empty state to one with the watcher index.
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterStateWithWatcherIndex, clusterStateWithoutWatcherIndex));
        verify(watcherService).setDesiredRunning(eq(clusterStateWithWatcherIndex), anyList(), anyString());

        // Now remove the watches index. Repeated intent is coalesced inside WatcherService.
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterStateWithoutWatcherIndex, clusterStateWithWatcherIndex));
        verify(watcherService).setDesiredPaused(anyString());

        reset(watcherService);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterStateWithoutWatcherIndex, clusterStateWithWatcherIndex));
        verify(watcherService).setDesiredPaused(anyString());
    }

    public void testWatcherServiceDoesNotStartIfIndexTemplatesAreMissing() throws Exception {
        DiscoveryNodes nodes = new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")).build();

        ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(projectId);
        boolean isHistoryTemplateAdded = randomBoolean();
        if (isHistoryTemplateAdded) {
            metadataBuilder.put(IndexTemplateMetadata.builder(HISTORY_TEMPLATE_NAME).patterns(randomIndexPatterns()));
        }
        ClusterState state = ClusterState.builder(new ClusterName("my-cluster")).nodes(nodes).putProjectMetadata(metadataBuilder).build();
        when(watcherService.validate(eq(state))).thenReturn(true);

        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", state, state));
        verify(watcherService, times(0)).setDesiredRunning(any(ClusterState.class), anyList(), anyString());
    }

    public void testWatcherStopsWhenMasterNodeIsMissing() {
        startWatcher();

        DiscoveryNodes nodes = new DiscoveryNodes.Builder().localNodeId("node_1").add(newNode("node_1")).build();
        ClusterState state = ClusterState.builder(new ClusterName("my-cluster")).nodes(nodes).build();
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", state, state));
        verify(watcherService).setDesiredPaused("no master node");
    }

    public void testWatcherStopsOnClusterLevelBlock() {
        startWatcher();

        DiscoveryNodes nodes = new DiscoveryNodes.Builder().localNodeId("node_1").masterNodeId("node_1").add(newNode("node_1")).build();
        ClusterBlocks clusterBlocks = ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_WRITES).build();
        ClusterState state = ClusterState.builder(new ClusterName("my-cluster")).nodes(nodes).blocks(clusterBlocks).build();
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", state, state));
        verify(watcherService).setDesiredPaused("write level cluster block");
    }

    public void testMasterOnlyNodeCanStart() {
        List<DiscoveryNodeRole> roles = Collections.singletonList(randomFrom(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INGEST_ROLE));
        ClusterState state = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(
                new DiscoveryNodes.Builder().masterNodeId("node_1")
                    .localNodeId("node_1")
                    .add(DiscoveryNodeUtils.builder("node_1").roles(new HashSet<>(roles)).build())
            )
            .build();

        lifeCycleService.clusterChanged(new ClusterChangedEvent("test", state, state));
        verify(watcherService).setDesiredRunning(state, List.of(), "starting");
    }

    public void testDataNodeWithoutDataCanStart() {
        ProjectMetadata metadata = ProjectMetadata.builder(projectId)
            .put(IndexTemplateMetadata.builder(HISTORY_TEMPLATE_NAME).patterns(randomIndexPatterns()))
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")))
            .putProjectMetadata(metadata)
            .build();

        lifeCycleService.clusterChanged(new ClusterChangedEvent("test", state, state));
        verify(watcherService).setDesiredPaused("no watcher index found");
    }

    // this emulates a node outage somewhere in the cluster that carried a watcher shard
    // the number of shards remains the same, but we need to ensure that watcher properly reloads
    // previously we only checked the local shard allocations, but we also need to check if shards in the cluster have changed
    public void testWatcherReloadsOnNodeOutageWithWatcherShard() {
        Index watchIndex = new Index(Watch.INDEX, "foo");
        ShardId shardId = new ShardId(watchIndex, 0);
        String localNodeId = randomFrom("node_1", "node_2");
        String outageNodeId = localNodeId.equals("node_1") ? "node_2" : "node_1";
        DiscoveryNodes previousDiscoveryNodes = new DiscoveryNodes.Builder().masterNodeId(localNodeId)
            .localNodeId(localNodeId)
            .add(newNode(localNodeId))
            .add(newNode(outageNodeId))
            .build();

        ShardRouting replicaShardRouting = TestShardRouting.newShardRouting(shardId, localNodeId, false, STARTED);
        ShardRouting primartShardRouting = TestShardRouting.newShardRouting(shardId, outageNodeId, true, STARTED);
        IndexRoutingTable previousWatchRoutingTable = IndexRoutingTable.builder(watchIndex)
            .addShard(replicaShardRouting)
            .addShard(primartShardRouting)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(Watch.INDEX)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 6))
            .build();

        ClusterState previousState = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(previousDiscoveryNodes)
            .putRoutingTable(projectId, RoutingTable.builder().add(previousWatchRoutingTable).build())
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();

        ShardRouting nowPrimaryShardRouting = replicaShardRouting.moveActiveReplicaToPrimary();
        IndexRoutingTable currentWatchRoutingTable = IndexRoutingTable.builder(watchIndex).addShard(nowPrimaryShardRouting).build();

        DiscoveryNodes currentDiscoveryNodes = new DiscoveryNodes.Builder().masterNodeId(localNodeId)
            .localNodeId(localNodeId)
            .add(newNode(localNodeId))
            .build();

        ClusterState currentState = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(currentDiscoveryNodes)
            .putRoutingTable(projectId, RoutingTable.builder().add(currentWatchRoutingTable).build())
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();

        // Publish both states; WatcherService decides whether their routing fingerprints require work.
        when(watcherService.validate(any())).thenReturn(true);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("whatever", previousState, currentState));

        reset(watcherService);
        when(watcherService.validate(any())).thenReturn(true);
        ClusterChangedEvent event = new ClusterChangedEvent("whatever", currentState, previousState);
        lifeCycleService.clusterChanged(event);
        verify(watcherService).setDesiredRunning(eq(event.state()), anyList(), anyString());
    }

    private void startWatcher() {
        Index index = new Index(Watch.INDEX, "uuid");
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addShard(
            TestShardRouting.newShardRouting(new ShardId(index, 0), "node_1", true, ShardRoutingState.STARTED)
        );
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(Watch.INDEX)
            .settings(settings(IndexVersion.current()).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 6)) // the internal index format,
                                                                                                            // required
            .numberOfShards(1)
            .numberOfReplicas(0);
        ProjectMetadata metadata = ProjectMetadata.builder(projectId)
            .put(IndexTemplateMetadata.builder(HISTORY_TEMPLATE_NAME).patterns(randomIndexPatterns()))
            .put(indexMetadataBuilder)
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")))
            .putRoutingTable(projectId, RoutingTable.builder().add(indexRoutingTableBuilder.build()).build())
            .putProjectMetadata(metadata)
            .build();
        ClusterState emptyState = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")))
            .putProjectMetadata(metadata)
            .build();

        when(watcherService.validate(state)).thenReturn(true);

        lifeCycleService.clusterChanged(new ClusterChangedEvent("foo", state, emptyState));
        verify(watcherService).setDesiredRunning(eq(state), anyList(), anyString());

        // reset the mock, the user has to mock everything themselves again
        reset(watcherService);
    }

    private List<String> randomIndexPatterns() {
        return IntStream.range(0, between(1, 10)).mapToObj(n -> randomAlphaOfLengthBetween(1, 100)).collect(Collectors.toList());
    }

    private static DiscoveryNode newNode(String nodeName) {
        return DiscoveryNodeUtils.builder(nodeName).build();
    }
}
