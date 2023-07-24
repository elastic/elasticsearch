/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.WatcherIndexingListener.Configuration;
import org.elasticsearch.xpack.watcher.WatcherIndexingListener.ShardAllocationConfiguration;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.junit.Before;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.xpack.watcher.WatcherIndexingListener.INACTIVE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WatcherIndexingListenerTests extends ESTestCase {

    private WatcherIndexingListener listener;
    private WatchParser parser = mock(WatchParser.class);
    private ClockMock clock = new ClockMock();
    private TriggerService triggerService = mock(TriggerService.class);

    private ShardId shardId = mock(ShardId.class);
    private Engine.IndexResult result = mock(Engine.IndexResult.class);
    private Engine.Index operation = mock(Engine.Index.class);
    private Engine.Delete delete = mock(Engine.Delete.class);

    @Before
    public void setup() throws Exception {
        clock.freeze();
        listener = new WatcherIndexingListener(parser, clock, triggerService, () -> WatcherState.STARTED);

        Map<ShardId, ShardAllocationConfiguration> map = new HashMap<>();
        map.put(shardId, new ShardAllocationConfiguration(0, 1, Collections.singletonList("foo")));

        listener.setConfiguration(new Configuration(Watch.INDEX, map));
    }

    public void testPreIndexCheckIndex() throws Exception {
        when(shardId.getIndexName()).thenReturn(randomAlphaOfLength(10));

        Engine.Index index = listener.preIndex(shardId, operation);
        assertThat(index, is(operation));
        verifyNoMoreInteractions(parser);
    }

    public void testPreIndexCheckActive() throws Exception {
        listener.setConfiguration(INACTIVE);
        when(shardId.getIndexName()).thenReturn(Watch.INDEX);

        Engine.Index index = listener.preIndex(shardId, operation);
        assertThat(index, is(operation));
        verifyNoMoreInteractions(parser);
    }

    public void testPostIndex() throws Exception {
        when(operation.id()).thenReturn(randomAlphaOfLength(10));
        when(operation.source()).thenReturn(BytesArray.EMPTY);
        when(shardId.getIndexName()).thenReturn(Watch.INDEX);
        List<Engine.Result.Type> types = new ArrayList<>(List.of(Engine.Result.Type.values()));
        types.remove(Engine.Result.Type.FAILURE);
        when(result.getResultType()).thenReturn(randomFrom(types));

        boolean watchActive = randomBoolean();
        boolean isNewWatch = randomBoolean();
        Watch watch = mockWatch("_id", watchActive, isNewWatch);
        when(parser.parseWithSecrets(any(), eq(true), any(), any(), any(), anyLong(), anyLong())).thenReturn(watch);

        listener.postIndex(shardId, operation, result);
        ZonedDateTime now = DateUtils.nowWithMillisResolution(clock);
        verify(parser).parseWithSecrets(eq(operation.id()), eq(true), eq(BytesArray.EMPTY), eq(now), any(), anyLong(), anyLong());

        if (isNewWatch) {
            if (watchActive) {
                verify(triggerService).add(eq(watch));
            } else {
                verify(triggerService).remove(eq("_id"));
            }
        }
    }

    public void testPostIndexWhenStopped() throws Exception {
        listener = new WatcherIndexingListener(parser, clock, triggerService, () -> WatcherState.STOPPED);
        Map<ShardId, ShardAllocationConfiguration> map = new HashMap<>();
        map.put(shardId, new ShardAllocationConfiguration(0, 1, Collections.singletonList("foo")));
        listener.setConfiguration(new Configuration(Watch.INDEX, map));
        when(operation.id()).thenReturn(randomAlphaOfLength(10));
        when(operation.source()).thenReturn(BytesArray.EMPTY);
        when(shardId.getIndexName()).thenReturn(Watch.INDEX);
        List<Engine.Result.Type> types = new ArrayList<>(List.of(Engine.Result.Type.values()));
        types.remove(Engine.Result.Type.FAILURE);
        when(result.getResultType()).thenReturn(randomFrom(types));

        boolean watchActive = randomBoolean();
        boolean isNewWatch = randomBoolean();
        Watch watch = mockWatch("_id", watchActive, isNewWatch);
        when(parser.parseWithSecrets(any(), eq(true), any(), any(), any(), anyLong(), anyLong())).thenReturn(watch);

        listener.postIndex(shardId, operation, result);
        ZonedDateTime now = DateUtils.nowWithMillisResolution(clock);
        verify(parser).parseWithSecrets(eq(operation.id()), eq(true), eq(BytesArray.EMPTY), eq(now), any(), anyLong(), anyLong());
        verifyNoMoreInteractions(triggerService);
    }

    // this test emulates an index with 10 shards, and ensures that triggering only happens on a
    // single shard
    public void testPostIndexWatchGetsOnlyTriggeredOnceAcrossAllShards() throws Exception {
        String id = randomAlphaOfLength(10);
        int totalShardCount = randomIntBetween(1, 10);
        boolean watchActive = randomBoolean();
        boolean isNewWatch = randomBoolean();
        Watch watch = mockWatch(id, watchActive, isNewWatch);
        when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);

        when(shardId.getIndexName()).thenReturn(Watch.INDEX);
        when(parser.parseWithSecrets(any(), eq(true), any(), any(), any(), anyLong(), anyLong())).thenReturn(watch);

        for (int idx = 0; idx < totalShardCount; idx++) {
            final Map<ShardId, ShardAllocationConfiguration> localShards = new HashMap<>();
            localShards.put(shardId, new ShardAllocationConfiguration(idx, totalShardCount, Collections.emptyList()));
            Configuration configuration = new Configuration(Watch.INDEX, localShards);
            listener.setConfiguration(configuration);
            listener.postIndex(shardId, operation, result);
        }

        // no matter how many shards we had, this should have been only called once
        if (isNewWatch) {
            if (watchActive) {
                verify(triggerService, times(1)).add(eq(watch));
            } else {
                verify(triggerService, times(1)).remove(eq(watch.id()));
            }
        }
    }

    private Watch mockWatch(String id, boolean active, boolean isNewWatch) {
        WatchStatus.State watchState = mock(WatchStatus.State.class);
        when(watchState.isActive()).thenReturn(active);

        WatchStatus watchStatus = mock(WatchStatus.class);
        when(watchStatus.state()).thenReturn(watchState);
        if (isNewWatch) {
            when(watchStatus.version()).thenReturn(-1L);
        } else {
            when(watchStatus.version()).thenReturn(randomLong());
        }

        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn(id);
        when(watch.status()).thenReturn(watchStatus);

        return watch;
    }

    public void testPostIndexCheckParsingException() throws Exception {
        String id = randomAlphaOfLength(10);
        when(operation.id()).thenReturn(id);
        when(operation.source()).thenReturn(BytesArray.EMPTY);
        when(shardId.getIndexName()).thenReturn(Watch.INDEX);
        when(parser.parseWithSecrets(any(), eq(true), any(), any(), any(), anyLong(), anyLong())).thenThrow(new IOException("self thrown"));
        when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);

        ElasticsearchParseException exc = expectThrows(
            ElasticsearchParseException.class,
            () -> listener.postIndex(shardId, operation, result)
        );
        assertThat(exc.getMessage(), containsString("Could not parse watch"));
        assertThat(exc.getMessage(), containsString(id));
    }

    public void testPostIndexRemoveTriggerOnDocumentRelatedException() throws Exception {
        when(operation.id()).thenReturn("_id");
        when(result.getResultType()).thenReturn(Engine.Result.Type.FAILURE);
        when(result.getFailure()).thenReturn(new RuntimeException());
        when(shardId.getIndexName()).thenReturn(Watch.INDEX);

        listener.postIndex(shardId, operation, result);
        verifyNoMoreInteractions(triggerService);
    }

    public void testPostIndexRemoveTriggerOnDocumentRelatedException_ignoreNonWatcherDocument() throws Exception {
        when(operation.id()).thenReturn("_id");
        when(result.getResultType()).thenReturn(Engine.Result.Type.FAILURE);
        when(result.getFailure()).thenReturn(new RuntimeException());
        when(shardId.getIndexName()).thenReturn(randomAlphaOfLength(4));

        listener.postIndex(shardId, operation, result);
        verifyNoMoreInteractions(triggerService);
    }

    public void testPostIndexRemoveTriggerOnEngineLevelException() throws Exception {
        when(operation.id()).thenReturn("_id");
        when(shardId.getIndexName()).thenReturn(Watch.INDEX);

        listener.postIndex(shardId, operation, new ElasticsearchParseException("whatever"));
        verifyNoMoreInteractions(triggerService);
    }

    public void testPostIndexRemoveTriggerOnEngineLevelException_ignoreNonWatcherDocument() throws Exception {
        when(operation.id()).thenReturn("_id");
        when(shardId.getIndexName()).thenReturn("anything");
        when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);

        listener.postIndex(shardId, operation, new ElasticsearchParseException("whatever"));
        verifyNoMoreInteractions(triggerService);
    }

    public void testPreDeleteCheckActive() throws Exception {
        listener.setConfiguration(INACTIVE);
        listener.preDelete(shardId, delete);

        verifyNoMoreInteractions(triggerService);
    }

    public void testPreDeleteCheckIndex() throws Exception {
        when(shardId.getIndexName()).thenReturn(randomAlphaOfLength(10));

        listener.preDelete(shardId, delete);

        verifyNoMoreInteractions(triggerService);
    }

    public void testPreDelete() throws Exception {
        when(shardId.getIndexName()).thenReturn(Watch.INDEX);
        when(delete.id()).thenReturn("_id");

        listener.preDelete(shardId, delete);

        verify(triggerService).remove(eq("_id"));
    }

    //
    // tests for cluster state updates
    //
    public void testClusterChangedNoMetadata() throws Exception {
        ClusterState state = mockClusterState(randomAlphaOfLength(10));
        listener.clusterChanged(new ClusterChangedEvent("any", state, state));

        assertThat(listener.getConfiguration().isIndexAndActive(Watch.INDEX), is(true));
    }

    public void testClusterChangedNoWatchIndex() throws Exception {
        Map<ShardId, ShardAllocationConfiguration> map = new HashMap<>();
        map.put(shardId, new ShardAllocationConfiguration(0, 1, Collections.singletonList("foo")));
        Configuration randomConfiguration = new Configuration(randomAlphaOfLength(10), map);
        listener.setConfiguration(randomConfiguration);

        ClusterState clusterState = mockClusterState(null);
        ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        when(clusterChangedEvent.metadataChanged()).thenReturn(true);
        when(clusterChangedEvent.state()).thenReturn(clusterState);

        listener.clusterChanged(clusterChangedEvent);

        assertThat(listener.getConfiguration(), equalTo(INACTIVE));
    }

    public void testClusterChangedWatchAliasChanged() throws Exception {
        String newActiveWatchIndex = randomAlphaOfLength(10);
        RoutingTable routingTable = mock(RoutingTable.class);
        when(routingTable.hasIndex(eq(newActiveWatchIndex))).thenReturn(true);

        ClusterState currentClusterState = mockClusterState(newActiveWatchIndex);
        when(currentClusterState.routingTable()).thenReturn(routingTable);
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(newNode("node_1")).localNodeId("node_1").build();
        when(currentClusterState.getNodes()).thenReturn(nodes);
        RoutingNodes routingNodes = mock(RoutingNodes.class);
        RoutingNode routingNode = mock(RoutingNode.class);
        boolean emptyShards = randomBoolean();

        if (emptyShards) {
            when(routingNode.shardsWithState(eq(newActiveWatchIndex), any(ShardRoutingState[].class))).thenReturn(Stream.empty());
        } else {
            Index index = new Index(newActiveWatchIndex, "uuid");
            ShardId shardId = new ShardId(index, 0);
            ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, "node_1", true, STARTED);
            when(routingNode.shardsWithState(eq(newActiveWatchIndex), eq(STARTED), eq(RELOCATING))).thenReturn(Stream.of(shardRouting));
            when(routingTable.allShards(eq(newActiveWatchIndex))).thenReturn(List.of(shardRouting));
            IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting).build();
            when(routingTable.index(newActiveWatchIndex)).thenReturn(indexRoutingTable);
        }

        when(routingNodes.node(eq("node_1"))).thenReturn(routingNode);
        when(currentClusterState.getRoutingNodes()).thenReturn(routingNodes);

        ClusterState previousClusterState = mockClusterState(randomAlphaOfLength(8));
        when(previousClusterState.routingTable()).thenReturn(routingTable);

        ClusterChangedEvent event = new ClusterChangedEvent("something", currentClusterState, previousClusterState);
        listener.clusterChanged(event);

        if (emptyShards) {
            assertThat(listener.getConfiguration(), is(INACTIVE));
        } else {
            assertThat(listener.getConfiguration().isIndexAndActive(newActiveWatchIndex), is(true));
        }
    }

    public void testClusterChangedNoRoutingChanges() throws Exception {
        Index index = new Index(Watch.INDEX, "foo");
        IndexRoutingTable watchRoutingTable = IndexRoutingTable.builder(index).build();
        ClusterState previousState = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")))
            .routingTable(RoutingTable.builder().add(watchRoutingTable).build())
            .build();

        ClusterState currentState = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(newNode("node_1")).add(newNode("node_2")))
            .routingTable(RoutingTable.builder().add(watchRoutingTable).build())
            .build();

        Configuration configuration = listener.getConfiguration();
        assertThat(configuration.isIndexAndActive(Watch.INDEX), is(true));

        ClusterChangedEvent event = new ClusterChangedEvent("something", currentState, previousState);
        listener.clusterChanged(event);

        assertThat(listener.getConfiguration(), is(configuration));
        assertThat(listener.getConfiguration().isIndexAndActive(Watch.INDEX), is(true));
    }

    // a shard is marked as relocating, no change in the routing yet (replica might be added,
    // shard might be offloaded)
    public void testCheckAllocationIdsOnShardStarted() throws Exception {
        Index index = new Index(Watch.INDEX, "foo");
        ShardId shardId = new ShardId(index, 0);
        ShardRoutingState randomState = randomFrom(STARTED, RELOCATING);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            "current",
            randomState == RELOCATING ? "other" : null,
            true,
            randomState
        );
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting).build();

        Map<ShardId, ShardAllocationConfiguration> allocationIds = listener.getLocalShardAllocationIds(
            asList(shardRouting),
            indexRoutingTable
        );

        assertThat(allocationIds.size(), is(1));
        assertThat(allocationIds.get(shardId).index, is(0));
        assertThat(allocationIds.get(shardId).shardCount, is(1));
    }

    public void testCheckAllocationIdsWithoutShards() throws Exception {
        Index index = new Index(Watch.INDEX, "foo");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, "other", true, STARTED);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting).build();

        Map<ShardId, ShardAllocationConfiguration> allocationIds = listener.getLocalShardAllocationIds(
            Collections.emptyList(),
            indexRoutingTable
        );
        assertThat(allocationIds.size(), is(0));
    }

    public void testCheckAllocationIdsWithSeveralShards() {
        // setup 5 shards, one replica, 10 shards total, all started
        Index index = new Index(Watch.INDEX, "foo");
        ShardId firstShardId = new ShardId(index, 0);
        ShardId secondShardId = new ShardId(index, 1);

        List<ShardRouting> localShards = new ArrayList<>();
        localShards.add(TestShardRouting.newShardRouting(firstShardId, "node1", true, STARTED));
        localShards.add(TestShardRouting.newShardRouting(secondShardId, "node1", true, STARTED));

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(localShards.get(0))
            .addShard(localShards.get(1))
            .addShard(TestShardRouting.newShardRouting(firstShardId, "node2", false, STARTED))
            .addShard(TestShardRouting.newShardRouting(secondShardId, "node2", false, STARTED))
            .build();

        Map<ShardId, ShardAllocationConfiguration> allocationIds = listener.getLocalShardAllocationIds(localShards, indexRoutingTable);
        assertThat(allocationIds.size(), is(2));
    }

    // no matter how many copies of a shard exist, a watch should always be triggered exactly once
    public void testShardConfigurationShouldBeTriggeredExactlyOnce() throws Exception {
        // random number of shards
        int numberOfShards = randomIntBetween(1, 20);
        int numberOfDocuments = randomIntBetween(1, 10000);
        BitSet bitSet = new BitSet(numberOfDocuments);
        logger.info("Testing [{}] documents with [{}] shards", numberOfDocuments, numberOfShards);

        for (int currentShardId = 0; currentShardId < numberOfShards; currentShardId++) {
            ShardAllocationConfiguration sac = new ShardAllocationConfiguration(currentShardId, numberOfShards, Collections.emptyList());

            for (int i = 0; i < numberOfDocuments; i++) {
                boolean shouldBeTriggered = sac.shouldBeTriggered("watch_" + i);
                boolean hasAlreadyBeenTriggered = bitSet.get(i);
                if (shouldBeTriggered) {
                    String message = Strings.format("Watch [%s] has already been " + "triggered", i);
                    assertThat(message, hasAlreadyBeenTriggered, is(false));
                    bitSet.set(i);
                }
            }
        }

        assertThat(bitSet.cardinality(), is(numberOfDocuments));
    }

    // ensure that non data nodes, deal properly with this cluster state listener
    public void testOnNonDataNodes() {
        listener.setConfiguration(INACTIVE);
        Index index = new Index(Watch.INDEX, "foo");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, "node2", true, STARTED);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting);

        DiscoveryNode node1 = DiscoveryNodeUtils.builder("node_1")
            .roles(new HashSet<>(Collections.singletonList(randomFrom(DiscoveryNodeRole.INGEST_ROLE, DiscoveryNodeRole.MASTER_ROLE))))
            .build();

        DiscoveryNode node2 = DiscoveryNodeUtils.builder("node_2")
            .roles(new HashSet<>(Collections.singletonList(DiscoveryNodeRole.DATA_ROLE)))
            .build();

        DiscoveryNode node3 = DiscoveryNodeUtils.builder("node_3")
            .roles(new HashSet<>(Collections.singletonList(DiscoveryNodeRole.DATA_ROLE)))
            .build();

        IndexMetadata.Builder indexMetadataBuilder = createIndexBuilder(Watch.INDEX, 1, 0);

        ClusterState previousState = ClusterState.builder(new ClusterName("my-cluster"))
            .metadata(Metadata.builder().put(indexMetadataBuilder))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(node1).add(node2).add(node3))
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        IndexMetadata.Builder newIndexMetadataBuilder = createIndexBuilder(Watch.INDEX, 1, 1);

        ShardRouting replicaShardRouting = TestShardRouting.newShardRouting(shardId, "node3", false, STARTED);
        IndexRoutingTable.Builder newRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting).addShard(replicaShardRouting);
        ClusterState currentState = ClusterState.builder(new ClusterName("my-cluster"))
            .metadata(Metadata.builder().put(newIndexMetadataBuilder))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(node1).add(node2).add(node3))
            .routingTable(RoutingTable.builder().add(newRoutingTable).build())
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("something", currentState, previousState);
        listener.clusterChanged(event);
        assertThat(listener.getConfiguration(), is(INACTIVE));
    }

    public void testListenerWorksIfOtherIndicesChange() throws Exception {
        DiscoveryNode node1 = newNode("node_1");
        DiscoveryNode node2 = newNode("node_2");

        Index index = new Index("random-index", "foo");
        ShardId firstShardId = new ShardId(index, 0);

        IndexMetadata.Builder indexMetadataBuilder = createIndexBuilder("random-index", 2, 1);

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(firstShardId, "node_1", true, STARTED))
            .addShard(TestShardRouting.newShardRouting(firstShardId, "node_2", false, STARTED));

        ClusterState previousState = ClusterState.builder(new ClusterName("my-cluster"))
            .metadata(Metadata.builder().put(indexMetadataBuilder))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(node1).add(node2))
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        IndexMetadata.Builder currentMetadataBuilder = createIndexBuilder(Watch.INDEX, 2, 1);

        boolean useWatchIndex = randomBoolean();
        String indexName = useWatchIndex ? Watch.INDEX : "other-index-name";
        Index otherIndex = new Index(indexName, "foo");
        ShardId watchShardId = new ShardId(otherIndex, 0);

        IndexRoutingTable.Builder currentRoutingTable = IndexRoutingTable.builder(otherIndex)
            .addShard(TestShardRouting.newShardRouting(watchShardId, "node_1", true, STARTED))
            .addShard(TestShardRouting.newShardRouting(watchShardId, "node_2", false, STARTED));

        ClusterState currentState = ClusterState.builder(new ClusterName("my-cluster"))
            .metadata(Metadata.builder().put(currentMetadataBuilder))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(node1).add(node2))
            .routingTable(RoutingTable.builder().add(currentRoutingTable).build())
            .build();

        listener.setConfiguration(INACTIVE);
        ClusterChangedEvent event = new ClusterChangedEvent("something", currentState, previousState);
        listener.clusterChanged(event);
        if (useWatchIndex) {
            assertThat(listener.getConfiguration(), is(not(INACTIVE)));
        } else {
            assertThat(listener.getConfiguration(), is(INACTIVE));
        }
    }

    // 4 nodes, each node has one shard, now node 3 fails, which means only one node should
    // reload, where as two should not
    // this test emulates on of those two nodes
    public void testThatShardConfigurationIsNotReloadedNonAffectedShardsChange() {
        listener.setConfiguration(INACTIVE);

        DiscoveryNode node1 = newNode("node_1");
        DiscoveryNode node2 = newNode("node_2");
        DiscoveryNode node3 = newNode("node_3");
        DiscoveryNode node4 = newNode("node_4");

        String localNode = randomFrom("node_1", "node_2");

        Index index = new Index(Watch.INDEX, "foo");
        ShardId firstShardId = new ShardId(index, 0);
        ShardId secondShardId = new ShardId(index, 1);

        IndexMetadata.Builder indexMetadataBuilder = createIndexBuilder(Watch.INDEX, 2, 1);

        ShardRouting firstShardRoutingPrimary = TestShardRouting.newShardRouting(firstShardId, "node_1", true, STARTED);
        ShardRouting firstShardRoutingReplica = TestShardRouting.newShardRouting(firstShardId, "node_2", false, STARTED);
        ShardRouting secondShardRoutingPrimary = TestShardRouting.newShardRouting(secondShardId, "node_3", true, STARTED);
        ShardRouting secondShardRoutingReplica = TestShardRouting.newShardRouting(secondShardId, "node_4", false, STARTED);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(firstShardRoutingPrimary)
            .addShard(firstShardRoutingReplica)
            .addShard(secondShardRoutingPrimary)
            .addShard(secondShardRoutingReplica);

        ClusterState previousState = ClusterState.builder(new ClusterName("my-cluster"))
            .metadata(Metadata.builder().put(indexMetadataBuilder))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId(localNode).add(node1).add(node2).add(node3).add(node4))
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        ClusterState emptyState = ClusterState.builder(new ClusterName("my-cluster"))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId(localNode).add(node1).add(node2).add(node3).add(node4))
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("something", previousState, emptyState);
        listener.clusterChanged(event);
        Configuration configuration = listener.getConfiguration();
        assertThat(configuration, is(not(INACTIVE)));

        // now create a cluster state where node 4 is missing
        IndexMetadata.Builder newIndexMetadataBuilder = createIndexBuilder(Watch.INDEX, 2, 1);

        IndexRoutingTable.Builder newRoutingTable = IndexRoutingTable.builder(index)
            .addShard(firstShardRoutingPrimary)
            .addShard(firstShardRoutingReplica)
            .addShard(secondShardRoutingPrimary);

        ClusterState currentState = ClusterState.builder(new ClusterName("my-cluster"))
            .metadata(Metadata.builder().put(newIndexMetadataBuilder))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId(localNode).add(node1).add(node2).add(node3).add(node4))
            .routingTable(RoutingTable.builder().add(newRoutingTable).build())
            .build();

        ClusterChangedEvent nodeGoneEvent = new ClusterChangedEvent("something", currentState, previousState);
        listener.clusterChanged(nodeGoneEvent);

        // ensure no configuration replacement has happened
        assertThat(listener.getConfiguration(), is(configuration));
    }

    // if the creates a .watches alias that points to two indices, set watcher to be inactive
    public void testWithAliasPointingToTwoIndicesSetsWatcherInactive() {
        listener.setConfiguration(INACTIVE);
        DiscoveryNode node1 = newNode("node_1");

        // index foo pointing to .watches
        Index fooIndex = new Index("foo", "someuuid");
        ShardId fooShardId = new ShardId(fooIndex, 0);
        ShardRouting fooShardRouting = TestShardRouting.newShardRouting(fooShardId, node1.getId(), true, STARTED);
        IndexRoutingTable.Builder fooIndexRoutingTable = IndexRoutingTable.builder(fooIndex).addShard(fooShardRouting);

        // regular cluster state with correct single alias pointing to watches index
        ClusterState previousState = ClusterState.builder(new ClusterName("my-cluster"))
            .metadata(Metadata.builder().put(createIndexBuilder("foo", 1, 0).putAlias(AliasMetadata.builder(Watch.INDEX))))
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(node1))
            .routingTable(RoutingTable.builder().add(fooIndexRoutingTable).build())
            .build();

        // index bar pointing to .watches
        Index barIndex = new Index("bar", "someuuid2");
        ShardId barShardId = new ShardId(barIndex, 0);
        IndexMetadata.Builder barIndexMetadata = createIndexBuilder("bar", 1, 0).putAlias(AliasMetadata.builder(Watch.INDEX));
        ShardRouting barShardRouting = TestShardRouting.newShardRouting(barShardId, node1.getId(), true, STARTED);
        IndexRoutingTable.Builder barIndexRoutingTable = IndexRoutingTable.builder(barIndex).addShard(barShardRouting);

        // cluster state with two indices pointing to the .watches index
        ClusterState currentState = ClusterState.builder(new ClusterName("my-cluster"))
            .metadata(
                Metadata.builder().put(createIndexBuilder("foo", 1, 0).putAlias(AliasMetadata.builder(Watch.INDEX))).put(barIndexMetadata)
            )
            .nodes(new DiscoveryNodes.Builder().masterNodeId("node_1").localNodeId("node_1").add(node1))
            .routingTable(
                RoutingTable.builder().add(IndexRoutingTable.builder(fooIndex).addShard(fooShardRouting)).add(barIndexRoutingTable).build()
            )
            .build();

        ClusterChangedEvent nodeGoneEvent = new ClusterChangedEvent("something", currentState, previousState);
        listener.clusterChanged(nodeGoneEvent);

        // ensure no configuration replacement has happened
        assertThat(listener.getConfiguration(), is(INACTIVE));
    }

    public void testThatIndexingListenerBecomesInactiveWithoutMasterNode() {
        ClusterState clusterStateWithMaster = mockClusterState(Watch.INDEX);
        ClusterState clusterStateWithoutMaster = mockClusterState(Watch.INDEX);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node_1").add(newNode("node_1")).build();
        when(clusterStateWithoutMaster.nodes()).thenReturn(nodes);

        assertThat(listener.getConfiguration(), is(not(INACTIVE)));
        listener.clusterChanged(new ClusterChangedEvent("something", clusterStateWithoutMaster, clusterStateWithMaster));

        assertThat(listener.getConfiguration(), is(INACTIVE));
    }

    public void testThatIndexingListenerBecomesInactiveOnClusterBlock() {
        ClusterState clusterState = mockClusterState(Watch.INDEX);
        ClusterState clusterStateWriteBlock = mockClusterState(Watch.INDEX);
        ClusterBlocks clusterBlocks = ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_WRITES).build();
        when(clusterStateWriteBlock.getBlocks()).thenReturn(clusterBlocks);

        assertThat(listener.getConfiguration(), is(not(INACTIVE)));
        listener.clusterChanged(new ClusterChangedEvent("something", clusterStateWriteBlock, clusterState));

        assertThat(listener.getConfiguration(), is(INACTIVE));
    }

    //
    // helper methods
    //
    /**
     * create a mock cluster state, the returns the specified index as watch index
     */
    private ClusterState mockClusterState(String watchIndex) {
        Metadata metadata = mock(Metadata.class);
        if (watchIndex == null) {
            when(metadata.getIndicesLookup()).thenReturn(Collections.emptySortedMap());
        } else {
            SortedMap<String, IndexAbstraction> indices = new TreeMap<>();

            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getIndex()).thenReturn(new Index(watchIndex, randomAlphaOfLength(10)));
            indices.put(watchIndex, new IndexAbstraction.ConcreteIndex(indexMetadata));

            // now point the alias, if the watch index is not .watches
            if (watchIndex.equals(Watch.INDEX) == false) {
                AliasMetadata aliasMetadata = mock(AliasMetadata.class);
                when(aliasMetadata.writeIndex()).thenReturn(true);
                when(aliasMetadata.getAlias()).thenReturn(Watch.INDEX);
                when(indexMetadata.getAliases()).thenReturn(Map.of(Watch.INDEX, aliasMetadata));
                indices.put(Watch.INDEX, new IndexAbstraction.Alias(aliasMetadata, List.of(indexMetadata)));
                when(metadata.index(any(Index.class))).thenReturn(indexMetadata);
            }

            when(metadata.getIndicesLookup()).thenReturn(indices);
        }

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(metadata);

        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node_1").masterNodeId("node_1").add(newNode("node_1")).build();
        when(clusterState.nodes()).thenReturn(nodes);
        when(clusterState.getBlocks()).thenReturn(ClusterBlocks.EMPTY_CLUSTER_BLOCK);

        return clusterState;
    }

    private IndexMetadata.Builder createIndexBuilder(String name, int numberOfShards, int numberOfReplicas) {
        return IndexMetadata.builder(name).settings(indexSettings(Version.CURRENT, numberOfShards, numberOfReplicas));
    }

    private static DiscoveryNode newNode(String nodeId) {
        return DiscoveryNodeUtils.create(nodeId);
    }
}
