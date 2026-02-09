/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.NO_OTHER_SHARDS_FOUND_RESPONSE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportFetchSearchShardInformationActionTests extends ESTestCase {

    private final Index index = new Index("my_index", "uuid");
    private final ShardId shardId = new ShardId(index, 0);
    private final UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "");
    private final ShardRouting newUnassignedPrimaryIndexOnly = ShardRouting.newUnassigned(
        shardId,
        true,
        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
        unassignedInfo,
        ShardRouting.Role.INDEX_ONLY
    );

    private final ThreadPool threadPool = mock(ThreadPool.class);
    private final ClusterService clusterService = mock(ClusterService.class);
    private final TransportService transportService = mock(TransportService.class);
    private final IndicesService indicesService = mock(IndicesService.class);

    private final TransportFetchSearchShardInformationAction action = new TransportFetchSearchShardInformationAction(
        threadPool,
        clusterService,
        DefaultProjectResolver.INSTANCE,
        transportService,
        new ActionFilters(new HashSet<>()),
        indicesService
    );

    public void testShardOperationThrowsException() {
        RuntimeException exc = new RuntimeException("failure");
        when(indicesService.getShardOrNull(shardId)).thenThrow(exc);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request("abc", shardId);
        action.shardOperation(request, ActionListener.wrap(response -> fail("should not be here"), e -> { assertThat(e, equalTo(exc)); }));
    }

    public void testShardOperationShardMissingOnNode() {
        when(indicesService.getShardOrNull(shardId)).thenReturn(null);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request("abc", shardId);
        action.shardOperation(request, ActionListener.wrap(response -> fail("should not be here"), e -> {
            assertThat(e, instanceOf(ShardNotFoundException.class));
        }));
    }

    public void testShardOperation() {
        IndexShard indexShard = mock(IndexShard.class);
        final long lastSearcherTime = randomLongBetween(0, 1000);
        when(indexShard.tryWithEngineOrNull(any())).thenReturn(lastSearcherTime);

        when(indicesService.getShardOrNull(shardId)).thenReturn(indexShard);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request("abc", shardId);
        action.shardOperation(request, ActionListener.wrap(response -> {
            assertThat(response.getLastSearcherAcquiredTime(), equalTo(lastSearcherTime));
        }, e -> fail("should not happen")));

        // test the function
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Function<Engine, Long>> functionCaptor = ArgumentCaptor.forClass(Function.class);
        verify(indexShard).tryWithEngineOrNull(functionCaptor.capture());

        SearchEngine searchEngine = mock(SearchEngine.class);
        when(searchEngine.getLastSearcherAcquiredTime()).thenReturn(lastSearcherTime + 1);
        assertThat(functionCaptor.getValue().apply(searchEngine), equalTo(lastSearcherTime + 1));
    }

    public void testShardsEmpty() {
        ShardRouting primaryShard = newUnassignedPrimaryIndexOnly.initialize("index_node", null, randomNonNegativeLong()).moveToStarted(1);

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard));
        ClusterState clusterState = createClusterStateFromRoutingTableBuilder(builder);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request("abc", shardId);
        Optional<ShardRouting> shardRouting = action.findSearchShard(clusterState.projectState(ProjectId.DEFAULT), request);

        assertThat(shardRouting, isEmpty());
    }

    public void testOnlyActiveShards() {
        ShardRouting primaryShard = newUnassignedPrimaryIndexOnly.initialize("index_node", null, randomNonNegativeLong()).moveToStarted(1);
        ShardRouting initializingShard = createSearchOnlyShard(shardId, "search_node_1");

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard))
            .addShard(initializingShard);
        ClusterState clusterState = createClusterStateFromRoutingTableBuilder(builder);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request("abc", shardId);
        Optional<ShardRouting> shardRouting = action.findSearchShard(clusterState.projectState(ProjectId.DEFAULT), request);

        assertThat(shardRouting, isEmpty());
    }

    public void testDifferentShardIdReturnsNoResults() {
        ShardRouting primaryShard = newUnassignedPrimaryIndexOnly.initialize("index_node", null, randomNonNegativeLong()).moveToStarted(1);
        ShardId otherShardId = new ShardId(index, 1);
        assertNotEquals(shardId, otherShardId);
        ShardRouting otherPrimaryShard = ShardRouting.newUnassigned(
            otherShardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            unassignedInfo,
            ShardRouting.Role.INDEX_ONLY
        ).initialize("index_node", null, randomNonNegativeLong()).moveToStarted(1);
        ShardRouting searchShard = createSearchOnlyShard(otherShardId, "search_node_1").moveToStarted(1);

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard))
            .addIndexShard(new IndexShardRoutingTable.Builder(otherShardId).addShard(otherPrimaryShard))
            .addShard(searchShard);
        ClusterState clusterState = createClusterStateFromRoutingTableBuilder(builder);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(null, shardId);
        Optional<ShardRouting> shardRouting = action.findSearchShard(clusterState.projectState(ProjectId.DEFAULT), request);

        assertThat(shardRouting, isEmpty());
    }

    public void testIgnoreShardsOnOwnNode() {
        ShardRouting primaryShard = newUnassignedPrimaryIndexOnly.initialize("index_node", null, randomNonNegativeLong()).moveToStarted(1);
        ShardRouting searchShard = createSearchOnlyShard(shardId, "search_node_2").moveToStarted(1);

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard))
            .addShard(searchShard);
        ClusterState clusterState = createClusterStateFromRoutingTableBuilder(builder);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(null, shardId);
        Optional<ShardRouting> shardRouting = action.findSearchShard(clusterState.projectState(ProjectId.DEFAULT), request);

        assertThat(shardRouting, isEmpty());
    }

    public void testShardRoutingIsReturned() {
        ShardRouting primaryShard = newUnassignedPrimaryIndexOnly.initialize("index_node", null, randomNonNegativeLong()).moveToStarted(1);
        ShardRouting searchShard = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard))
            .addShard(searchShard);
        ClusterState clusterState = createClusterStateFromRoutingTableBuilder(builder);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(null, shardId);
        Optional<ShardRouting> shardRouting = action.findSearchShard(clusterState.projectState(ProjectId.DEFAULT), request);

        assertThat(shardRouting, not(isEmpty()));
    }

    public void testShardRoutingReturnsRelocatingShard() {
        ShardRouting primaryShard = newUnassignedPrimaryIndexOnly.initialize("index_node", null, randomNonNegativeLong()).moveToStarted(1);
        ShardRouting searchShard = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1).relocate("search_node_1000", 1);

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard))
            .addShard(searchShard);
        ClusterState clusterState = createClusterStateFromRoutingTableBuilder(builder);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(null, shardId);
        Optional<ShardRouting> shardRouting = action.findSearchShard(clusterState.projectState(ProjectId.DEFAULT), request);

        assertThat(shardRouting, not(isEmpty()));
    }

    public void testRandomShardIsPicked() throws Exception {
        ShardRouting primaryShard = newUnassignedPrimaryIndexOnly.initialize("index_node", null, randomNonNegativeLong()).moveToStarted(1);
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard));

        for (int i = 0; i < 100; i++) {
            builder.addShard(createSearchOnlyShard(shardId, "search_node_" + i).moveToStarted(1));
        }

        ClusterState clusterState = createClusterStateFromRoutingTableBuilder(builder);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(null, shardId);
        Optional<ShardRouting> shardRoutingOptional = action.findSearchShard(clusterState.projectState(ProjectId.DEFAULT), request);
        assertThat(shardRoutingOptional, not(isEmpty()));
        ShardRouting shardRouting = shardRoutingOptional.get();

        assertBusy(() -> {
            Optional<ShardRouting> srOptional = action.findSearchShard(clusterState.projectState(ProjectId.DEFAULT), request);
            assertThat(srOptional, not(isEmpty()));
            ShardRouting nextShardRouting = srOptional.get();
            assertNotEquals(shardRouting, nextShardRouting);
        }, 10, TimeUnit.SECONDS);
    }

    public void testNodeIdFromRequestHasPrecedence() {
        ShardRouting primaryShard = newUnassignedPrimaryIndexOnly.initialize("index_node", null, randomNonNegativeLong()).moveToStarted(1);
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard));

        for (int i = 0; i < 5; i++) {
            builder.addShard(createSearchOnlyShard(shardId, "search_node_" + i).moveToStarted(1));
        }

        ClusterState clusterState = createClusterStateFromRoutingTableBuilder(builder);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(
            "search_node_3",
            shardId
        );
        Optional<ShardRouting> shardRoutingOptional = action.findSearchShard(clusterState.projectState(ProjectId.DEFAULT), request);
        assertThat(shardRoutingOptional, not(isEmpty()));
        ShardRouting shardRouting = shardRoutingOptional.get();

        assertThat(shardRouting.currentNodeId(), equalTo("search_node_3"));
    }

    public void testIndexWithinDataStreamMostRecentIndexIsSetToNow() {
        reset(transportService);

        // have a data stream named like index
        String indexName = ".ds-logs-2025-01-28-000002";
        final Index backingIndex = new Index(indexName, randomUUID());
        final ShardId shardId = new ShardId(backingIndex, 0);

        ShardRouting primaryShard = createPrimaryShard(shardId);
        ShardRouting searchShard = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1).relocate("search_node_1000", 1);

        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(backingIndex)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard))
            .addShard(searchShard);
        RoutingTable routingTable = RoutingTable.builder().add(builder).build();
        ClusterState clusterState = createClusterStateWithDataStreams(routingTable, 2, idx -> System.currentTimeMillis(), backingIndex);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        long currentTimeMillis = randomLongBetween(1000, 2000);
        when(threadPool.absoluteTimeInMillis()).thenReturn(currentTimeMillis);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(null, shardId);

        action.doExecute(createTask(), request, ActionListener.wrap(response -> {
            assertThat(response.getLastSearcherAcquiredTime(), equalTo(currentTimeMillis));
        }, e -> fail("should not happen")));

        verifyNoInteractions(transportService);
    }

    public void testIndexWithinDataStreamOlderIndexIsNotSetToNow() {
        reset(transportService);

        // index is ending on 1, but generation is 2
        // also no other shard copy of this one exists in the cluster
        // so this should exit with a no other shards found response
        String indexName = ".ds-logs-2025-01-28-000001";
        final Index backingIndex = new Index(indexName, randomUUID());
        final ShardId shardId = new ShardId(backingIndex, 0);

        final Index index2 = new Index(".ds-logs-2025-01-28-000002", randomUUID());
        final ShardId shardId2 = new ShardId(index2, 0);

        ShardRouting primaryShard = createPrimaryShard(shardId);
        ShardRouting primaryShardSecondIndex = createPrimaryShard(shardId2);

        IndexRoutingTable.Builder indexRoutingTable1 = IndexRoutingTable.builder(backingIndex)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard));
        IndexRoutingTable.Builder indexRoutingTable2 = IndexRoutingTable.builder(index2)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId2).addShard(primaryShardSecondIndex));
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable1).add(indexRoutingTable2).build();
        ClusterState clusterState = createClusterStateWithDataStreams(
            routingTable,
            2,
            idx -> System.currentTimeMillis(),
            backingIndex,
            index2
        );
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(null, shardId);

        action.doExecute(
            createTask(),
            request,
            ActionListener.wrap(
                response -> { assertThat(response, equalTo(NO_OTHER_SHARDS_FOUND_RESPONSE)); },
                e -> fail("should not happen")
            )
        );

        verifyNoInteractions(transportService);
    }

    public void testIndexWithinDataStreamOlderThanMaxAllowedAge() {
        reset(transportService);

        // have a data stream named like index
        String indexName = ".ds-logs-2025-01-28-000001";
        final Index backingIndex = new Index(indexName, randomUUID());
        final ShardId shardId = new ShardId(backingIndex, 0);

        ShardRouting primaryShard = createPrimaryShard(shardId);
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(backingIndex)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard));

        long currentTimeMillis = System.currentTimeMillis();
        when(threadPool.absoluteTimeInMillis()).thenReturn(currentTimeMillis);

        Function<Index, Long> oldIndexCreationDate = idx -> {
            long oneMonthAgo = currentTimeMillis - (30 * 86400 * 1000L);
            return idx.getName().equals(indexName) ? oneMonthAgo : currentTimeMillis;
        };

        RoutingTable routingTable = RoutingTable.builder().add(builder).build();
        ClusterState clusterState = createClusterStateWithDataStreams(routingTable, 1, oldIndexCreationDate, backingIndex);
        when(clusterService.state()).thenReturn(clusterState);

        TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(null, shardId);

        action.doExecute(
            createTask(),
            request,
            ActionListener.wrap(
                response -> { assertThat(response, equalTo(NO_OTHER_SHARDS_FOUND_RESPONSE)); },
                e -> fail("should not happen")
            )
        );

        verifyNoInteractions(transportService);
    }

    private Task createTask() {
        return new Task(1, "type", TransportFetchSearchShardInformationAction.TYPE.name(), "description", null, Collections.emptyMap());
    }

    private ClusterState createClusterStateWithDataStreams(
        RoutingTable routingTable,
        int generation,
        Function<Index, Long> creationDateFunction,
        Index... indices
    ) {
        ImmutableOpenMap<ProjectId, RoutingTable> globalRoutingTableMap = ImmutableOpenMap.builder(Map.of(ProjectId.DEFAULT, routingTable))
            .build();
        DiscoveryNode indexNode = DiscoveryNodeUtils.create("index_node");
        DiscoveryNode searchNode1 = DiscoveryNodeUtils.create("search_node_1");
        DiscoveryNode searchNode2 = DiscoveryNodeUtils.create("search_node_2");

        DataStreamMetadata dataStreamMetadata = new DataStreamMetadata(
            ImmutableOpenMap.<String, DataStream>builder()
                .fPut(
                    "my_data_stream",
                    DataStreamTestHelper.newInstance("my_data_stream", Arrays.asList(indices), generation, null, false, null)
                )
                .build(),
            ImmutableOpenMap.of()
        );

        Map<String, IndexMetadata> indexMetaDataMap = Arrays.stream(indices)
            .map(
                index -> Map.entry(
                    index.getName(),
                    IndexMetadata.builder(index.getName())
                        .settings(
                            Settings.builder()
                                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                                .put(IndexMetadata.SETTING_CREATION_DATE, creationDateFunction.apply(index))
                                .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                        )
                        .numberOfReplicas(1)
                        .numberOfShards(1)
                        .build()
                )
            )
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        ProjectMetadata projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataStreamMetadata.TYPE, dataStreamMetadata)
            .indices(indexMetaDataMap)
            .build();

        return ClusterState.builder(new ClusterName("test_cluster"))
            .routingTable(new GlobalRoutingTable(globalRoutingTableMap))
            .putProjectMetadata(projectMetadata)
            .metadata(Metadata.builder().putCustom(DataStreamMetadata.TYPE, dataStreamMetadata).put(projectMetadata).build())
            .nodes(
                DiscoveryNodes.builder()
                    .add(indexNode)
                    .masterNodeId(indexNode.getId())
                    .add(searchNode2)
                    .localNodeId(searchNode2.getId())
                    .add(searchNode1)
                    .build()
            )
            .build();
    }

    private ClusterState createClusterStateFromRoutingTableBuilder(IndexRoutingTable.Builder builder) {
        RoutingTable routingTable = RoutingTable.builder().add(builder).build();

        ImmutableOpenMap<ProjectId, RoutingTable> globalRoutingTableMap = ImmutableOpenMap.builder(Map.of(ProjectId.DEFAULT, routingTable))
            .build();
        DiscoveryNode indexNode = DiscoveryNodeUtils.create("index_node");
        DiscoveryNode searchNode1 = DiscoveryNodeUtils.create("search_node_1");
        DiscoveryNode searchNode2 = DiscoveryNodeUtils.create("search_node_2");

        return ClusterState.builder(new ClusterName("test_cluster"))
            .routingTable(new GlobalRoutingTable(globalRoutingTableMap))
            .nodes(DiscoveryNodes.builder().add(indexNode).add(searchNode1).add(searchNode2).localNodeId(searchNode2.getId()).build())
            .build();
    }

    private ShardRouting createSearchOnlyShard(ShardId shardId, String nodeId) {
        return ShardRouting.newUnassigned(
            shardId,
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            unassignedInfo,
            ShardRouting.Role.SEARCH_ONLY
        ).initialize(nodeId, null, randomNonNegativeLong());
    }

    private ShardRouting createPrimaryShard(ShardId shardId) {
        return ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            unassignedInfo,
            ShardRouting.Role.INDEX_ONLY
        ).initialize("index_node", null, randomNonNegativeLong()).moveToStarted(1);
    }
}
