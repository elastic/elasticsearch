/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse.IndexResult;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.shards.ClusterShardLimitIT;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfoTests;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.MetaDataIndexStateService.INDEX_CLOSED_BLOCK;
import static org.elasticsearch.cluster.metadata.MetaDataIndexStateService.INDEX_CLOSED_BLOCK_ID;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.shards.ClusterShardLimitIT.ShardCounts.forDataNodeCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaDataIndexStateServiceTests extends ESTestCase {

    public void testCloseRoutingTable() {
        final Set<Index> nonBlockedIndices = new HashSet<>();
        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        final Map<Index, IndexResult> results = new HashMap<>();

        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTable")).build();
        for (int i = 0; i < randomIntBetween(1, 25); i++) {
            final String indexName = "index-" + i;

            if (randomBoolean()) {
                state = addOpenedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
                nonBlockedIndices.add(state.metaData().index(indexName).getIndex());
            } else {
                final ClusterBlock closingBlock = MetaDataIndexStateService.createIndexClosingBlock();
                state = addBlockedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state, closingBlock);
                final Index index = state.metaData().index(indexName).getIndex();
                blockedIndices.put(index, closingBlock);
                if (randomBoolean()) {
                    results.put(index, new CloseIndexResponse.IndexResult(index));
                } else {
                    results.put(index, new CloseIndexResponse.IndexResult(index, new Exception("test")));
                }
            }
        }

        final ClusterState updatedState = MetaDataIndexStateService.closeRoutingTable(state, blockedIndices, results).v1();
        assertThat(updatedState.metaData().indices().size(), equalTo(nonBlockedIndices.size() + blockedIndices.size()));

        for (Index nonBlockedIndex : nonBlockedIndices) {
            assertIsOpened(nonBlockedIndex.getName(), updatedState);
            assertThat(updatedState.blocks().hasIndexBlockWithId(nonBlockedIndex.getName(), INDEX_CLOSED_BLOCK_ID), is(false));
        }
        for (Index blockedIndex : blockedIndices.keySet()) {
            if (results.get(blockedIndex).hasFailures() == false) {
                assertIsClosed(blockedIndex.getName(), updatedState);
            } else {
                assertIsOpened(blockedIndex.getName(), updatedState);
                assertThat(updatedState.blocks().hasIndexBlockWithId(blockedIndex.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
            }
        }
    }

    public void testCloseRoutingTableWithRestoredIndex() {
        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTableWithRestoredIndex")).build();

        String indexName = "restored-index";
        ClusterBlock block = MetaDataIndexStateService.createIndexClosingBlock();
        state = addRestoredIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
        state = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().blocks(state.blocks()).addIndexBlock(indexName, block))
            .build();

        final Index index = state.metaData().index(indexName).getIndex();
        final ClusterState updatedState =
            MetaDataIndexStateService.closeRoutingTable(state, singletonMap(index, block), singletonMap(index, new IndexResult(index)))
                .v1();
        assertIsOpened(index.getName(), updatedState);
        assertThat(updatedState.blocks().hasIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
    }

    public void testCloseRoutingTableWithSnapshottedIndex() {
        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTableWithSnapshottedIndex")).build();

        String indexName = "snapshotted-index";
        ClusterBlock block = MetaDataIndexStateService.createIndexClosingBlock();
        state = addSnapshotIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
        state = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().blocks(state.blocks()).addIndexBlock(indexName, block))
            .build();

        final Index index = state.metaData().index(indexName).getIndex();
        final ClusterState updatedState =
            MetaDataIndexStateService.closeRoutingTable(state, singletonMap(index, block), singletonMap(index, new IndexResult(index)))
                .v1();
        assertIsOpened(index.getName(), updatedState);
        assertThat(updatedState.blocks().hasIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
    }

    public void testCloseRoutingTableRemovesRoutingTable() {
        final Set<Index> nonBlockedIndices = new HashSet<>();
        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        final Map<Index, IndexResult> results = new HashMap<>();
        final ClusterBlock closingBlock = MetaDataIndexStateService.createIndexClosingBlock();

        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTableRemovesRoutingTable")).build();
        for (int i = 0; i < randomIntBetween(1, 25); i++) {
            final String indexName = "index-" + i;

            if (randomBoolean()) {
                state = addOpenedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
                nonBlockedIndices.add(state.metaData().index(indexName).getIndex());
            } else {
                state = addBlockedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state, closingBlock);
                final Index index = state.metaData().index(indexName).getIndex();
                blockedIndices.put(index, closingBlock);
                if (randomBoolean()) {
                    results.put(index, new CloseIndexResponse.IndexResult(index));
                } else {
                    results.put(index, new CloseIndexResponse.IndexResult(index, new Exception("test")));
                }
            }
        }

        state = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes())
                .add(new DiscoveryNode("old_node", buildNewFakeTransportAddress(), emptyMap(),
                    new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES), Version.V_7_0_0))
                .add(new DiscoveryNode("new_node", buildNewFakeTransportAddress(), emptyMap(),
                    new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES), Version.V_7_2_0)))
            .build();

        state = MetaDataIndexStateService.closeRoutingTable(state, blockedIndices, results).v1();
        assertThat(state.metaData().indices().size(), equalTo(nonBlockedIndices.size() + blockedIndices.size()));

        for (Index nonBlockedIndex : nonBlockedIndices) {
            assertIsOpened(nonBlockedIndex.getName(), state);
            assertThat(state.blocks().hasIndexBlockWithId(nonBlockedIndex.getName(), INDEX_CLOSED_BLOCK_ID), is(false));
        }
        for (Index blockedIndex : blockedIndices.keySet()) {
            if (results.get(blockedIndex).hasFailures() == false) {
                IndexMetaData indexMetaData = state.metaData().index(blockedIndex);
                assertThat(indexMetaData.getState(), is(IndexMetaData.State.CLOSE));
                Settings indexSettings = indexMetaData.getSettings();
                assertThat(indexSettings.hasValue(MetaDataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(false));
                assertThat(state.blocks().hasIndexBlock(blockedIndex.getName(), MetaDataIndexStateService.INDEX_CLOSED_BLOCK), is(true));
                assertThat("Index must have only 1 block with [id=" + MetaDataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
                    state.blocks().indices().getOrDefault(blockedIndex.getName(), emptySet()).stream()
                        .filter(clusterBlock -> clusterBlock.id() == MetaDataIndexStateService.INDEX_CLOSED_BLOCK_ID).count(), equalTo(1L));
                assertThat("Index routing table should have been removed when closing the index on mixed cluster version",
                    state.routingTable().index(blockedIndex), nullValue());
            } else {
                assertIsOpened(blockedIndex.getName(), state);
                assertThat(state.blocks().hasIndexBlock(blockedIndex.getName(), closingBlock), is(true));
            }
        }
    }

    public void testAddIndexClosedBlocks() {
        final ClusterState initialState = ClusterState.builder(new ClusterName("testAddIndexClosedBlocks")).build();
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            Index[] indices = new Index[]{new Index("_name", "_uid")};
            expectThrows(IndexNotFoundException.class, () ->
                MetaDataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, initialState));
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            Index[] indices = Index.EMPTY_ARRAY;

            ClusterState updatedState = MetaDataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, initialState);
            assertSame(initialState, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addClosedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            Index[] indices = new Index[]{state.metaData().index("closed").getIndex()};

            ClusterState updatedState = MetaDataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
            assertSame(state, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addClosedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            Index[] indices = new Index[]{state.metaData().index("opened").getIndex(), state.metaData().index("closed").getIndex()};

            ClusterState updatedState = MetaDataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
            assertNotSame(state, updatedState);

            Index opened = updatedState.metaData().index("opened").getIndex();
            assertTrue(blockedIndices.containsKey(opened));
            assertHasBlock("opened", updatedState, blockedIndices.get(opened));

            Index closed = updatedState.metaData().index("closed").getIndex();
            assertFalse(blockedIndices.containsKey(closed));
        }
        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
                    ClusterState state = addRestoredIndex("restored", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
                    if (randomBoolean()) {
                        state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                    }
                    if (randomBoolean()) {
                        state = addOpenedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                    }
                    Index[] indices = new Index[]{state.metaData().index("restored").getIndex()};
                    MetaDataIndexStateService.addIndexClosedBlocks(indices, unmodifiableMap(emptyMap()), state);
                });
            assertThat(exception.getMessage(), containsString("Cannot close indices that are being restored: [[restored]]"));
        }
        {
            SnapshotInProgressException exception = expectThrows(SnapshotInProgressException.class, () -> {
                ClusterState state = addSnapshotIndex("snapshotted", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
                if (randomBoolean()) {
                    state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                if (randomBoolean()) {
                    state = addOpenedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                Index[] indices = new Index[]{state.metaData().index("snapshotted").getIndex()};
                MetaDataIndexStateService.addIndexClosedBlocks(indices, unmodifiableMap(emptyMap()), state);
            });
            assertThat(exception.getMessage(), containsString("Cannot close indices that are being snapshotted: [[snapshotted]]"));
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addOpenedIndex("index-1", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex("index-2", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            state = addOpenedIndex("index-3", randomIntBetween(1, 3), randomIntBetween(0, 3), state);

            Index index1 = state.metaData().index("index-1").getIndex();
            Index index2 = state.metaData().index("index-2").getIndex();
            Index index3 = state.metaData().index("index-3").getIndex();
            Index[] indices = new Index[]{index1, index2, index3};

            ClusterState updatedState = MetaDataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
            assertNotSame(state, updatedState);

            for (Index index : indices) {
                assertTrue(blockedIndices.containsKey(index));
                assertHasBlock(index.getName(), updatedState, blockedIndices.get(index));
            }
        }
    }

    public void testAddIndexClosedBlocksReusesBlocks() {
        ClusterState state = ClusterState.builder(new ClusterName("testAddIndexClosedBlocksReuseBlocks")).build();
        state = addOpenedIndex("test", randomIntBetween(1, 3), randomIntBetween(0, 3), state);

        Index test = state.metaData().index("test").getIndex();
        Index[] indices = new Index[]{test};

        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        state = MetaDataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);

        assertTrue(blockedIndices.containsKey(test));
        assertHasBlock(test.getName(), state, blockedIndices.get(test));

        final Map<Index, ClusterBlock> blockedIndices2 = new HashMap<>();
        state = MetaDataIndexStateService.addIndexClosedBlocks(indices, blockedIndices2, state);

        assertTrue(blockedIndices2.containsKey(test));
        assertHasBlock(test.getName(), state, blockedIndices2.get(test));
        assertEquals(blockedIndices.get(test), blockedIndices2.get(test));
    }

    public void testValidateShardLimit() {
        int nodesInCluster = randomIntBetween(2, 90);
        ClusterShardLimitIT.ShardCounts counts = forDataNodeCount(nodesInCluster);
        Settings clusterSettings = Settings.builder()
            .put(MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), counts.getShardsPerNode())
            .build();
        ClusterState state = createClusterForShardLimitTest(nodesInCluster, counts.getFirstIndexShards(), counts.getFirstIndexReplicas(),
            counts.getFailingIndexShards(), counts.getFailingIndexReplicas(), clusterSettings);

        Index[] indices = Arrays.stream(state.metaData().indices().values().toArray(IndexMetaData.class))
            .map(IndexMetaData::getIndex)
            .collect(Collectors.toList())
            .toArray(new Index[2]);

        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        ValidationException exception = expectThrows(ValidationException.class,
            () -> MetaDataIndexStateService.validateShardLimit(state, indices));
        assertEquals("Validation Failed: 1: this action would add [" + totalShards + "] total shards, but this cluster currently has [" +
            currentShards + "]/[" + maxShards + "] maximum shards open;", exception.getMessage());
    }

    public void testIsIndexVerifiedBeforeClosed() {
        final ClusterState initialState = ClusterState.builder(new ClusterName("testIsIndexMetaDataClosed")).build();
        {
            String indexName = "open";
            ClusterState state = addOpenedIndex(indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            assertFalse(MetaDataIndexStateService.isIndexVerifiedBeforeClosed(state.getMetaData().index(indexName)));
        }
        {
            String indexName = "closed";
            ClusterState state = addClosedIndex(indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            assertTrue(MetaDataIndexStateService.isIndexVerifiedBeforeClosed(state.getMetaData().index(indexName)));
        }
        {
            String indexName = "closed-no-setting";
            IndexMetaData indexMetaData = IndexMetaData.builder(indexName)
                .state(IndexMetaData.State.CLOSE)
                .creationDate(randomNonNegativeLong())
                .settings(Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
                    .put(SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 3)))
                .build();
            assertFalse(MetaDataIndexStateService.isIndexVerifiedBeforeClosed(indexMetaData));
        }
    }

    public void testCloseFailedIfBlockDisappeared() {
        ClusterState state = ClusterState.builder(new ClusterName("failedIfBlockDisappeared")).build();
        Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        int numIndices = between(1, 10);
        Set<Index> disappearedIndices = new HashSet<>();
        Map<Index, IndexResult> verifyResults = new HashMap<>();
        for (int i = 0; i < numIndices; i++) {
            String indexName = "test-" + i;
            state = addOpenedIndex(indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            Index index = state.metaData().index(indexName).getIndex();
            state = MetaDataIndexStateService.addIndexClosedBlocks(new Index[]{index}, blockedIndices, state);
            if (randomBoolean()) {
                state = ClusterState.builder(state)
                    .blocks(ClusterBlocks.builder().blocks(state.blocks()).removeIndexBlocks(indexName).build())
                    .build();
                disappearedIndices.add(index);
            }
            verifyResults.put(index, new IndexResult(index));
        }
        Collection<IndexResult> closingResults =
            MetaDataIndexStateService.closeRoutingTable(state, blockedIndices, unmodifiableMap(verifyResults)).v2();
        assertThat(closingResults, hasSize(numIndices));
        Set<Index> failedIndices = closingResults.stream().filter(IndexResult::hasFailures)
            .map(IndexResult::getIndex).collect(Collectors.toSet());
        assertThat(failedIndices, equalTo(disappearedIndices));
    }

    public static ClusterState createClusterForShardLimitTest(int nodesInCluster, int openIndexShards, int openIndexReplicas,
                                                              int closedIndexShards, int closedIndexReplicas, Settings clusterSettings) {
        ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodes = ImmutableOpenMap.builder();
        for (int i = 0; i < nodesInCluster; i++) {
            dataNodes.put(randomAlphaOfLengthBetween(5, 15), mock(DiscoveryNode.class));
        }
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getDataNodes()).thenReturn(dataNodes.build());

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        state = addOpenedIndex(randomAlphaOfLengthBetween(5, 15), openIndexShards, openIndexReplicas, state);
        state = addClosedIndex(randomAlphaOfLengthBetween(5, 15), closedIndexShards, closedIndexReplicas, state);

        final MetaData.Builder metaData = MetaData.builder(state.metaData());
        if (randomBoolean()) {
            metaData.persistentSettings(clusterSettings);
        } else {
            metaData.transientSettings(clusterSettings);
        }
        return ClusterState.builder(state).metaData(metaData).nodes(nodes).build();
    }

    private static ClusterState addOpenedIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        return addIndex(state, index, numShards, numReplicas, IndexMetaData.State.OPEN, null);
    }

    private static ClusterState addClosedIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        return addIndex(state, index, numShards, numReplicas, IndexMetaData.State.CLOSE, INDEX_CLOSED_BLOCK);
    }

    private static ClusterState addBlockedIndex(final String index, final int numShards, final int numReplicas, final ClusterState state,
                                                final ClusterBlock closingBlock) {
        return addIndex(state, index, numShards, numReplicas, IndexMetaData.State.OPEN, closingBlock);
    }

    private static ClusterState addRestoredIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        ClusterState newState = addOpenedIndex(index, numShards, numReplicas, state);

        final ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder = ImmutableOpenMap.builder();
        for (ShardRouting shardRouting : newState.routingTable().index(index).randomAllActiveShardsIt()) {
            shardsBuilder.put(shardRouting.shardId(), new RestoreInProgress.ShardRestoreStatus(shardRouting.currentNodeId()));
        }

        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        final RestoreInProgress.Entry entry =
            new RestoreInProgress.Entry("_uuid", snapshot, RestoreInProgress.State.INIT,
                Collections.singletonList(index), shardsBuilder.build());
        return ClusterState.builder(newState)
            .putCustom(RestoreInProgress.TYPE, new RestoreInProgress.Builder().add(entry).build())
            .build();
    }

    private static ClusterState addSnapshotIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        ClusterState newState = addOpenedIndex(index, numShards, numReplicas, state);

        final ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsBuilder = ImmutableOpenMap.builder();
        for (ShardRouting shardRouting : newState.routingTable().index(index).randomAllActiveShardsIt()) {
            shardsBuilder.put(shardRouting.shardId(), new SnapshotsInProgress.ShardSnapshotStatus(shardRouting.currentNodeId(), "1"));
        }

        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        final SnapshotsInProgress.Entry entry =
            new SnapshotsInProgress.Entry(snapshot, randomBoolean(), false, SnapshotsInProgress.State.INIT,
                Collections.singletonList(new IndexId(index, index)), randomNonNegativeLong(), randomLong(), shardsBuilder.build(),
                SnapshotInfoTests.randomUserMetadata(), randomBoolean());
        return ClusterState.builder(newState).putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(entry)).build();
    }

    private static ClusterState addIndex(final ClusterState currentState,
                                         final String index,
                                         final int numShards,
                                         final int numReplicas,
                                         final IndexMetaData.State state,
                                         @Nullable final ClusterBlock block) {

        final Settings.Builder settings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, numShards)
            .put(SETTING_NUMBER_OF_REPLICAS, numReplicas);
        if (state == IndexMetaData.State.CLOSE) {
            settings.put(MetaDataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true);
        }
        final IndexMetaData indexMetaData = IndexMetaData.builder(index)
            .state(state)
            .creationDate(randomNonNegativeLong())
            .settings(settings)
            .build();

        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(currentState);
        clusterStateBuilder.metaData(MetaData.builder(currentState.metaData()).put(indexMetaData, true));

        final IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetaData.getIndex());
        for (int j = 0; j < indexMetaData.getNumberOfShards(); j++) {
            ShardId shardId = new ShardId(indexMetaData.getIndex(), j);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingBuilder.addShard(newShardRouting(shardId, randomAlphaOfLength(10), true, ShardRoutingState.STARTED));
            for (int k = 0; k < indexMetaData.getNumberOfReplicas(); k++) {
                indexShardRoutingBuilder.addShard(newShardRouting(shardId, randomAlphaOfLength(10), false, ShardRoutingState.STARTED));
            }
            indexRoutingTable.addIndexShard(indexShardRoutingBuilder.build());
        }
        clusterStateBuilder.routingTable(RoutingTable.builder(currentState.routingTable()).add(indexRoutingTable).build());

        if (block != null) {
            clusterStateBuilder.blocks(ClusterBlocks.builder().blocks(currentState.blocks()).addIndexBlock(index, block));
        }
        return clusterStateBuilder.build();
    }

    private static void assertIsOpened(final String indexName, final ClusterState clusterState) {
        final IndexMetaData indexMetaData = clusterState.metaData().indices().get(indexName);
        assertThat(indexMetaData.getState(), is(IndexMetaData.State.OPEN));
        assertThat(indexMetaData.getSettings().hasValue(MetaDataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(false));
        assertThat(clusterState.routingTable().index(indexName), notNullValue());
        assertThat(clusterState.blocks().hasIndexBlock(indexName, MetaDataIndexStateService.INDEX_CLOSED_BLOCK), is(false));
        assertThat(clusterState.routingTable().index(indexName), notNullValue());
    }

    private static void assertIsClosed(final String indexName, final ClusterState clusterState) {
        final IndexMetaData indexMetaData = clusterState.metaData().indices().get(indexName);
        assertThat(indexMetaData.getState(), is(IndexMetaData.State.CLOSE));
        final Settings indexSettings = indexMetaData.getSettings();
        assertThat(indexSettings.hasValue(MetaDataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(true));
        assertThat(indexSettings.getAsBoolean(MetaDataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), false), is(true));
        assertThat(clusterState.blocks().hasIndexBlock(indexName, MetaDataIndexStateService.INDEX_CLOSED_BLOCK), is(true));
        assertThat("Index " + indexName + " must have only 1 block with [id=" + MetaDataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
            clusterState.blocks().indices().getOrDefault(indexName, emptySet()).stream()
                .filter(clusterBlock -> clusterBlock.id() == MetaDataIndexStateService.INDEX_CLOSED_BLOCK_ID).count(), equalTo(1L));

        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        assertThat(indexRoutingTable, notNullValue());

        for(IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
            assertThat(shardRoutingTable.shards().stream().allMatch(ShardRouting::unassigned), is(true));
            assertThat(shardRoutingTable.shards().stream().map(ShardRouting::unassignedInfo).map(UnassignedInfo::getReason)
                .allMatch(info -> info == UnassignedInfo.Reason.INDEX_CLOSED), is(true));
        }
    }

    private static void assertHasBlock(final String indexName, final ClusterState clusterState, final ClusterBlock closingBlock) {
        assertThat(clusterState.blocks().hasIndexBlock(indexName, closingBlock), is(true));
        assertThat("Index " + indexName + " must have only 1 block with [id=" + MetaDataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
            clusterState.blocks().indices().getOrDefault(indexName, emptySet()).stream()
                .filter(clusterBlock -> clusterBlock.id() == MetaDataIndexStateService.INDEX_CLOSED_BLOCK_ID).count(), equalTo(1L));
    }
}
