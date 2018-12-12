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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
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
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.shards.ClusterShardLimitIT.ShardCounts.forDataNodeCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaDataIndexStateServiceTests extends ESTestCase {

    public void testCloseRoutingTable() {
        final Set<Index> nonBlockedIndices = new HashSet<>();
        final Map<Index, AcknowledgedResponse> blockedIndices = new HashMap<>();

        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTable")).build();
        for (int i = 0; i < randomIntBetween(1, 25); i++) {
            final String indexName = randomAlphaOfLengthBetween(5, 15);

            if (randomBoolean()) {
                state = addOpenedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
                nonBlockedIndices.add(state.metaData().index(indexName).getIndex());
            } else {
                state = addBlockedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
                blockedIndices.put(state.metaData().index(indexName).getIndex(), new AcknowledgedResponse(randomBoolean()));
            }
        }

        final ClusterState updatedState = MetaDataIndexStateService.closeRoutingTable(state, blockedIndices);
        assertThat(updatedState.metaData().indices().size(), equalTo(nonBlockedIndices.size() + blockedIndices.size()));

        for (Index nonBlockedIndex : nonBlockedIndices) {
            assertIsOpened(nonBlockedIndex.getName(), updatedState);
        }
        for (Map.Entry<Index, AcknowledgedResponse> blockedIndex : blockedIndices.entrySet()) {
            if (blockedIndex.getValue().isAcknowledged()) {
                assertIsClosed(blockedIndex.getKey().getName(), updatedState);
            } else {
                assertIsOpened(blockedIndex.getKey().getName(), updatedState);
            }
        }
    }

    public void testAddIndexClosedBlocks() {
        final ClusterState initialState = ClusterState.builder(new ClusterName("testAddIndexClosedBlocks")).build();
        {
            final Set<Index> blockedIndices = new HashSet<>();
            expectThrows(IndexNotFoundException.class, () ->
                MetaDataIndexStateService.addIndexClosedBlocks(new Index[]{new Index("_name", "_uid")}, initialState, blockedIndices));
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Set<Index> blockedIndices = new HashSet<>();
            Index[] indices = Index.EMPTY_ARRAY;

            ClusterState updatedState = MetaDataIndexStateService.addIndexClosedBlocks(indices, initialState, blockedIndices);
            assertSame(initialState, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Set<Index> blockedIndices = new HashSet<>();
            ClusterState state = addClosedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            Index[] indices = new Index[]{state.metaData().index("closed").getIndex()};

            ClusterState updatedState = MetaDataIndexStateService.addIndexClosedBlocks(indices, state, blockedIndices);
            assertSame(state, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Set<Index> blockedIndices = new HashSet<>();
            ClusterState state = addClosedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            Index[] indices = new Index[]{state.metaData().index("opened").getIndex(), state.metaData().index("closed").getIndex()};

            ClusterState updatedState = MetaDataIndexStateService.addIndexClosedBlocks(indices, state, blockedIndices);
            assertNotSame(state, updatedState);
            assertTrue(blockedIndices.contains(updatedState.metaData().index("opened").getIndex()));
            assertFalse(blockedIndices.contains(updatedState.metaData().index("closed").getIndex()));
            assertIsBlocked("opened", updatedState, true);
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
                    MetaDataIndexStateService.addIndexClosedBlocks(indices, state, new HashSet<>());
                });
            assertThat(exception.getMessage(), containsString("Cannot close indices that are being restored: [[restored]]"));
        }
        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
                ClusterState state = addSnapshotIndex("snapshotted", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
                if (randomBoolean()) {
                    state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                if (randomBoolean()) {
                    state = addOpenedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                Index[] indices = new Index[]{state.metaData().index("snapshotted").getIndex()};
                MetaDataIndexStateService.addIndexClosedBlocks(indices, state, new HashSet<>());
            });
            assertThat(exception.getMessage(), containsString("Cannot close indices that are being snapshotted: [[snapshotted]]"));
        }
        {
            final Set<Index> blockedIndices = new HashSet<>();
            ClusterState state = addOpenedIndex("index-1", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex("index-2", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            state = addOpenedIndex("index-3", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            final boolean mixedVersions = randomBoolean();
            if (mixedVersions) {
                state = ClusterState.builder(state)
                    .nodes(DiscoveryNodes.builder(state.nodes())
                        .add(new DiscoveryNode("old_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
                             new HashSet<>(Arrays.asList(DiscoveryNode.Role.values())), Version.V_6_0_0)))
                    .build();
            }
            Index[] indices = new Index[]{state.metaData().index("index-1").getIndex(),
                state.metaData().index("index-2").getIndex(), state.metaData().index("index-3").getIndex()};

            ClusterState updatedState = MetaDataIndexStateService.addIndexClosedBlocks(indices, state, blockedIndices);
            assertNotSame(state, updatedState);
            assertTrue(blockedIndices.contains(updatedState.metaData().index("index-1").getIndex()));
            assertTrue(blockedIndices.contains(updatedState.metaData().index("index-2").getIndex()));
            assertTrue(blockedIndices.contains(updatedState.metaData().index("index-3").getIndex()));
            if (mixedVersions) {
                assertIsClosed("index-1", updatedState);
                assertIsClosed("index-2", updatedState);
                assertIsClosed("index-2", updatedState);
            } else {
                assertIsBlocked("index-1", updatedState, true);
                assertIsBlocked("index-2", updatedState, true);
                assertIsBlocked("index-3", updatedState, true);
            }
        }
    }

    public void testValidateShardLimit() {
        int nodesInCluster = randomIntBetween(2,100);
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
        return addIndex(state, index, numShards, numReplicas, IndexMetaData.State.CLOSE, MetaDataIndexStateService.INDEX_CLOSED_BLOCK);
    }

    private static ClusterState addBlockedIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        return addIndex(state, index, numShards, numReplicas, IndexMetaData.State.OPEN, MetaDataIndexStateService.INDEX_CLOSED_BLOCK);
    }

    private static ClusterState addRestoredIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        ClusterState newState = addOpenedIndex(index, numShards, numReplicas, state);

        final ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder = ImmutableOpenMap.builder();
        for (ShardRouting shardRouting : newState.routingTable().index(index).randomAllActiveShardsIt()) {
            shardsBuilder.put(shardRouting.shardId(), new RestoreInProgress.ShardRestoreStatus(shardRouting.currentNodeId()));
        }

        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        final RestoreInProgress.Entry entry =
            new RestoreInProgress.Entry(snapshot, RestoreInProgress.State.INIT, Collections.singletonList(index), shardsBuilder.build());
        return ClusterState.builder(newState).putCustom(RestoreInProgress.TYPE, new RestoreInProgress(entry)).build();
    }

    private static ClusterState addSnapshotIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        ClusterState newState = addOpenedIndex(index, numShards, numReplicas, state);

        final ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsBuilder = ImmutableOpenMap.builder();
        for (ShardRouting shardRouting : newState.routingTable().index(index).randomAllActiveShardsIt()) {
            shardsBuilder.put(shardRouting.shardId(), new SnapshotsInProgress.ShardSnapshotStatus(shardRouting.currentNodeId()));
        }

        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        final SnapshotsInProgress.Entry entry =
            new SnapshotsInProgress.Entry(snapshot, randomBoolean(), false, SnapshotsInProgress.State.INIT,
                Collections.singletonList(new IndexId(index, index)), randomNonNegativeLong(), randomLong(), shardsBuilder.build());
        return ClusterState.builder(newState).putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(entry)).build();
    }

    private static ClusterState addIndex(final ClusterState currentState,
                                         final String index,
                                         final int numShards,
                                         final int numReplicas,
                                         final IndexMetaData.State state,
                                         @Nullable final ClusterBlock block) {
        final IndexMetaData indexMetaData = IndexMetaData.builder(index)
            .state(state)
            .creationDate(randomNonNegativeLong())
            .settings(Settings.builder()
                .put(SETTING_VERSION_CREATED, Version.CURRENT)
                .put(SETTING_NUMBER_OF_SHARDS, numShards)
                .put(SETTING_NUMBER_OF_REPLICAS, numReplicas))
            .build();

        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(currentState);
        clusterStateBuilder.metaData(MetaData.builder(currentState.metaData()).put(indexMetaData, true));

        if (state == IndexMetaData.State.OPEN) {
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
        }
        if (block != null) {
            clusterStateBuilder.blocks(ClusterBlocks.builder().blocks(currentState.blocks()).addIndexBlock(index, block));
        }
        return clusterStateBuilder.build();
    }

    private static void assertIsOpened(final String indexName, final ClusterState clusterState) {
        assertThat(clusterState.metaData().index(indexName).getState(), is(IndexMetaData.State.OPEN));
        assertThat(clusterState.routingTable().index(indexName), notNullValue());
        assertIsBlocked(indexName, clusterState, false);
    }

    private static void assertIsClosed(final String indexName, final ClusterState clusterState) {
        assertThat(clusterState.metaData().index(indexName).getState(), is(IndexMetaData.State.CLOSE));
        assertThat(clusterState.routingTable().index(indexName), nullValue());
        assertIsBlocked(indexName, clusterState, true);
    }

    private static void assertIsBlocked(final String indexName, final ClusterState clusterState, final boolean blocked) {
        assertThat(clusterState.blocks().hasIndexBlock(indexName, MetaDataIndexStateService.INDEX_CLOSED_BLOCK), is(blocked));
    }
}
