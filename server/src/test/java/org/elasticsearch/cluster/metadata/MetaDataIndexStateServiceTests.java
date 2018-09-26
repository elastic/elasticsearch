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

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;

public class MetaDataIndexStateServiceTests extends ESAllocationTestCase {

    private MetaDataIndexStateService service;

    @Before
    public void setUpService() {
        AllocationService allocationService = createAllocationService(Settings.EMPTY);
        service = new MetaDataIndexStateService(Settings.EMPTY, null, allocationService, null, null, null);
    }

    public void testCloseIndexNotFound() {
        final ClusterState clusterState = createClusterState();
        final Index[] indices = new Index[]{new Index("test", "_na_")};
        expectThrows(IndexNotFoundException.class, () -> service.closeIndices(clusterState, indices));
    }

    public void testCloseIndexWithNoIndices() {
        final ClusterState clusterState = createClusterState();
        assertSame(clusterState, service.closeIndices(clusterState, Index.EMPTY_ARRAY));
    }

    public void testCloseIndexWithOnGoingRestore() {
        final ClusterState initialState = createClusterState();

        final List<Index> indices = new ArrayList<>();
        for (ObjectCursor<IndexMetaData> index : initialState.metaData().indices().values()) {
            indices.add(index.value.getIndex());
        }

        final List<Index> indicesToRestore = randomSubsetOf(randomIntBetween(1, Math.max(2, indices.size())), indices);
        assertThat(indicesToRestore, not(empty()));

        Snapshot snapshot = new Snapshot("repository", new SnapshotId("snapshot", "_nan_"));
        RestoreInProgress.State state = randomFrom(RestoreInProgress.State.INIT, RestoreInProgress.State.STARTED);
        ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shards = ImmutableOpenMap.builder();
        indicesToRestore.forEach(i -> shards.put(new ShardId(i, 0), new RestoreInProgress.ShardRestoreStatus("0")));
        List<String> listOfIndices = indicesToRestore.stream().map(Index::getName).collect(toList());
        RestoreInProgress.Entry restore = new RestoreInProgress.Entry(snapshot, state, listOfIndices, shards.build());

        final ClusterState clusterState = new ClusterState.Builder(initialState)
            .putCustom(RestoreInProgress.TYPE, new RestoreInProgress(restore))
            .build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> service.closeIndices(clusterState, indicesToRestore.toArray(Index.EMPTY_ARRAY)));
        assertThat(e.getMessage(), containsString("Cannot close indices that are being restored"));
    }

    public void testCloseIndexWithOnGoingSnapshot() {
        final ClusterState initialState = createClusterState();

        final List<Index> indices = new ArrayList<>();
        for (ObjectCursor<IndexMetaData> index : initialState.metaData().indices().values()) {
            indices.add(index.value.getIndex());
        }

        final List<Index> indicesToSnapshot = randomSubsetOf(randomIntBetween(1, Math.max(2, indices.size())), indices);
        assertThat(indicesToSnapshot, not(empty()));

        Snapshot snapshot = new Snapshot("repository", new SnapshotId("snapshot", "_nan_"));
        SnapshotsInProgress.State state = randomFrom(SnapshotsInProgress.State.INIT, SnapshotsInProgress.State.STARTED);
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
        indicesToSnapshot.forEach(i -> shards.put(new ShardId(i, 0), new SnapshotsInProgress.ShardSnapshotStatus("0")));
        List<IndexId> listOfIndices = indicesToSnapshot.stream().map(i -> new IndexId(i.getName(), i.getUUID())).collect(toList());
        SnapshotsInProgress.Entry entry =
            new SnapshotsInProgress.Entry(snapshot, true, false, state, listOfIndices, 0L, 0L, shards.build());

        final ClusterState clusterState = new ClusterState.Builder(initialState)
            .putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(entry))
            .build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> service.closeIndices(clusterState, indicesToSnapshot.toArray(Index.EMPTY_ARRAY)));
        assertThat(e.getMessage(), containsString("Cannot close indices that are being snapshotted"));
    }

    public void testCloseIndices() {
        final ClusterState initialState = createClusterState();

        final List<Index> indices = new ArrayList<>();
        for (ObjectCursor<IndexMetaData> index : initialState.metaData().indices().values()) {
            indices.add(index.value.getIndex());
        }

        final Index[] indicesToClose = randomSubsetOf(randomIntBetween(1, Math.max(2, indices.size())), indices).toArray(Index.EMPTY_ARRAY);
        assertThat(indicesToClose, not(emptyArray()));

        final ClusterState updatedClusterState = service.closeIndices(initialState, indicesToClose);
        for (Index closedIndex : indicesToClose) {
            assertEquals(IndexMetaData.State.CLOSE, updatedClusterState.metaData().index(closedIndex).getState());
            assertTrue(updatedClusterState.blocks().hasIndexBlock(closedIndex.getName(), MetaDataIndexStateService.INDEX_CLOSED_BLOCK));

            IndexRoutingTable routingTable = updatedClusterState.routingTable().index(closedIndex);
            for (IntObjectCursor<IndexShardRoutingTable> cursor : routingTable.shards()) {
                for (ShardRouting shardRouting : cursor.value.shards()) {
                    assertEquals(ShardRoutingState.UNASSIGNED, shardRouting.state());
                    assertEquals(UnassignedInfo.Reason.INDEX_CLOSED, shardRouting.unassignedInfo().getReason());
                }
            }
        }
    }

    private ClusterState createClusterState() {
        final int nbIndices = randomIntBetween(2, 10);
        final int numberOfShards = randomIntBetween(1, 5);
        final int numberOfReplicas = randomBoolean() ? 0 : randomIntBetween(1, 3);

        int numberOfDataNodes = numberOfReplicas + 1;
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfDataNodes; i++) {
            final DiscoveryNode node = newNode(Integer.toString(i), Collections.singleton(DiscoveryNode.Role.DATA));
            discoBuilder = discoBuilder.add(node);
        }

        final DiscoveryNode master = newNode(Integer.toString(numberOfDataNodes), Collections.singleton(DiscoveryNode.Role.MASTER));
        discoBuilder.add(master);
        discoBuilder.localNodeId(master.getId());
        discoBuilder.masterNodeId(master.getId());

        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        final MetaData.Builder metadataBuilder = MetaData.builder();
        for (int i = 0; i < nbIndices; i++) {
            final String index = "index-" + i;
            IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(index)
                .settings(Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis()));

            final List<IndexShardRoutingTable> indexShardRoutingTables = new ArrayList<>();

            final String primaryNode = newNode(Integer.toString(0)).getId();
            for (int primary = 0; primary < numberOfShards; primary++) {
                final ShardId shardId = new ShardId(index, "_na_", primary);

                final List<ShardRouting> shards = new ArrayList<>(numberOfReplicas + 1);
                ShardRouting primaryShard = newShardRouting(index, primary, primaryNode, null, true, ShardRoutingState.STARTED);
                shards.add(primaryShard);

                for (int replica = 0; replica < numberOfReplicas; replica++) {
                    String replicaNodeId = newNode(Integer.toString(replica + 1)).getId();
                    shards.add(newShardRouting(index, primary, replicaNodeId, null, false, ShardRoutingState.STARTED));
                }
                indexMetaDataBuilder.putInSyncAllocationIds(primary,
                    shards.stream().map(r -> r.allocationId().getId()).collect(Collectors.toSet()));

                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
                shards.forEach(indexShardRoutingBuilder::addShard);
                indexShardRoutingTables.add(indexShardRoutingBuilder.build());
            }

            final IndexMetaData indexMetaData = indexMetaDataBuilder.build();
            metadataBuilder.put(indexMetaData, false).generateClusterUuidIfNeeded();

            final IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetaData.getIndex());
            indexShardRoutingTables.forEach(indexRoutingTableBuilder::addIndexShard);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        return ClusterState.builder(new ClusterName("test"))
            .nodes(discoBuilder)
            .metaData(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();
    }
}
