/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse.IndexResult;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MetadataIndexStateServiceTests extends ESTestCase {

    public void testCloseRoutingTable() {
        final Set<Index> nonBlockedIndices = new HashSet<>();
        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        final Map<Index, IndexResult> results = new HashMap<>();

        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTable")).build();
        for (int i = 0; i < randomIntBetween(1, 25); i++) {
            final String indexName = "index-" + i;

            if (randomBoolean()) {
                state = addOpenedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
                nonBlockedIndices.add(state.metadata().index(indexName).getIndex());
            } else {
                final ClusterBlock closingBlock = MetadataIndexStateService.createIndexClosingBlock();
                state = addBlockedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state, closingBlock);
                final Index index = state.metadata().index(indexName).getIndex();
                blockedIndices.put(index, closingBlock);
                if (randomBoolean()) {
                    results.put(index, new CloseIndexResponse.IndexResult(index));
                } else {
                    results.put(index, new CloseIndexResponse.IndexResult(index, new Exception("test")));
                }
            }
        }

        final ClusterState updatedState = MetadataIndexStateService.closeRoutingTable(
            state,
            blockedIndices,
            results,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        ).v1();
        assertThat(updatedState.metadata().indices().size(), equalTo(nonBlockedIndices.size() + blockedIndices.size()));

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
        ClusterBlock block = MetadataIndexStateService.createIndexClosingBlock();
        state = addRestoredIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
        state = ClusterState.builder(state).blocks(ClusterBlocks.builder().blocks(state.blocks()).addIndexBlock(indexName, block)).build();

        final Index index = state.metadata().index(indexName).getIndex();
        final ClusterState updatedState = MetadataIndexStateService.closeRoutingTable(
            state,
            Map.of(index, block),
            Map.of(index, new IndexResult(index)),
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        ).v1();
        assertIsOpened(index.getName(), updatedState);
        assertThat(updatedState.blocks().hasIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
    }

    public void testCloseRoutingTableWithSnapshottedIndex() {
        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTableWithSnapshottedIndex")).build();

        String indexName = "snapshotted-index";
        ClusterBlock block = MetadataIndexStateService.createIndexClosingBlock();
        state = addSnapshotIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
        state = ClusterState.builder(state).blocks(ClusterBlocks.builder().blocks(state.blocks()).addIndexBlock(indexName, block)).build();

        final Index index = state.metadata().index(indexName).getIndex();
        final ClusterState updatedState = MetadataIndexStateService.closeRoutingTable(
            state,
            Map.of(index, block),
            Map.of(index, new IndexResult(index)),
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        ).v1();
        assertIsOpened(index.getName(), updatedState);
        assertThat(updatedState.blocks().hasIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
    }

    public void testAddIndexClosedBlocks() {
        final ClusterState initialState = ClusterState.builder(new ClusterName("testAddIndexClosedBlocks")).build();
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            Index[] indices = new Index[] { new Index("_name", "_uid") };
            expectThrows(
                IndexNotFoundException.class,
                () -> MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, initialState)
            );
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            Index[] indices = Index.EMPTY_ARRAY;

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, initialState);
            assertSame(initialState, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addClosedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            Index[] indices = new Index[] { state.metadata().index("closed").getIndex() };

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
            assertSame(state, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addClosedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            Index[] indices = new Index[] { state.metadata().index("opened").getIndex(), state.metadata().index("closed").getIndex() };

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
            assertNotSame(state, updatedState);

            Index opened = updatedState.metadata().index("opened").getIndex();
            assertTrue(blockedIndices.containsKey(opened));
            assertHasBlock("opened", updatedState, blockedIndices.get(opened));

            Index closed = updatedState.metadata().index("closed").getIndex();
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
                Index[] indices = new Index[] { state.metadata().index("restored").getIndex() };
                MetadataIndexStateService.addIndexClosedBlocks(indices, Map.of(), state);
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
                Index[] indices = new Index[] { state.metadata().index("snapshotted").getIndex() };
                MetadataIndexStateService.addIndexClosedBlocks(indices, Map.of(), state);
            });
            assertThat(exception.getMessage(), containsString("Cannot close indices that are being snapshotted: [[snapshotted]]"));
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addOpenedIndex("index-1", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex("index-2", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            state = addOpenedIndex("index-3", randomIntBetween(1, 3), randomIntBetween(0, 3), state);

            Index index1 = state.metadata().index("index-1").getIndex();
            Index index2 = state.metadata().index("index-2").getIndex();
            Index index3 = state.metadata().index("index-3").getIndex();
            Index[] indices = new Index[] { index1, index2, index3 };

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
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

        Index test = state.metadata().index("test").getIndex();
        Index[] indices = new Index[] { test };

        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        state = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);

        assertTrue(blockedIndices.containsKey(test));
        assertHasBlock(test.getName(), state, blockedIndices.get(test));

        final Map<Index, ClusterBlock> blockedIndices2 = new HashMap<>();
        state = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices2, state);

        assertTrue(blockedIndices2.containsKey(test));
        assertHasBlock(test.getName(), state, blockedIndices2.get(test));
        assertEquals(blockedIndices.get(test), blockedIndices2.get(test));
    }

    public void testIsIndexVerifiedBeforeClosed() {
        final ClusterState initialState = ClusterState.builder(new ClusterName("testIsIndexMetadataClosed")).build();
        {
            String indexName = "open";
            ClusterState state = addOpenedIndex(indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            assertFalse(MetadataIndexStateService.isIndexVerifiedBeforeClosed(state.getMetadata().index(indexName)));
        }
        {
            String indexName = "closed";
            ClusterState state = addClosedIndex(indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            assertTrue(MetadataIndexStateService.isIndexVerifiedBeforeClosed(state.getMetadata().index(indexName)));
        }
        {
            String indexName = "closed-no-setting";
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .state(IndexMetadata.State.CLOSE)
                .creationDate(randomNonNegativeLong())
                .settings(indexSettings(Version.CURRENT, randomIntBetween(1, 3), randomIntBetween(0, 3)))
                .build();
            assertFalse(MetadataIndexStateService.isIndexVerifiedBeforeClosed(indexMetadata));
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
            Index index = state.metadata().index(indexName).getIndex();
            state = MetadataIndexStateService.addIndexClosedBlocks(new Index[] { index }, blockedIndices, state);
            if (randomBoolean()) {
                state = ClusterState.builder(state)
                    .blocks(ClusterBlocks.builder().blocks(state.blocks()).removeIndexBlocks(indexName).build())
                    .build();
                disappearedIndices.add(index);
            }
            verifyResults.put(index, new IndexResult(index));
        }
        Collection<IndexResult> closingResults = MetadataIndexStateService.closeRoutingTable(
            state,
            blockedIndices,
            Map.copyOf(verifyResults),
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        ).v2();
        assertThat(closingResults, hasSize(numIndices));
        Set<Index> failedIndices = closingResults.stream()
            .filter(IndexResult::hasFailures)
            .map(IndexResult::getIndex)
            .collect(Collectors.toSet());
        assertThat(failedIndices, equalTo(disappearedIndices));
    }

    public static ClusterState addOpenedIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        return addIndex(state, index, numShards, numReplicas, IndexMetadata.State.OPEN, null);
    }

    public static ClusterState addClosedIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        return addIndex(state, index, numShards, numReplicas, IndexMetadata.State.CLOSE, INDEX_CLOSED_BLOCK);
    }

    private static ClusterState addBlockedIndex(
        final String index,
        final int numShards,
        final int numReplicas,
        final ClusterState state,
        final ClusterBlock closingBlock
    ) {
        return addIndex(state, index, numShards, numReplicas, IndexMetadata.State.OPEN, closingBlock);
    }

    private static ClusterState addRestoredIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        ClusterState newState = addOpenedIndex(index, numShards, numReplicas, state);

        final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder = new HashMap<>();
        for (ShardRouting shardRouting : newState.routingTable().index(index).randomAllActiveShardsIt()) {
            shardsBuilder.put(shardRouting.shardId(), new RestoreInProgress.ShardRestoreStatus(shardRouting.currentNodeId()));
        }

        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        final RestoreInProgress.Entry entry = new RestoreInProgress.Entry(
            "_uuid",
            snapshot,
            RestoreInProgress.State.INIT,
            false,
            Collections.singletonList(index),
            shardsBuilder
        );
        return ClusterState.builder(newState).putCustom(RestoreInProgress.TYPE, new RestoreInProgress.Builder().add(entry).build()).build();
    }

    private static ClusterState addSnapshotIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        ClusterState newState = addOpenedIndex(index, numShards, numReplicas, state);

        final Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsBuilder = new HashMap<>();
        for (ShardRouting shardRouting : newState.routingTable().index(index).randomAllActiveShardsIt()) {
            shardsBuilder.put(
                shardRouting.shardId(),
                new SnapshotsInProgress.ShardSnapshotStatus(shardRouting.currentNodeId(), new ShardGeneration(1L))
            );
        }

        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        final SnapshotsInProgress.Entry entry = SnapshotsInProgress.Entry.snapshot(
            snapshot,
            randomBoolean(),
            false,
            SnapshotsInProgress.State.INIT,
            Collections.singletonMap(index, new IndexId(index, index)),
            Collections.emptyList(),
            Collections.emptyList(),
            randomNonNegativeLong(),
            randomLong(),
            shardsBuilder,
            null,
            SnapshotInfoTestUtils.randomUserMetadata(),
            IndexVersionUtils.randomVersion(random())
        );
        return ClusterState.builder(newState).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(entry)).build();
    }

    private static ClusterState addIndex(
        final ClusterState currentState,
        final String index,
        final int numShards,
        final int numReplicas,
        final IndexMetadata.State state,
        @Nullable final ClusterBlock block
    ) {

        final Settings.Builder settings = indexSettings(Version.CURRENT, numShards, numReplicas);
        if (state == IndexMetadata.State.CLOSE) {
            settings.put(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true);
        }
        final IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .state(state)
            .creationDate(randomNonNegativeLong())
            .settings(settings)
            .build();

        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(currentState);
        clusterStateBuilder.metadata(Metadata.builder(currentState.metadata()).put(indexMetadata, true));

        final IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int j = 0; j < indexMetadata.getNumberOfShards(); j++) {
            ShardId shardId = new ShardId(indexMetadata.getIndex(), j);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingBuilder.addShard(newShardRouting(shardId, randomAlphaOfLength(10), true, ShardRoutingState.STARTED));
            for (int k = 0; k < indexMetadata.getNumberOfReplicas(); k++) {
                indexShardRoutingBuilder.addShard(newShardRouting(shardId, randomAlphaOfLength(10), false, ShardRoutingState.STARTED));
            }
            indexRoutingTable.addIndexShard(indexShardRoutingBuilder);
        }
        clusterStateBuilder.routingTable(RoutingTable.builder(currentState.routingTable()).add(indexRoutingTable).build());

        if (block != null) {
            clusterStateBuilder.blocks(ClusterBlocks.builder().blocks(currentState.blocks()).addIndexBlock(index, block));
        }
        return clusterStateBuilder.build();
    }

    private static void assertIsOpened(final String indexName, final ClusterState clusterState) {
        final IndexMetadata indexMetadata = clusterState.metadata().indices().get(indexName);
        assertThat(indexMetadata.getState(), is(IndexMetadata.State.OPEN));
        assertThat(indexMetadata.getSettings().hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(false));
        assertThat(clusterState.routingTable().index(indexName), notNullValue());
        assertThat(clusterState.blocks().hasIndexBlock(indexName, MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(false));
        assertThat(clusterState.routingTable().index(indexName), notNullValue());
    }

    private static void assertIsClosed(final String indexName, final ClusterState clusterState) {
        final IndexMetadata indexMetadata = clusterState.metadata().indices().get(indexName);
        assertThat(indexMetadata.getState(), is(IndexMetadata.State.CLOSE));
        final Settings indexSettings = indexMetadata.getSettings();
        assertThat(indexSettings.hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(true));
        assertThat(indexSettings.getAsBoolean(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), false), is(true));
        assertThat(clusterState.blocks().hasIndexBlock(indexName, MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(true));
        assertThat(
            "Index " + indexName + " must have only 1 block with [id=" + MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
            clusterState.blocks()
                .indices()
                .getOrDefault(indexName, Set.of())
                .stream()
                .filter(clusterBlock -> clusterBlock.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID)
                .count(),
            equalTo(1L)
        );

        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        assertThat(indexRoutingTable, notNullValue());

        for (int i = 0; i < indexRoutingTable.size(); i++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(i);
            assertThat(RoutingNodesHelper.asStream(shardRoutingTable).allMatch(ShardRouting::unassigned), is(true));
            assertThat(
                RoutingNodesHelper.asStream(shardRoutingTable)
                    .map(ShardRouting::unassignedInfo)
                    .map(UnassignedInfo::getReason)
                    .allMatch(info -> info == UnassignedInfo.Reason.INDEX_CLOSED),
                is(true)
            );
        }
    }

    private static void assertHasBlock(final String indexName, final ClusterState clusterState, final ClusterBlock closingBlock) {
        assertThat(clusterState.blocks().hasIndexBlock(indexName, closingBlock), is(true));
        assertThat(
            "Index " + indexName + " must have only 1 block with [id=" + MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
            clusterState.blocks()
                .indices()
                .getOrDefault(indexName, Set.of())
                .stream()
                .filter(clusterBlock -> clusterBlock.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID)
                .count(),
            equalTo(1L)
        );
    }
}
