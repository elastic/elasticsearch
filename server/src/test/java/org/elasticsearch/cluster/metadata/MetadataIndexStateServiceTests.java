/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

    private ProjectId projectId;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        projectId = randomProjectIdOrDefault();
    }

    public void testCloseRoutingTable() {
        final Set<Index> nonBlockedIndices = new HashSet<>();
        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        final Map<Index, IndexResult> results = new HashMap<>();

        ClusterState state = stateWithProject("testCloseRoutingTable", projectId);
        for (int i = 0; i < randomIntBetween(1, 25); i++) {
            final String indexName = "index-" + i;

            if (randomBoolean()) {
                state = addOpenedIndex(projectId, indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
                nonBlockedIndices.add(state.metadata().getProject(projectId).index(indexName).getIndex());
            } else {
                final ClusterBlock closingBlock = MetadataIndexStateService.createIndexClosingBlock();
                state = addBlockedIndex(projectId, indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state, closingBlock);
                final Index index = state.metadata().getProject(projectId).index(indexName).getIndex();
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
            projectId,
            blockedIndices,
            results,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        ).v1();
        assertThat(
            updatedState.metadata().getProject(projectId).indices().size(),
            equalTo(nonBlockedIndices.size() + blockedIndices.size())
        );

        for (Index nonBlockedIndex : nonBlockedIndices) {
            assertIsOpened(nonBlockedIndex.getName(), updatedState, projectId);
            assertThat(updatedState.blocks().hasIndexBlockWithId(projectId, nonBlockedIndex.getName(), INDEX_CLOSED_BLOCK_ID), is(false));
        }
        for (Index blockedIndex : blockedIndices.keySet()) {
            if (results.get(blockedIndex).hasFailures() == false) {
                assertIsClosed(blockedIndex.getName(), updatedState, projectId);
            } else {
                assertIsOpened(blockedIndex.getName(), updatedState, projectId);
                assertThat(updatedState.blocks().hasIndexBlockWithId(projectId, blockedIndex.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
            }
        }
    }

    public void testCloseRoutingTableWithRestoredIndex() {
        ClusterState state = stateWithProject("testCloseRoutingTableWithRestoredIndex", projectId);

        String indexName = "restored-index";
        ClusterBlock block = MetadataIndexStateService.createIndexClosingBlock();
        state = addRestoredIndex(projectId, indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
        state = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().blocks(state.blocks()).addIndexBlock(projectId, indexName, block))
            .build();

        final Index index = state.metadata().getProject(projectId).index(indexName).getIndex();
        final ClusterState updatedState = MetadataIndexStateService.closeRoutingTable(
            state,
            projectId,
            Map.of(index, block),
            Map.of(index, new IndexResult(index)),
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        ).v1();
        assertIsOpened(index.getName(), updatedState, projectId);
        assertThat(updatedState.blocks().hasIndexBlockWithId(projectId, index.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
    }

    public void testCloseRoutingTableWithSnapshottedIndex() {
        ClusterState state = stateWithProject("testCloseRoutingTableWithSnapshottedIndex", projectId);

        String indexName = "snapshotted-index";
        ClusterBlock block = MetadataIndexStateService.createIndexClosingBlock();
        state = addSnapshotIndex(projectId, indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
        state = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().blocks(state.blocks()).addIndexBlock(projectId, indexName, block))
            .build();

        final Index index = state.metadata().getProject(projectId).index(indexName).getIndex();
        final ClusterState updatedState = MetadataIndexStateService.closeRoutingTable(
            state,
            projectId,
            Map.of(index, block),
            Map.of(index, new IndexResult(index)),
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        ).v1();
        assertIsOpened(index.getName(), updatedState, projectId);
        assertThat(updatedState.blocks().hasIndexBlockWithId(projectId, index.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
    }

    public void testCloseRoutingTableWithReshardingIndex() {
        ClusterState state = stateWithProject("testCloseRoutingTableWithReshardingIndex", projectId);

        String indexName = "resharding-index";
        ClusterBlock block = MetadataIndexStateService.createIndexClosingBlock();
        state = addOpenedIndex(projectId, indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);

        var updatedMetadata = IndexMetadata.builder(state.metadata().getProject(projectId).index(indexName))
            .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(randomIntBetween(1, 5), 2))
            .build();
        state = ClusterState.builder(state)
            .putProjectMetadata(ProjectMetadata.builder(state.metadata().getProject(projectId)).put(updatedMetadata, true))
            .build();

        state = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().blocks(state.blocks()).addIndexBlock(projectId, indexName, block))
            .build();

        final Index index = state.metadata().getProject(projectId).index(indexName).getIndex();
        final Tuple<ClusterState, List<IndexResult>> result = MetadataIndexStateService.closeRoutingTable(
            state,
            projectId,
            Map.of(index, block),
            Map.of(index, new IndexResult(index)),
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        final ClusterState updatedState = result.v1();
        assertIsOpened(index.getName(), updatedState, projectId);
        assertThat(updatedState.blocks().hasIndexBlockWithId(projectId, index.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
        assertThat(result.v2().get(0).getException(), notNullValue());
    }

    public void testAddIndexClosedBlocks() {
        final ClusterState initialState = stateWithProject("testAddIndexClosedBlocks", projectId);
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            Index[] indices = new Index[] { new Index("_name", "_uid") };
            expectThrows(
                IndexNotFoundException.class,
                () -> MetadataIndexStateService.addIndexClosedBlocks(projectId, indices, blockedIndices, initialState)
            );
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            Index[] indices = Index.EMPTY_ARRAY;

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(projectId, indices, blockedIndices, initialState);
            assertSame(initialState, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addClosedIndex(projectId, "closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            Index[] indices = new Index[] { state.metadata().getProject(projectId).index("closed").getIndex() };

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(projectId, indices, blockedIndices, state);
            assertSame(state, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addClosedIndex(projectId, "closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex(projectId, "opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            Index[] indices = new Index[] {
                state.metadata().getProject(projectId).index("opened").getIndex(),
                state.metadata().getProject(projectId).index("closed").getIndex() };

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(projectId, indices, blockedIndices, state);
            assertNotSame(state, updatedState);

            Index opened = updatedState.metadata().getProject(projectId).index("opened").getIndex();
            assertTrue(blockedIndices.containsKey(opened));
            assertHasBlock(projectId, "opened", updatedState, blockedIndices.get(opened));

            Index closed = updatedState.metadata().getProject(projectId).index("closed").getIndex();
            assertFalse(blockedIndices.containsKey(closed));
        }
        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
                ClusterState state = addRestoredIndex(projectId, "restored", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
                if (randomBoolean()) {
                    state = addOpenedIndex(projectId, "opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                if (randomBoolean()) {
                    state = addOpenedIndex(projectId, "closed", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                Index[] indices = new Index[] { state.metadata().getProject(projectId).index("restored").getIndex() };
                MetadataIndexStateService.addIndexClosedBlocks(projectId, indices, Map.of(), state);
            });
            assertThat(exception.getMessage(), containsString("Cannot close indices that are being restored: [[restored]]"));
        }
        {
            SnapshotInProgressException exception = expectThrows(SnapshotInProgressException.class, () -> {
                ClusterState state = addSnapshotIndex(
                    projectId,
                    "snapshotted",
                    randomIntBetween(1, 3),
                    randomIntBetween(0, 3),
                    initialState
                );
                if (randomBoolean()) {
                    state = addOpenedIndex(projectId, "opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                if (randomBoolean()) {
                    state = addOpenedIndex(projectId, "closed", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                Index[] indices = new Index[] { state.metadata().getProject(projectId).index("snapshotted").getIndex() };
                MetadataIndexStateService.addIndexClosedBlocks(projectId, indices, Map.of(), state);
            });
            assertThat(exception.getMessage(), containsString("Cannot close indices that are being snapshotted: [[snapshotted]]"));
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addOpenedIndex(projectId, "index-1", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex(projectId, "index-2", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            state = addOpenedIndex(projectId, "index-3", randomIntBetween(1, 3), randomIntBetween(0, 3), state);

            Index index1 = state.metadata().getProject(projectId).index("index-1").getIndex();
            Index index2 = state.metadata().getProject(projectId).index("index-2").getIndex();
            Index index3 = state.metadata().getProject(projectId).index("index-3").getIndex();
            Index[] indices = new Index[] { index1, index2, index3 };

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(projectId, indices, blockedIndices, state);
            assertNotSame(state, updatedState);

            for (Index index : indices) {
                assertTrue(blockedIndices.containsKey(index));
                assertHasBlock(projectId, index.getName(), updatedState, blockedIndices.get(index));
            }
        }
    }

    public void testAddIndexClosedBlocksReusesBlocks() {
        ClusterState state = stateWithProject("testAddIndexClosedBlocksReuseBlocks", projectId);
        state = addOpenedIndex(projectId, "test", randomIntBetween(1, 3), randomIntBetween(0, 3), state);

        Index test = state.metadata().getProject(projectId).index("test").getIndex();
        Index[] indices = new Index[] { test };

        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        state = MetadataIndexStateService.addIndexClosedBlocks(projectId, indices, blockedIndices, state);

        assertTrue(blockedIndices.containsKey(test));
        assertHasBlock(projectId, test.getName(), state, blockedIndices.get(test));

        final Map<Index, ClusterBlock> blockedIndices2 = new HashMap<>();
        state = MetadataIndexStateService.addIndexClosedBlocks(projectId, indices, blockedIndices2, state);

        assertTrue(blockedIndices2.containsKey(test));
        assertHasBlock(projectId, test.getName(), state, blockedIndices2.get(test));
        assertEquals(blockedIndices.get(test), blockedIndices2.get(test));
    }

    public void testIsIndexVerifiedBeforeClosed() {
        final ClusterState initialState = stateWithProject("testIsIndexMetadataClosed", projectId);
        {
            String indexName = "open";
            ClusterState state = addOpenedIndex(projectId, indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            assertFalse(MetadataIndexStateService.isIndexVerifiedBeforeClosed(state.getMetadata().getProject(projectId).index(indexName)));
        }
        {
            String indexName = "closed";
            ClusterState state = addClosedIndex(projectId, indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            assertTrue(MetadataIndexStateService.isIndexVerifiedBeforeClosed(state.getMetadata().getProject(projectId).index(indexName)));
        }
        {
            String indexName = "closed-no-setting";
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .state(IndexMetadata.State.CLOSE)
                .creationDate(randomNonNegativeLong())
                .settings(indexSettings(IndexVersion.current(), randomIntBetween(1, 3), randomIntBetween(0, 3)))
                .build();
            assertFalse(MetadataIndexStateService.isIndexVerifiedBeforeClosed(indexMetadata));
        }
    }

    public void testCloseFailedIfBlockDisappeared() {
        ClusterState state = stateWithProject("failedIfBlockDisappeared", projectId);
        Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        int numIndices = between(1, 10);
        Set<Index> disappearedIndices = new HashSet<>();
        Map<Index, IndexResult> verifyResults = new HashMap<>();
        for (int i = 0; i < numIndices; i++) {
            String indexName = "test-" + i;
            state = addOpenedIndex(projectId, indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            Index index = state.metadata().getProject(projectId).index(indexName).getIndex();
            state = MetadataIndexStateService.addIndexClosedBlocks(projectId, new Index[] { index }, blockedIndices, state);
            if (randomBoolean()) {
                state = ClusterState.builder(state)
                    .blocks(ClusterBlocks.builder().blocks(state.blocks()).removeIndexBlocks(projectId, indexName).build())
                    .build();
                disappearedIndices.add(index);
            }
            verifyResults.put(index, new IndexResult(index));
        }
        Collection<IndexResult> closingResults = MetadataIndexStateService.closeRoutingTable(
            state,
            projectId,
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

    public static ClusterState addOpenedIndex(
        final ProjectId projectId,
        final String index,
        final int numShards,
        final int numReplicas,
        final ClusterState state
    ) {
        return addIndex(state, projectId, index, numShards, numReplicas, IndexMetadata.State.OPEN, null);
    }

    public static ClusterState addClosedIndex(
        final ProjectId projectId,
        final String index,
        final int numShards,
        final int numReplicas,
        final ClusterState state
    ) {
        return addIndex(state, projectId, index, numShards, numReplicas, IndexMetadata.State.CLOSE, INDEX_CLOSED_BLOCK);
    }

    private static ClusterState addBlockedIndex(
        final ProjectId projectId,
        final String index,
        final int numShards,
        final int numReplicas,
        final ClusterState state,
        final ClusterBlock closingBlock
    ) {
        return addIndex(state, projectId, index, numShards, numReplicas, IndexMetadata.State.OPEN, closingBlock);
    }

    private static ClusterState addRestoredIndex(
        final ProjectId projectId,
        final String index,
        final int numShards,
        final int numReplicas,
        final ClusterState state
    ) {
        ClusterState newState = addOpenedIndex(projectId, index, numShards, numReplicas, state);

        final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder = new HashMap<>();
        for (ShardRouting shardRouting : newState.routingTable(projectId).index(index).randomAllActiveShardsIt()) {
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

    private static ClusterState addSnapshotIndex(
        final ProjectId projectId,
        final String index,
        final int numShards,
        final int numReplicas,
        final ClusterState state
    ) {
        ClusterState newState = addOpenedIndex(projectId, index, numShards, numReplicas, state);

        final Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsBuilder = new HashMap<>();
        for (ShardRouting shardRouting : newState.routingTable(projectId).index(index).randomAllActiveShardsIt()) {
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
            SnapshotsInProgress.State.STARTED,
            Collections.singletonMap(index, new IndexId(index, index)),
            Collections.emptyList(),
            Collections.emptyList(),
            randomNonNegativeLong(),
            randomLong(),
            shardsBuilder,
            null,
            SnapshotInfoTestUtils.randomUserMetadata(),
            IndexVersionUtils.randomVersion()
        );
        return ClusterState.builder(newState).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(entry)).build();
    }

    private static ClusterState addIndex(
        final ClusterState currentState,
        final ProjectId projectId,
        final String index,
        final int numShards,
        final int numReplicas,
        final IndexMetadata.State state,
        @Nullable final ClusterBlock block
    ) {

        final Settings.Builder settings = indexSettings(IndexVersion.current(), numShards, numReplicas);
        if (state == IndexMetadata.State.CLOSE) {
            settings.put(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true);
        }
        final IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .state(state)
            .creationDate(randomNonNegativeLong())
            .settings(settings)
            .build();

        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(currentState);
        clusterStateBuilder.putProjectMetadata(
            ProjectMetadata.builder(currentState.metadata().getProject(projectId)).put(indexMetadata, true)
        );

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
        clusterStateBuilder.putRoutingTable(
            projectId,
            RoutingTable.builder(currentState.routingTable(projectId)).add(indexRoutingTable).build()
        );

        if (block != null) {
            clusterStateBuilder.blocks(ClusterBlocks.builder().blocks(currentState.blocks()).addIndexBlock(projectId, index, block));
        }
        return clusterStateBuilder.build();
    }

    private static void assertIsOpened(final String indexName, final ClusterState clusterState, final ProjectId projectId) {
        final IndexMetadata indexMetadata = clusterState.metadata().getProject(projectId).indices().get(indexName);
        assertThat(indexMetadata.getState(), is(IndexMetadata.State.OPEN));
        assertThat(indexMetadata.getSettings().hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(false));
        assertThat(clusterState.routingTable(projectId).index(indexName), notNullValue());
        assertThat(clusterState.blocks().hasIndexBlock(projectId, indexName, MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(false));
        assertThat(clusterState.routingTable(projectId).index(indexName), notNullValue());
    }

    private static void assertIsClosed(final String indexName, final ClusterState clusterState, final ProjectId projectId) {
        final IndexMetadata indexMetadata = clusterState.metadata().getProject(projectId).indices().get(indexName);
        assertThat(indexMetadata.getState(), is(IndexMetadata.State.CLOSE));
        final Settings indexSettings = indexMetadata.getSettings();
        assertThat(indexSettings.hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(true));
        assertThat(indexSettings.getAsBoolean(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), false), is(true));
        assertThat(clusterState.blocks().hasIndexBlock(projectId, indexName, MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(true));
        assertThat(
            "Index " + indexName + " must have only 1 block with [id=" + MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
            clusterState.blocks()
                .indices(projectId)
                .getOrDefault(indexName, Set.of())
                .stream()
                .filter(clusterBlock -> clusterBlock.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID)
                .count(),
            equalTo(1L)
        );

        final IndexRoutingTable indexRoutingTable = clusterState.routingTable(projectId).index(indexName);
        assertThat(indexRoutingTable, notNullValue());

        for (int i = 0; i < indexRoutingTable.size(); i++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(i);
            assertThat(RoutingNodesHelper.asStream(shardRoutingTable).allMatch(ShardRouting::unassigned), is(true));
            assertThat(
                RoutingNodesHelper.asStream(shardRoutingTable)
                    .map(ShardRouting::unassignedInfo)
                    .map(UnassignedInfo::reason)
                    .allMatch(info -> info == UnassignedInfo.Reason.INDEX_CLOSED),
                is(true)
            );
        }
    }

    private static void assertHasBlock(
        final ProjectId projectId,
        final String indexName,
        final ClusterState clusterState,
        final ClusterBlock closingBlock
    ) {
        assertThat(clusterState.blocks().hasIndexBlock(projectId, indexName, closingBlock), is(true));
        assertThat(
            "Index " + indexName + " must have only 1 block with [id=" + MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
            clusterState.blocks()
                .indices(projectId)
                .getOrDefault(indexName, Set.of())
                .stream()
                .filter(clusterBlock -> clusterBlock.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID)
                .count(),
            equalTo(1L)
        );
    }

    private static ClusterState stateWithProject(String clusterName, ProjectId projectId) {
        final ClusterState state = ClusterState.builder(new ClusterName(clusterName)).build();
        if (Metadata.DEFAULT_PROJECT_ID.equals(projectId)) {
            return state;
        }
        return ClusterState.builder(state).putProjectMetadata(ProjectMetadata.builder(projectId)).build();
    }
}
