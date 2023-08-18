/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetadataDeleteIndexServiceTests extends ESTestCase {
    private AllocationService allocationService;
    private MetadataDeleteIndexService service;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(ClusterState.class), any(String.class), any())).thenAnswer(
            mockInvocation -> mockInvocation.getArguments()[0]
        );
        service = new MetadataDeleteIndexService(
            Settings.EMPTY,
            ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool()),
            allocationService
        );
    }

    public void testDeleteMissing() {
        Index index = new Index("missing", "doesn't matter");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> service.deleteIndices(state, Set.of(index)));
        assertEquals(index, e.getIndex());
    }

    public void testDeleteSnapshotting() {
        String index = randomAlphaOfLength(5);
        Snapshot snapshot = new Snapshot("doesn't matter", new SnapshotId("snapshot name", "snapshot uuid"));
        SnapshotsInProgress snaps = SnapshotsInProgress.EMPTY.withAddedEntry(
            SnapshotsInProgress.Entry.snapshot(
                snapshot,
                true,
                false,
                SnapshotsInProgress.State.INIT,
                Map.of(index, new IndexId(index, "doesn't matter")),
                Collections.emptyList(),
                Collections.emptyList(),
                System.currentTimeMillis(),
                (long) randomIntBetween(0, 1000),
                Map.of(),
                null,
                SnapshotInfoTestUtils.randomUserMetadata(),
                IndexVersionUtils.randomVersion(random())
            )
        );
        ClusterState state = ClusterState.builder(clusterState(index)).putCustom(SnapshotsInProgress.TYPE, snaps).build();
        Exception e = expectThrows(
            SnapshotInProgressException.class,
            () -> service.deleteIndices(state, Set.of(state.metadata().getIndices().get(index).getIndex()))
        );
        assertEquals(
            "Cannot delete indices that are being snapshotted: [["
                + index
                + "]]. Try again after snapshot finishes "
                + "or cancel the currently running snapshot.",
            e.getMessage()
        );
    }

    public void testDeleteUnassigned() throws Exception {
        // Create an unassigned index
        String index = randomAlphaOfLength(5);
        ClusterState before = clusterState(index);

        // Mock the built reroute
        when(allocationService.reroute(any(ClusterState.class), anyString(), any())).then(i -> i.getArguments()[0]);

        // Remove it
        final ClusterState after = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            before,
            service.executor,
            List.of(
                new DeleteIndexClusterStateUpdateRequest(ActionListener.noop()).indices(
                    new Index[] { before.metadata().getIndices().get(index).getIndex() }
                )
            )
        );

        // It is gone
        assertNull(after.metadata().getIndices().get(index));
        assertNull(after.routingTable().index(index));
        assertNull(after.blocks().indices().get(index));

        // Make sure we actually attempted to reroute
        verify(allocationService).reroute(any(ClusterState.class), any(String.class), any());
    }

    public void testDeleteIndexWithAnAlias() {
        String index = randomAlphaOfLength(5);
        String alias = randomAlphaOfLength(5);

        IndexMetadata idxMetadata = IndexMetadata.builder(index)
            .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
            .putAlias(AliasMetadata.builder(alias).writeIndex(true).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(idxMetadata, false))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(idxMetadata).build())
            .blocks(ClusterBlocks.builder().addBlocks(idxMetadata))
            .build();

        ClusterState after = service.deleteIndices(before, Set.of(before.metadata().getIndices().get(index).getIndex()));

        assertNull(after.metadata().getIndices().get(index));
        assertNull(after.routingTable().index(index));
        assertNull(after.blocks().indices().get(index));
        assertNull(after.metadata().getIndicesLookup().get(alias));
        assertThat(after.metadata().aliasedIndices(alias), empty());
    }

    public void testDeleteBackingIndexForDataStream() {
        int numBackingIndices = randomIntBetween(2, 5);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        ClusterState before = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of()
        );

        int numIndexToDelete = randomIntBetween(1, numBackingIndices - 1);

        Index indexToDelete = before.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, numIndexToDelete)).getIndex();
        ClusterState after = service.deleteIndices(before, Set.of(indexToDelete));

        assertThat(after.metadata().getIndices().get(indexToDelete.getName()), nullValue());
        assertThat(after.metadata().getIndices().size(), equalTo(numBackingIndices - 1));
        assertThat(after.metadata().getIndices().get(DataStream.getDefaultBackingIndexName(dataStreamName, numIndexToDelete)), nullValue());
    }

    public void testDeleteMultipleBackingIndexForDataStream() {
        int numBackingIndices = randomIntBetween(3, 5);
        int numBackingIndicesToDelete = randomIntBetween(2, numBackingIndices - 1);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        ClusterState before = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of()
        );

        List<Integer> indexNumbersToDelete = randomSubsetOf(
            numBackingIndicesToDelete,
            IntStream.rangeClosed(1, numBackingIndices - 1).boxed().toList()
        );

        Set<Index> indicesToDelete = new HashSet<>();
        for (int k : indexNumbersToDelete) {
            indicesToDelete.add(before.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, k)).getIndex());
        }
        ClusterState after = service.deleteIndices(before, indicesToDelete);

        DataStream dataStream = after.metadata().dataStreams().get(dataStreamName);
        assertThat(dataStream, notNullValue());
        assertThat(dataStream.getIndices().size(), equalTo(numBackingIndices - indexNumbersToDelete.size()));
        for (Index i : indicesToDelete) {
            assertThat(after.metadata().getIndices().get(i.getName()), nullValue());
            assertFalse(dataStream.getIndices().contains(i));
        }
        assertThat(after.metadata().getIndices().size(), equalTo(numBackingIndices - indexNumbersToDelete.size()));
    }

    public void testDeleteCurrentWriteIndexForDataStream() {
        int numBackingIndices = randomIntBetween(1, 5);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        ClusterState before = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of()
        );

        Index indexToDelete = before.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, numBackingIndices)).getIndex();
        Exception e = expectThrows(IllegalArgumentException.class, () -> service.deleteIndices(before, Set.of(indexToDelete)));

        assertThat(
            e.getMessage(),
            containsString(
                "index [" + indexToDelete.getName() + "] is the write index for data stream [" + dataStreamName + "] and cannot be deleted"
            )
        );
    }

    private ClusterState clusterState(String index) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build())
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata))
            .build();
    }
}
