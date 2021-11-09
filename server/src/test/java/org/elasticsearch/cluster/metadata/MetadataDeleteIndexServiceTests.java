/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotDeletionsPending;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.core.IsNull;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
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
        when(allocationService.reroute(any(ClusterState.class), any(String.class))).thenAnswer(
            mockInvocation -> mockInvocation.getArguments()[0]
        );
        service = new MetadataDeleteIndexService(Settings.EMPTY, null, allocationService);
    }

    public void testDeleteMissing() {
        Index index = new Index("missing", "doesn't matter");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> service.deleteIndices(state, singleton(index)));
        assertEquals(index, e.getIndex());
    }

    public void testDeleteSnapshotting() {
        String index = randomAlphaOfLength(5);
        Snapshot snapshot = new Snapshot("doesn't matter", new SnapshotId("snapshot name", "snapshot uuid"));
        SnapshotsInProgress snaps = SnapshotsInProgress.EMPTY.withAddedEntry(
            new SnapshotsInProgress.Entry(
                snapshot,
                true,
                false,
                SnapshotsInProgress.State.INIT,
                singletonMap(index, new IndexId(index, "doesn't matter")),
                Collections.emptyList(),
                Collections.emptyList(),
                System.currentTimeMillis(),
                (long) randomIntBetween(0, 1000),
                ImmutableOpenMap.of(),
                null,
                SnapshotInfoTestUtils.randomUserMetadata(),
                VersionUtils.randomVersion(random())
            )
        );
        ClusterState state = ClusterState.builder(clusterState(index)).putCustom(SnapshotsInProgress.TYPE, snaps).build();
        Exception e = expectThrows(
            SnapshotInProgressException.class,
            () -> service.deleteIndices(state, singleton(state.metadata().getIndices().get(index).getIndex()))
        );
        assertEquals(
            "Cannot delete indices that are being snapshotted: [["
                + index
                + "]]. Try again after snapshot finishes "
                + "or cancel the currently running snapshot.",
            e.getMessage()
        );
    }

    public void testDeleteUnassigned() {
        // Create an unassigned index
        String index = randomAlphaOfLength(5);
        ClusterState before = clusterState(index);

        // Mock the built reroute
        when(allocationService.reroute(any(ClusterState.class), any(String.class))).then(i -> i.getArguments()[0]);

        // Remove it
        ClusterState after = service.deleteIndices(before, singleton(before.metadata().getIndices().get(index).getIndex()));

        // It is gone
        assertNull(after.metadata().getIndices().get(index));
        assertNull(after.routingTable().index(index));
        assertNull(after.blocks().indices().get(index));

        // Make sure we actually attempted to reroute
        verify(allocationService).reroute(any(ClusterState.class), any(String.class));
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

        assertThat(after.metadata().getIndices().get(indexToDelete.getName()), IsNull.nullValue());
        assertThat(after.metadata().getIndices().size(), equalTo(numBackingIndices - 1));
        assertThat(
            after.metadata().getIndices().get(DataStream.getDefaultBackingIndexName(dataStreamName, numIndexToDelete)),
            IsNull.nullValue()
        );
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
            IntStream.rangeClosed(1, numBackingIndices - 1).boxed().collect(Collectors.toList())
        );

        Set<Index> indicesToDelete = new HashSet<>();
        for (int k : indexNumbersToDelete) {
            indicesToDelete.add(before.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, k)).getIndex());
        }
        ClusterState after = service.deleteIndices(before, indicesToDelete);

        DataStream dataStream = after.metadata().dataStreams().get(dataStreamName);
        assertThat(dataStream, IsNull.notNullValue());
        assertThat(dataStream.getIndices().size(), equalTo(numBackingIndices - indexNumbersToDelete.size()));
        for (Index i : indicesToDelete) {
            assertThat(after.metadata().getIndices().get(i.getName()), IsNull.nullValue());
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

    public void testDeleteIndexWithSnapshotDeletion() {
        final boolean deleteSnapshot = randomBoolean();
        final IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put("index.version.created", VersionUtils.randomVersion(random()))
                    .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE)
                    .put(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY, "repo_name")
                    .put(SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY, randomBoolean() ? null : "repo_uuid")
                    .put(SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY, "snap_name")
                    .put(SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY, "snap_uuid")
                    .put(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, deleteSnapshot)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        final ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(indexMetadata, false)
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        new RepositoriesMetadata(List.of(new RepositoryMetadata("repo_name", "fs", Settings.EMPTY).withUuid("repo_uuid")))
                    )
            )
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata))
            .build();

        final ClusterState updatedState = service.deleteIndices(initialState, Set.of(indexMetadata.getIndex()));
        assertThat(updatedState.metadata().getIndices().get("test"), nullValue());
        assertThat(updatedState.blocks().indices().get("test"), nullValue());
        assertThat(updatedState.routingTable().index("test"), nullValue());

        final SnapshotDeletionsPending updatedPendingDeletions = updatedState.custom(SnapshotDeletionsPending.TYPE);
        if (deleteSnapshot) {
            assertThat(updatedPendingDeletions, notNullValue());
            assertThat(updatedPendingDeletions.isEmpty(), equalTo(false));
            assertThat(updatedPendingDeletions.contains(new SnapshotId("snap_name", "snap_uuid")), equalTo(true));
        } else {
            assertThat(updatedPendingDeletions, nullValue());
        }
    }

    public void testDeleteMultipleIndicesWithSnapshotDeletion() {
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), "fs", Settings.EMPTY);
        if (randomBoolean()) {
            repositoryMetadata = repositoryMetadata.withUuid(UUIDs.randomBase64UUID());
        }

        final Metadata.Builder metadataBuilder = Metadata.builder();
        metadataBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(repositoryMetadata)));
        final RoutingTable.Builder routingBuilder = RoutingTable.builder();

        final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        final Set<Index> indices = new HashSet<>();

        final int nbIndices = randomIntBetween(2, 10);
        for (int i = 0; i < nbIndices; i++) {
            Settings.Builder indexSettingsBuilder = Settings.builder()
                .put("index.version.created", VersionUtils.randomVersion(random()))
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE)
                .put(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, true)
                .put(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY, repositoryMetadata.name())
                .put(SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY, snapshotId.getName())
                .put(SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY, snapshotId.getUUID());
            if (randomBoolean()) {
                indexSettingsBuilder.put(SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY, repositoryMetadata.uuid());
            }
            IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10) + i)
                .settings(indexSettingsBuilder.build())
                .numberOfShards(randomIntBetween(1, 3))
                .numberOfReplicas(randomInt(1))
                .build();
            metadataBuilder.put(indexMetadata, false);
            routingBuilder.addAsNew(indexMetadata);
            indices.add(indexMetadata.getIndex());
        }

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(routingBuilder.build())
            .metadata(metadataBuilder)
            .build();

        SnapshotDeletionsPending pendingDeletions = clusterState.custom(SnapshotDeletionsPending.TYPE, SnapshotDeletionsPending.EMPTY);
        while (indices.size() > 0) {
            assertThat(pendingDeletions.isEmpty(), equalTo(true));

            List<Index> indicesToDelete = randomSubsetOf(randomIntBetween(1, Math.max(1, indices.size() - 1)), indices);
            clusterState = service.deleteIndices(clusterState, Set.copyOf(indicesToDelete));
            indicesToDelete.forEach(indices::remove);

            for (Index deletedIndex : indicesToDelete) {
                assertThat(clusterState.metadata().index(deletedIndex), nullValue());
                assertThat(clusterState.routingTable().index(deletedIndex), nullValue());
            }

            pendingDeletions = clusterState.custom(SnapshotDeletionsPending.TYPE, SnapshotDeletionsPending.EMPTY);
        }

        assertThat(pendingDeletions.isEmpty(), equalTo(false));
        assertThat(pendingDeletions.contains(snapshotId), equalTo(true));
    }

    private ClusterState clusterState(String index) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata))
            .build();
    }
}
