/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
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
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> MetadataDeleteIndexService.deleteIndices(state, Set.of(index), Settings.EMPTY)
        );
        assertEquals(index, e.getIndex());
    }

    public void testDeleteSnapshotting() {
        String indexName = randomAlphaOfLength(5);
        Snapshot snapshot = new Snapshot("doesn't matter", new SnapshotId("snapshot name", "snapshot uuid"));
        SnapshotsInProgress snaps = SnapshotsInProgress.EMPTY.withAddedEntry(
            SnapshotsInProgress.Entry.snapshot(
                snapshot,
                true,
                false,
                SnapshotsInProgress.State.INIT,
                Map.of(indexName, new IndexId(indexName, "doesn't matter")),
                Collections.emptyList(),
                Collections.emptyList(),
                System.currentTimeMillis(),
                (long) randomIntBetween(0, 1000),
                Map.of(),
                null,
                SnapshotInfoTestUtils.randomUserMetadata(),
                IndexVersionUtils.randomVersion()
            )
        );
        final Index index = new Index(indexName, randomUUID());
        ClusterState state = ClusterState.builder(clusterState(index)).putCustom(SnapshotsInProgress.TYPE, snaps).build();
        Exception e = expectThrows(
            SnapshotInProgressException.class,
            () -> MetadataDeleteIndexService.deleteIndices(state, Set.of(index), Settings.EMPTY)
        );
        assertEquals(
            "Cannot delete indices that are being snapshotted: ["
                + index
                + "]. Try again after snapshot finishes "
                + "or cancel the currently running snapshot.",
            e.getMessage()
        );
    }

    public void testDeleteUnassigned() throws Exception {
        // Create an unassigned index
        String indexName = randomAlphaOfLength(5);
        Index index = new Index(indexName, randomUUID());
        ClusterState before = clusterState(index);

        final var projectId = before.metadata().projectFor(index).id();

        // Mock the built reroute
        when(allocationService.reroute(any(ClusterState.class), anyString(), any())).then(i -> i.getArguments()[0]);

        // Remove it
        final ClusterState after = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            before,
            service.executor,
            List.of(
                new MetadataDeleteIndexService.DeleteIndicesClusterStateUpdateTask(
                    Set.of(index),
                    TEST_REQUEST_TIMEOUT,
                    ActionListener.noop()
                )
            )
        );

        // It is gone
        assertThat(after.metadata().lookupProject(index), isEmpty());
        assertThat(after.metadata().getProject(projectId).indices().get(indexName), nullValue());
        assertThat(after.metadata().getProject(projectId).hasIndex(index), equalTo(false));
        assertNull(after.routingTable(projectId).index(indexName));
        assertNull(after.blocks().indices(projectId).get(indexName));

        // Make sure we actually attempted to reroute
        verify(allocationService).reroute(any(ClusterState.class), any(String.class), any());
    }

    public void testDeleteIndexWithAnAlias() {
        ProjectId projectId = randomProjectIdOrDefault();
        String index = randomAlphaOfLength(5);
        String alias = randomAlphaOfLength(5);

        IndexMetadata idxMetadata = IndexMetadata.builder(index)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersionUtils.randomVersion()))
            .putAlias(AliasMetadata.builder(alias).writeIndex(true).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(idxMetadata, false).build())
            .putRoutingTable(
                projectId,
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(idxMetadata).build()
            )
            .blocks(ClusterBlocks.builder().addBlocks(projectId, idxMetadata))
            .build();

        ClusterState after = MetadataDeleteIndexService.deleteIndices(
            before,
            Set.of(before.metadata().getProject(projectId).indices().get(index).getIndex()),
            Settings.EMPTY
        );

        assertNull(after.metadata().getProject(projectId).indices().get(index));
        assertNull(after.routingTable(projectId).index(index));
        assertNull(after.blocks().indices(projectId).get(index));
        assertNull(after.metadata().getProject(projectId).getIndicesLookup().get(alias));
        assertThat(after.metadata().getProject(projectId).aliasedIndices(alias), empty());
    }

    public void testDeleteBackingIndexForDataStream() {
        int numBackingIndices = randomIntBetween(2, 5);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        ProjectMetadata before = DataStreamTestHelper.getProjectWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of()
        );

        // Adjusting the random index to match zero-based indexing of the list
        int numIndexToDelete = randomIntBetween(0, numBackingIndices - 2);

        Index indexToDelete = before.index(before.dataStreams().get(dataStreamName).getIndices().get(numIndexToDelete)).getIndex();
        ClusterState after = MetadataDeleteIndexService.deleteIndices(
            projectStateFromProject(before),
            Set.of(indexToDelete),
            Settings.EMPTY
        );

        final var afterProject = after.metadata().getProject(before.id());
        assertThat(afterProject.indices().get(indexToDelete.getName()), nullValue());
        assertThat(afterProject.indices().size(), equalTo(numBackingIndices - 1));
        assertThat(afterProject.indices().get(indexToDelete.getName()), nullValue());
    }

    public void testDeleteFailureIndexForDataStream() {
        long now = System.currentTimeMillis();
        int numBackingIndices = randomIntBetween(2, 5);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        ProjectMetadata before = DataStreamTestHelper.getProjectWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of(),
            now,
            Settings.EMPTY,
            0,
            false,
            true
        );

        int numIndexToDelete = randomIntBetween(1, numBackingIndices - 1);

        Index indexToDelete = before.index(DataStream.getDefaultFailureStoreName(dataStreamName, numIndexToDelete, now)).getIndex();
        ClusterState afterState = MetadataDeleteIndexService.deleteIndices(
            projectStateFromProject(before),
            Set.of(indexToDelete),
            Settings.EMPTY
        );

        final var afterProject = afterState.metadata().getProject(before.id());
        assertThat(afterProject.indices().get(indexToDelete.getName()), nullValue());
        assertThat(afterProject.indices().size(), equalTo(2 * numBackingIndices - 1));
        assertThat(afterProject.indices().get(DataStream.getDefaultFailureStoreName(dataStreamName, numIndexToDelete, now)), nullValue());
    }

    public void testDeleteMultipleBackingIndexForDataStream() {
        int numBackingIndices = randomIntBetween(3, 5);
        int numBackingIndicesToDelete = randomIntBetween(2, numBackingIndices - 1);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        ProjectMetadata before = DataStreamTestHelper.getProjectWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of()
        );

        List<Integer> indexNumbersToDelete = randomSubsetOf(
            numBackingIndicesToDelete,
            IntStream.rangeClosed(1, numBackingIndices - 1).boxed().toList()
        );

        Set<Index> indicesToDelete = new HashSet<>();
        for (int k : indexNumbersToDelete) {
            final var index = before.dataStreams().get(dataStreamName).getIndices().get(k - 1);
            indicesToDelete.add(index);
        }
        ClusterState after = MetadataDeleteIndexService.deleteIndices(projectStateFromProject(before), indicesToDelete, Settings.EMPTY);

        final var afterProject = after.metadata().getProject(before.id());
        DataStream dataStream = afterProject.dataStreams().get(dataStreamName);
        assertThat(dataStream, notNullValue());
        assertThat(dataStream.getIndices().size(), equalTo(numBackingIndices - indexNumbersToDelete.size()));
        for (Index i : indicesToDelete) {
            assertThat(afterProject.indices().get(i.getName()), nullValue());
            assertFalse(dataStream.getIndices().contains(i));
        }
        assertThat(afterProject.indices().size(), equalTo(numBackingIndices - indexNumbersToDelete.size()));
    }

    public void testDeleteCurrentWriteIndexForDataStream() {
        int numBackingIndices = randomIntBetween(1, 5);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        ProjectMetadata before = DataStreamTestHelper.getProjectWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of()
        );

        Index indexToDelete = before.index(before.dataStreams().get(dataStreamName).getWriteIndex()).getIndex();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDeleteIndexService.deleteIndices(projectStateFromProject(before), Set.of(indexToDelete), Settings.EMPTY)
        );

        assertThat(
            e.getMessage(),
            containsString(
                "index [" + indexToDelete.getName() + "] is the write index for data stream [" + dataStreamName + "] and cannot be deleted"
            )
        );
    }

    public void testDeleteMultipleFailureIndexForDataStream() {
        int numBackingIndices = randomIntBetween(3, 5);
        int numBackingIndicesToDelete = randomIntBetween(2, numBackingIndices - 1);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        long ts = System.currentTimeMillis();
        ProjectMetadata before = DataStreamTestHelper.getProjectWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of(),
            ts,
            Settings.EMPTY,
            1,
            false,
            true
        );

        List<Integer> indexNumbersToDelete = randomSubsetOf(
            numBackingIndicesToDelete,
            IntStream.rangeClosed(1, numBackingIndices - 1).boxed().toList()
        );

        Set<Index> indicesToDelete = new HashSet<>();
        for (int k : indexNumbersToDelete) {
            indicesToDelete.add(before.index(DataStream.getDefaultFailureStoreName(dataStreamName, k, ts)).getIndex());
        }
        ClusterState after = MetadataDeleteIndexService.deleteIndices(projectStateFromProject(before), indicesToDelete, Settings.EMPTY);

        final var afterProject = after.metadata().getProject(before.id());
        DataStream dataStream = afterProject.dataStreams().get(dataStreamName);
        assertThat(dataStream, notNullValue());
        assertThat(dataStream.getFailureIndices().size(), equalTo(numBackingIndices - indexNumbersToDelete.size()));
        for (Index i : indicesToDelete) {
            assertThat(afterProject.indices().get(i.getName()), nullValue());
            assertFalse(dataStream.getFailureIndices().contains(i));
        }
        assertThat(afterProject.indices().size(), equalTo((2 * numBackingIndices) - indexNumbersToDelete.size()));
    }

    public void testDeleteCurrentWriteFailureIndexForDataStream() {
        int numBackingIndices = randomIntBetween(1, 5);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        long ts = System.currentTimeMillis();
        ProjectMetadata before = DataStreamTestHelper.getProjectWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of(),
            ts,
            Settings.EMPTY,
            1,
            false,
            true
        );

        Index indexToDelete = before.index(DataStream.getDefaultFailureStoreName(dataStreamName, numBackingIndices, ts)).getIndex();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDeleteIndexService.deleteIndices(projectStateFromProject(before), Set.of(indexToDelete), Settings.EMPTY)
        );

        assertThat(
            e.getMessage(),
            containsString(
                "index ["
                    + indexToDelete.getName()
                    + "] is the failure store write index for data stream ["
                    + dataStreamName
                    + "] and cannot be deleted"
            )
        );
    }

    public void testDeleteIndicesFromMultipleProjects() {
        final int numProjects = randomIntBetween(2, 5);

        final Set<Index> indicesToDelete = new HashSet<>();
        final Metadata.Builder metadataBuilder = Metadata.builder();
        for (int p = 0; p < numProjects; p++) {
            final int numberOfIndicesToCreate = randomIntBetween(1, 10);
            final int numberOfIndicesToDelete = randomIntBetween(1, numberOfIndicesToCreate);
            final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomUniqueProjectId());
            for (int i = 0; i < numberOfIndicesToCreate; i++) {
                final Index index = new Index(randomAlphaOfLengthBetween(8, 12), randomUUID());
                projectBuilder.put(
                    IndexMetadata.builder(index.getName())
                        .settings(
                            indexSettings(
                                IndexVersionUtils.randomVersion(),
                                index.getUUID(),
                                randomIntBetween(1, 3),
                                randomIntBetween(0, 2)
                            )
                        )
                );
                if (i < numberOfIndicesToDelete) {
                    indicesToDelete.add(index);
                }
            }
            metadataBuilder.put(projectBuilder);
        }

        final var metadata = metadataBuilder.build();
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .build();

        final ClusterState after = MetadataDeleteIndexService.deleteIndices(before, indicesToDelete, Settings.EMPTY);

        assertThat(after, not(sameInstance(before)));
        assertThat(
            after.metadata().getTotalNumberOfIndices(),
            equalTo(before.metadata().getTotalNumberOfIndices() - indicesToDelete.size())
        );
        for (Index idx : indicesToDelete) {
            assertThat(after.metadata().findIndex(idx), isEmpty());
        }
        assertThat(after.metadata().projects(), aMapWithSize(numProjects));
    }

    private ClusterState clusterState(Index index) {
        final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersionUtils.randomVersion(), index.getUUID(), 1, 1))
            .build();
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata.Builder metadataBuilder = Metadata.builder().put(ProjectMetadata.builder(projectId).put(indexMetadata, false));

        if (randomBoolean()) {
            final ProjectMetadata.Builder secondProject = ProjectMetadata.builder(randomUniqueProjectId());
            if (randomBoolean()) {
                secondProject.put(
                    IndexMetadata.builder(index.getName()).settings(indexSettings(IndexVersion.current(), randomUUID(), 1, 1))
                );
            }
            metadataBuilder.put(secondProject);
        }

        final var metadata = metadataBuilder.build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata))
            .build();
    }
}
