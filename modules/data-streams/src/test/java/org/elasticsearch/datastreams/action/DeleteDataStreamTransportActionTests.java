/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assume;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class DeleteDataStreamTransportActionTests extends ESTestCase {

    private final IndexNameExpressionResolver iner = TestIndexNameExpressionResolver.newInstance();
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final Consumer<String> validator = s -> EmptySystemIndices.INSTANCE.validateDataStreamAccess(s, threadContext);

    public void testDeleteDataStream() {
        final String dataStreamName = "my-data-stream";
        final List<String> otherIndices = randomSubsetOf(List.of("foo", "bar", "baz"));

        ClusterState initialState = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamName, 2)),
            otherIndices
        );
        final var projectId = initialState.metadata().projects().keySet().iterator().next();
        final var stateWithProjects = addProjectsWithDataStreams(initialState, dataStreamName, otherIndices);
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName });
        ClusterState newState = DeleteDataStreamTransportAction.removeDataStream(
            iner,
            TestProjectResolvers.singleProject(projectId).getProjectState(stateWithProjects),
            req,
            validator,
            Settings.EMPTY
        );
        assertThat(newState.metadata().getProject(projectId).dataStreams().size(), equalTo(0));
        assertThat(newState.metadata().getProject(projectId).indices().size(), equalTo(otherIndices.size()));
        for (String indexName : otherIndices) {
            assertThat(newState.metadata().getProject(projectId).indices().get(indexName).getIndex().getName(), equalTo(indexName));
        }
        // Ensure the other projects did not get affected.
        for (ProjectMetadata project : stateWithProjects.metadata().projects().values()) {
            if (project.id().equals(projectId)) {
                continue;
            }
            assertEquals(1, project.dataStreams().size());
            // Other indices + 2 for the backing indices of the data stream.
            assertEquals(otherIndices.size() + 2, project.indices().size());
        }
    }

    public void testDeleteDataStreamWithFailureStore() {
        Assume.assumeTrue(DataStream.isFailureStoreFeatureFlagEnabled());

        final String dataStreamName = "my-data-stream";
        final List<String> otherIndices = randomSubsetOf(List.of("foo", "bar", "baz"));

        ClusterState cs = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamName, 2)),
            otherIndices,
            System.currentTimeMillis(),
            Settings.EMPTY,
            1,
            false,
            true
        );
        final var projectId = cs.metadata().projects().keySet().iterator().next();
        final var stateWithProjects = addProjectsWithDataStreams(cs, dataStreamName, otherIndices);
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName });
        ClusterState newState = DeleteDataStreamTransportAction.removeDataStream(
            iner,
            TestProjectResolvers.singleProject(projectId).getProjectState(stateWithProjects),
            req,
            validator,
            Settings.EMPTY
        );
        assertThat(newState.metadata().getProject(projectId).dataStreams().size(), equalTo(0));
        assertThat(newState.metadata().getProject(projectId).indices().size(), equalTo(otherIndices.size()));
        for (String indexName : otherIndices) {
            assertThat(newState.metadata().getProject(projectId).indices().get(indexName).getIndex().getName(), equalTo(indexName));
        }
        // Ensure the other projects did not get affected.
        for (ProjectMetadata project : stateWithProjects.metadata().projects().values()) {
            if (project.id().equals(projectId)) {
                continue;
            }
            assertEquals(1, project.dataStreams().size());
            // Other indices + 2 for the backing indices of the data stream.
            assertEquals(otherIndices.size() + 2, project.indices().size());
        }
    }

    public void testDeleteMultipleDataStreams() {
        String[] dataStreamNames = { "foo", "bar", "baz", "eggplant" };
        ClusterState cs = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(
                new Tuple<>(dataStreamNames[0], randomIntBetween(1, 3)),
                new Tuple<>(dataStreamNames[1], randomIntBetween(1, 3)),
                new Tuple<>(dataStreamNames[2], randomIntBetween(1, 3)),
                new Tuple<>(dataStreamNames[3], randomIntBetween(1, 3))
            ),
            List.of()
        );
        final var projectId = cs.metadata().projects().keySet().iterator().next();
        final var stateWithProjects = addProjectsWithDataStreams(cs, randomFrom(dataStreamNames), List.of());

        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "ba*", "eggplant" });
        ClusterState newState = DeleteDataStreamTransportAction.removeDataStream(
            iner,
            TestProjectResolvers.singleProject(projectId).getProjectState(stateWithProjects),
            req,
            validator,
            Settings.EMPTY
        );
        assertThat(newState.metadata().getProject(projectId).dataStreams().size(), equalTo(1));
        DataStream remainingDataStream = newState.metadata().getProject(projectId).dataStreams().get(dataStreamNames[0]);
        assertNotNull(remainingDataStream);
        assertThat(newState.metadata().getProject(projectId).indices().size(), equalTo(remainingDataStream.getIndices().size()));
        for (Index i : remainingDataStream.getIndices()) {
            assertThat(newState.metadata().getProject(projectId).indices().get(i.getName()).getIndex(), equalTo(i));
        }
        // Ensure the other projects did not get affected.
        for (ProjectMetadata project : stateWithProjects.metadata().projects().values()) {
            if (project.id().equals(projectId)) {
                continue;
            }
            assertEquals(1, project.dataStreams().size());
        }
    }

    public void testDeleteSnapshottingDataStream() {
        final String dataStreamName = "my-data-stream1";
        final String dataStreamName2 = "my-data-stream2";
        final List<String> otherIndices = randomSubsetOf(List.of("foo", "bar", "baz"));

        ClusterState cs = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamName, 2), new Tuple<>(dataStreamName2, 2)),
            otherIndices
        );
        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.EMPTY.withAddedEntry(createEntry(dataStreamName, "repo1", false))
            .withAddedEntry(createEntry(dataStreamName2, "repo2", true));
        ClusterState snapshotCs = ClusterState.builder(cs).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress).build();
        final var projectId = snapshotCs.metadata().projects().keySet().iterator().next();
        final var stateWithProjects = addProjectsWithDataStreams(snapshotCs, dataStreamName2, otherIndices);

        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName });
        SnapshotInProgressException e = expectThrows(
            SnapshotInProgressException.class,
            () -> DeleteDataStreamTransportAction.removeDataStream(
                iner,
                TestProjectResolvers.singleProject(projectId).getProjectState(stateWithProjects),
                req,
                validator,
                Settings.EMPTY
            )
        );

        assertThat(
            e.getMessage(),
            equalTo(
                "Cannot delete data streams that are being snapshotted: [my-data-stream1]. Try again after "
                    + "snapshot finishes or cancel the currently running snapshot."
            )
        );
        // Ensure the other projects did not get affected.
        for (ProjectMetadata project : stateWithProjects.metadata().projects().values()) {
            if (project.id().equals(projectId)) {
                continue;
            }
            assertEquals(1, project.dataStreams().size());
        }
    }

    private SnapshotsInProgress.Entry createEntry(String dataStreamName, String repo, boolean partial) {
        return SnapshotsInProgress.Entry.snapshot(
            new Snapshot(repo, new SnapshotId("", "")),
            false,
            partial,
            SnapshotsInProgress.State.SUCCESS,
            Collections.emptyMap(),
            List.of(dataStreamName),
            Collections.emptyList(),
            0,
            1,
            ImmutableOpenMap.of(),
            null,
            null,
            null
        );
    }

    public void testDeleteNonexistentDataStream() {
        final String dataStreamName = "my-data-stream";
        String[] dataStreamNames = { "foo", "bar", "baz", "eggplant" };
        ClusterState cs = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(
                new Tuple<>(dataStreamNames[0], randomIntBetween(1, 3)),
                new Tuple<>(dataStreamNames[1], randomIntBetween(1, 3)),
                new Tuple<>(dataStreamNames[2], randomIntBetween(1, 3)),
                new Tuple<>(dataStreamNames[3], randomIntBetween(1, 3))
            ),
            List.of()
        );
        final var projectId = cs.metadata().projects().keySet().iterator().next();
        final var stateWithProjects = addProjectsWithDataStreams(cs, randomFrom(dataStreamNames), List.of());

        expectThrows(
            ResourceNotFoundException.class,
            () -> DeleteDataStreamTransportAction.removeDataStream(
                iner,
                TestProjectResolvers.singleProject(projectId).getProjectState(stateWithProjects),
                new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName }),
                validator,
                Settings.EMPTY
            )
        );

        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStreamName + "*" }
        );
        ClusterState newState = DeleteDataStreamTransportAction.removeDataStream(
            iner,
            TestProjectResolvers.singleProject(projectId).getProjectState(stateWithProjects),
            req,
            validator,
            Settings.EMPTY
        );
        assertThat(newState, sameInstance(stateWithProjects));
        assertThat(
            newState.metadata().getProject(projectId).dataStreams().size(),
            equalTo(stateWithProjects.metadata().getProject(projectId).dataStreams().size())
        );
        assertThat(
            newState.metadata().getProject(projectId).dataStreams().keySet(),
            containsInAnyOrder(stateWithProjects.metadata().getProject(projectId).dataStreams().keySet().toArray(Strings.EMPTY_ARRAY))
        );
    }

    private ClusterState addProjectsWithDataStreams(ClusterState initialState, String dataStreamName, List<String> otherIndices) {
        final var metadataBuilder = Metadata.builder(initialState.metadata());
        final var routingTableBuilder = GlobalRoutingTable.builder();
        final int numberOfProjects = randomIntBetween(0, 5);
        for (int i = 0; i < numberOfProjects; i++) {
            final var id = new ProjectId(randomUUID());
            var projectBuilder = ProjectMetadata.builder(id);
            DataStreamTestHelper.getClusterStateWithDataStreams(
                projectBuilder,
                List.of(Tuple.tuple(dataStreamName, 2)),
                otherIndices,
                System.currentTimeMillis(),
                Settings.EMPTY,
                1,
                false,
                false
            );
            metadataBuilder.put(projectBuilder.build());
            routingTableBuilder.put(id, RoutingTable.EMPTY_ROUTING_TABLE);
        }
        return ClusterState.builder(initialState).metadata(metadataBuilder.build()).routingTable(routingTableBuilder.build()).build();
    }
}
