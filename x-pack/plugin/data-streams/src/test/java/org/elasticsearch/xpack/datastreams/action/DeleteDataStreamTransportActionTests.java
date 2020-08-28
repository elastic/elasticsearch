/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.datastreams.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DataStreamTestHelper;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteDataStreamTransportActionTests extends ESTestCase {

    public void testDeleteDataStream() {
        final String dataStreamName = "my-data-stream";
        final List<String> otherIndices = randomSubsetOf(List.of("foo", "bar", "baz"));

        ClusterState cs = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>(dataStreamName, 2)), otherIndices);
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(new String[] { dataStreamName });
        ClusterState newState = DeleteDataStreamTransportAction.removeDataStream(getMetadataDeleteIndexService(), cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(0));
        assertThat(newState.metadata().indices().size(), equalTo(otherIndices.size()));
        for (String indexName : otherIndices) {
            assertThat(newState.metadata().indices().get(indexName).getIndex().getName(), equalTo(indexName));
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

        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(new String[] { "ba*", "eggplant" });
        ClusterState newState = DeleteDataStreamTransportAction.removeDataStream(getMetadataDeleteIndexService(), cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(1));
        DataStream remainingDataStream = newState.metadata().dataStreams().get(dataStreamNames[0]);
        assertNotNull(remainingDataStream);
        assertThat(newState.metadata().indices().size(), equalTo(remainingDataStream.getIndices().size()));
        for (Index i : remainingDataStream.getIndices()) {
            assertThat(newState.metadata().indices().get(i.getName()).getIndex(), equalTo(i));
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
        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.of(
            List.of(createEntry(dataStreamName, "repo1", false), createEntry(dataStreamName2, "repo2", true))
        );
        ClusterState snapshotCs = ClusterState.builder(cs).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress).build();

        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(new String[] { dataStreamName });
        SnapshotInProgressException e = expectThrows(
            SnapshotInProgressException.class,
            () -> DeleteDataStreamTransportAction.removeDataStream(getMetadataDeleteIndexService(), snapshotCs, req)
        );

        assertThat(
            e.getMessage(),
            equalTo(
                "Cannot delete data streams that are being snapshotted: [my-data-stream1]. Try again after "
                    + "snapshot finishes or cancel the currently running snapshot."
            )
        );
    }

    private SnapshotsInProgress.Entry createEntry(String dataStreamName, String repo, boolean partial) {
        return new SnapshotsInProgress.Entry(
            new Snapshot(repo, new SnapshotId("", "")),
            false,
            partial,
            SnapshotsInProgress.State.SUCCESS,
            Collections.emptyList(),
            List.of(dataStreamName),
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
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(new String[] { dataStreamName });
        ClusterState newState = DeleteDataStreamTransportAction.removeDataStream(getMetadataDeleteIndexService(), cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(cs.metadata().dataStreams().size()));
        assertThat(
            newState.metadata().dataStreams().keySet(),
            containsInAnyOrder(cs.metadata().dataStreams().keySet().toArray(Strings.EMPTY_ARRAY))
        );
    }

    @SuppressWarnings("unchecked")
    private static MetadataDeleteIndexService getMetadataDeleteIndexService() {
        MetadataDeleteIndexService s = mock(MetadataDeleteIndexService.class);
        when(s.deleteIndices(any(ClusterState.class), any(Set.class))).thenAnswer(mockInvocation -> {
            ClusterState currentState = (ClusterState) mockInvocation.getArguments()[0];
            Set<Index> indices = (Set<Index>) mockInvocation.getArguments()[1];

            final Metadata.Builder b = Metadata.builder(currentState.metadata());
            for (Index index : indices) {
                b.remove(index.getName());
            }

            return ClusterState.builder(currentState).metadata(b.build()).build();
        });

        return s;
    }

}
