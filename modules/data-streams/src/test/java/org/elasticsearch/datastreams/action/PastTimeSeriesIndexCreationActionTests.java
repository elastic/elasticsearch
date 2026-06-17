/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.PastTimeSeriesIndexCreationAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.action.TransportPastTimeSeriesIndexCreationAction.PastTimeSeriesIndexCreationExecutor;
import org.elasticsearch.datastreams.action.TransportPastTimeSeriesIndexCreationAction.PastTsdbIndexCreationTask;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PastTimeSeriesIndexCreationActionTests extends ESTestCase {

    private static final String DATA_STREAM = "my-tsdb";

    private ProjectId projectId;
    private MetadataCreateDataStreamService createDataStreamService;
    private PastTimeSeriesIndexCreationExecutor executor;
    private ProjectResolver projectResolver;
    private SystemIndices systemIndices;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        projectId = randomProjectIdOrDefault();
        createDataStreamService = mock(MetadataCreateDataStreamService.class);
        when(createDataStreamService.createPastBackingIndex(any(), any(), any(), any(), any(), any(), any(), any(), any())).thenAnswer(
            inv -> {
                ClusterState clusterState = inv.getArgument(1);
                String indexName = inv.getArgument(6);
                Instant startTime = inv.getArgument(7);
                Instant endTime = inv.getArgument(8);
                IndexMetadata indexMetadata = createIndexMetadata(indexName, startTime, endTime);
                ProjectMetadata projectMetadata = clusterState.projectState(projectId).metadata();
                DataStream dataStream = projectMetadata.dataStreams().get(DATA_STREAM).unsafeAddBackingIndex(indexMetadata.getIndex());
                ProjectMetadata project = ProjectMetadata.builder(projectMetadata).put(indexMetadata, false).put(dataStream).build();
                return ClusterState.builder(clusterState).putProjectMetadata(project).build();
            }
        );
        projectResolver = TestProjectResolvers.singleProject(projectId);
        systemIndices = mock(SystemIndices.class);
    }

    public void testSortAndRetrieve() {
        // Test no TSDB indices
        {
            ClusterState state = stateWithNoTsdbIndices();
            DataStream ds = state.projectState(projectId).metadata().dataStreams().get(DATA_STREAM);
            var result = PastTimeSeriesIndexCreationExecutor.sortAndRetrieveExistingBackingIndices(
                ds,
                state.projectState(projectId).metadata()
            );
            assertThat(result, empty());
        }
        // Retrieve all TSDB indices in ascending order
        {
            Instant start1 = Instant.parse("2024-01-15T00:00:00Z");
            Instant start2 = Instant.parse("2024-01-16T00:00:00Z");
            Instant start3 = Instant.parse("2024-01-17T00:00:00Z");
            ProjectMetadata project = DataStreamTestHelper.getProjectWithDataStream(
                projectId,
                DATA_STREAM,
                List.of(
                    Tuple.tuple(start3, Instant.parse("2024-01-18T00:00:00Z")),
                    Tuple.tuple(start1, start2),
                    Tuple.tuple(start2, start3)
                )
            );
            // Updated project metadata with a mixed backing index data stream
            String nonTsdbName = randomIndexName();
            IndexMetadata nonTsdb = IndexMetadata.builder(nonTsdbName)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            DataStream mixedDs = project.dataStreams().get(DATA_STREAM).unsafeAddBackingIndex(nonTsdb.getIndex());
            project = ProjectMetadata.builder(project).put(nonTsdb, false).put(mixedDs).build();

            var result = PastTimeSeriesIndexCreationExecutor.sortAndRetrieveExistingBackingIndices(
                project.dataStreams().get(DATA_STREAM),
                project
            );
            assertThat(result, hasSize(3));
            assertThat(result.pop().start(), is(start1.toEpochMilli()));
            assertThat(result.pop().start(), is(start2.toEpochMilli()));
            assertThat(result.pop().start(), is(start3.toEpochMilli()));
        }
    }

    public void testCreateIndicesWhenNeeded() throws Exception {
        Instant now = Instant.now();
        ClusterState clusterState = stateWithExisting(List.of(), now);
        List<Integer> dayOffsets = List.of(5, 3, 2);
        List<String> createdNames = new ArrayList<>();
        // Add two timestamps that fall within one index
        {
            Instant ts1 = getTimestampWithinDay(now, dayOffsets.getFirst(), randomIntBetween(2, 5));
            Instant ts2 = getTimestampWithinDay(now, dayOffsets.getFirst(), randomIntBetween(7, 12));
            TaskResult result = run(clusterState, ts1.toEpochMilli(), ts2.toEpochMilli());
            assertThat(result.covered, containsInAnyOrder(ts1, ts2));
            assertThat(result.createdNames.size(), is(1));
            createdNames.addAll(result.createdNames);
            clusterState = result.state();
        }

        // Add two timestamp that will create two indices
        {
            Instant ts1 = getTimestampWithinDay(now, dayOffsets.get(1), randomIntBetween(1, 5));
            Instant ts2 = getTimestampWithinDay(now, dayOffsets.get(2), randomIntBetween(13, 18));
            TaskResult result = run(clusterState, ts1.toEpochMilli(), ts2.toEpochMilli());
            assertThat(result.covered, containsInAnyOrder(ts1, ts2));
            assertThat(result.createdNames.size(), is(2));
            createdNames.addAll(result.createdNames);
            clusterState = result.state();
        }
        // Add another timestamp already covered
        {
            Instant ts = getTimestampWithinDay(now, randomFrom(dayOffsets), randomIntBetween(1, 23));
            TaskResult result = run(clusterState, ts.toEpochMilli());
            assertThat(result.covered, containsInAnyOrder(ts));
            assertThat(result.createdNames, empty());
            clusterState = result.state();
        }

        // Add timestamp at index end time, should create new one
        {
            // Take the last one so the end time doesn't overlap
            Instant ts = clusterState.projectState(projectId).metadata().index(createdNames.getLast()).getTimeSeriesEnd();
            TaskResult result = run(clusterState, ts.toEpochMilli());
            assertThat(result.covered, containsInAnyOrder(ts));
            assertThat(result.createdNames.size(), is(1));
        }
    }

    public void testFillGapBetweenIndices() throws Exception {
        Instant now = Instant.now();
        // Smaller gap
        {
            Instant previousEndTime = Instant.parse("2024-06-17T02:02:02Z");
            Instant nextStartTime = Instant.parse("2024-06-17T23:02:02Z");
            ClusterState clusterState = stateWithExisting(
                List.of(
                    Tuple.tuple(Instant.parse("2024-06-16T07:02:02Z"), previousEndTime),
                    Tuple.tuple(nextStartTime, Instant.parse("2024-06-18T02:02:02Z"))
                ),
                now
            );
            long ts = randomLongBetween(previousEndTime.toEpochMilli(), nextStartTime.toEpochMilli() - 1);
            TaskResult result = run(clusterState, ts);
            assertThat(result.createdNames.size(), is(1));
            assertThat(result.covered(), containsInAnyOrder(Instant.ofEpochMilli(ts)));
            IndexMetadata im = result.state().projectState(projectId).metadata().index(result.createdNames.getFirst());
            assertThat(im.getTimeSeriesStart(), is(previousEndTime));
            assertThat(im.getTimeSeriesEnd(), is(nextStartTime));
        }

        // Larger gap
        {
            Instant previousEndTime = Instant.parse("2024-06-17T02:02:02Z");
            Instant nextStartTime = Instant.parse("2024-06-18T05:02:02Z");
            ClusterState clusterState = stateWithExisting(
                List.of(
                    Tuple.tuple(Instant.parse("2024-06-16T07:02:02Z"), previousEndTime),
                    Tuple.tuple(nextStartTime, Instant.parse("2024-06-18T22:02:02Z"))
                ),
                now
            );
            long ts = randomLongBetween(previousEndTime.toEpochMilli(), nextStartTime.toEpochMilli() - 1);
            TaskResult result = run(clusterState, ts);
            assertThat(result.createdNames.size(), is(1));
            assertThat(result.covered(), containsInAnyOrder(Instant.ofEpochMilli(ts)));
            IndexMetadata im = result.state().projectState(projectId).metadata().index(result.createdNames.getFirst());
            assertThat(im.getTimeSeriesStart(), is(previousEndTime));
            assertThat(im.getTimeSeriesEnd(), is(nextStartTime));
        }
    }

    public void testDataStreamNotFound() {
        ClusterState emptyState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        long ts = Instant.parse("2024-01-15T09:00:00Z").toEpochMilli();
        var task = new PastTsdbIndexCreationTask(DATA_STREAM, new long[] { ts }, TimeValue.ZERO, ActionListener.noop());
        expectThrows(
            ResourceNotFoundException.class,
            () -> PastTimeSeriesIndexCreationExecutor.executeTask(
                emptyState,
                projectResolver,
                createDataStreamService,
                systemIndices,
                task,
                new ArrayList<>(),
                new HashSet<>()
            )
        );
    }

    public void testEnsureNoSnapshotInProgressThrows() {
        ClusterState state = stateWithExisting(
            List.of(Tuple.tuple(Instant.parse("2024-01-15T00:00:00Z"), Instant.parse("2024-01-16T00:00:00Z"))),
            Instant.now()
        );
        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.EMPTY.withAddedEntry(snapshotEntry(DATA_STREAM, projectId, false));
        ClusterState stateWithSnapshot = ClusterState.builder(state).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress).build();

        expectThrows(
            SnapshotInProgressException.class,
            () -> PastTimeSeriesIndexCreationExecutor.ensureNoSnapshotInProgress(stateWithSnapshot.projectState(projectId), DATA_STREAM)
        );
    }

    @SuppressWarnings("unchecked")
    public void testReplicatedDataStreamFails() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.createTaskQueue(any(), any(), any())).thenReturn(mock(MasterServiceTaskQueue.class));

        var action = new TransportPastTimeSeriesIndexCreationAction(
            mock(TransportService.class),
            clusterService,
            systemIndices,
            Settings.EMPTY,
            threadPool,
            mock(ActionFilters.class),
            mock(AllocationService.class),
            createDataStreamService,
            projectResolver
        );

        String indexName = DataStream.getDefaultBackingIndexName(DATA_STREAM, 1);
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DataStream replicatedDs = DataStream.builder(DATA_STREAM, List.of(indexMetadata.getIndex()))
            .setGeneration(1)
            .setReplicated(true)
            .build();
        ProjectMetadata project = ProjectMetadata.builder(projectId).put(indexMetadata, false).put(replicatedDs).build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build();

        var request = new PastTimeSeriesIndexCreationAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            DATA_STREAM,
            List.of(Instant.parse("2024-01-15T09:00:00Z"))
        );

        AtomicReference<Exception> capturedFailure = new AtomicReference<>();
        action.masterOperation(mock(Task.class), request, state, ActionListener.wrap(r -> {}, capturedFailure::set));

        assertThat(capturedFailure.get(), instanceOf(ElasticsearchException.class));
        assertThat(capturedFailure.get().getMessage(), containsString(DATA_STREAM));
    }

    /** Builds a ClusterState with a TSDB data stream whose backing indices cover the given time ranges. */
    private ClusterState stateWithExisting(List<Tuple<Instant, Instant>> timeSlices, Instant now) {
        timeSlices = new ArrayList<>(timeSlices);
        timeSlices.add(Tuple.tuple(now, now.plus(randomIntBetween(1, 3), ChronoUnit.DAYS)));
        return ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(DataStreamTestHelper.getProjectWithDataStream(projectId, DATA_STREAM, timeSlices))
            .build();
    }

    /** Builds a ClusterState with a data stream that has one non-TSDB (standard) backing index. */
    private ClusterState stateWithNoTsdbIndices() {
        String indexName = DataStream.getDefaultBackingIndexName(DATA_STREAM, 1);
        IndexMetadata nonTsdb = createIndexMetadata(indexName, null, null);
        DataStream ds = DataStream.builder(DATA_STREAM, List.of(nonTsdb.getIndex())).setGeneration(1).build();
        ProjectMetadata project = ProjectMetadata.builder(projectId).put(nonTsdb, false).put(ds).build();
        return ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build();
    }

    private static IndexMetadata createIndexMetadata(String indexName, Instant startTime, Instant endTime) {
        Settings.Builder settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current());
        if (startTime != null && endTime != null) {
            settings.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), startTime.toEpochMilli())
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), endTime.toEpochMilli());
        }
        return IndexMetadata.builder(indexName).settings(settings).numberOfShards(1).numberOfReplicas(0).build();
    }

    private TaskResult run(ClusterState state, long... timestamps) throws Exception {
        long[] sorted = timestamps.clone();
        java.util.Arrays.sort(sorted);
        var task = new PastTsdbIndexCreationTask(DATA_STREAM, sorted, TimeValue.ZERO, ActionListener.noop());
        List<String> createdNames = new ArrayList<>();
        Set<Instant> covered = new HashSet<>();
        ClusterState updatedClusterState = PastTimeSeriesIndexCreationExecutor.executeTask(
            state,
            projectResolver,
            createDataStreamService,
            systemIndices,
            task,
            createdNames,
            covered
        );
        return new TaskResult(updatedClusterState, covered, createdNames);
    }

    private record TaskResult(ClusterState state, Set<Instant> covered, List<String> createdNames) {}

    private static Instant getTimestampWithinDay(Instant now, int dayOffsets, int hourOffset) {
        return now.minus(dayOffsets, ChronoUnit.DAYS).minus(hourOffset, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);
    }

    private static SnapshotsInProgress.Entry snapshotEntry(String dataStreamName, ProjectId projectId, boolean partial) {
        return SnapshotsInProgress.Entry.snapshot(
            new Snapshot(projectId, "repo", new SnapshotId("snap", "")),
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
}
