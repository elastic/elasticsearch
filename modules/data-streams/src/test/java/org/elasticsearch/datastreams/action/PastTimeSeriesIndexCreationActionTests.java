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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.PastTimeSeriesIndexCreationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.action.TransportPastTimeSeriesIndexCreationAction.PastTimeSeriesIndexCreationExecutor;
import org.elasticsearch.datastreams.action.TransportPastTimeSeriesIndexCreationAction.PastTsdbIndexCreationTask;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PastTimeSeriesIndexCreationActionTests extends ESTestCase {

    private static final String DATA_STREAM = "my-tsdb";
    private static final long INDEX_INTERVAL_MILLIS = TimeValue.timeValueDays(1).millis();

    private ProjectId projectId;
    private MetadataCreateDataStreamService createDataStreamService;
    private PastTimeSeriesIndexCreationExecutor executor;
    private ProjectResolver projectResolver;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        projectId = randomProjectIdOrDefault();
        createDataStreamService = mock(MetadataCreateDataStreamService.class);
        when(createDataStreamService.createPastBackingIndex(any(), any(), any(), any(), any(), any(), any(), any())).thenAnswer(inv -> {
            ClusterState clusterState = inv.getArgument(0);
            String indexName = inv.getArgument(5);
            Instant startTime = inv.getArgument(6);
            Instant endTime = inv.getArgument(7);
            IndexMetadata indexMetadata = createIndexMetadata(indexName, startTime, endTime);
            ProjectMetadata projectMetadata = clusterState.projectState(projectId).metadata();
            DataStream dataStream = projectMetadata.dataStreams().get(DATA_STREAM).unsafeAddBackingIndex(indexMetadata.getIndex());
            ProjectMetadata project = ProjectMetadata.builder(projectMetadata).put(indexMetadata, false).put(dataStream).build();
            return ClusterState.builder(clusterState).putProjectMetadata(project).build();
        });
        projectResolver = TestProjectResolvers.singleProject(projectId);
    }

    public void testSortAndRetrieve() {
        // Test no TSDB indices
        {
            ClusterState state = stateWithNoTsdbIndices();
            DataStream ds = state.projectState(projectId).metadata().dataStreams().get(DATA_STREAM);
            var result = PastTimeSeriesIndexCreationExecutor.retrieveSortedTimeWindows(ds, state.projectState(projectId).metadata());
            assertThat(result, empty());
        }
        // Retrieve all continuous TSDB indices in a single window
        {
            Instant start1 = Instant.parse("2024-01-15T00:00:00Z");
            Instant start2 = Instant.parse("2024-01-16T00:00:00Z");
            Instant start3 = Instant.parse("2024-01-17T00:00:00Z");
            Instant end = Instant.parse("2024-01-18T00:00:00Z");
            ProjectMetadata project = DataStreamTestHelper.getProjectWithDataStream(
                projectId,
                DATA_STREAM,
                List.of(Tuple.tuple(start3, end), Tuple.tuple(start1, start2), Tuple.tuple(start2, start3))
            );
            // Updated project metadata with a consequent and unsored ts backing indices in a mixed data stream
            String nonTsdbName = randomIndexName();
            IndexMetadata nonTsdb = IndexMetadata.builder(nonTsdbName)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            DataStream mixedDs = project.dataStreams().get(DATA_STREAM).unsafeAddBackingIndex(nonTsdb.getIndex());
            project = ProjectMetadata.builder(project).put(nonTsdb, false).put(mixedDs).build();

            var result = PastTimeSeriesIndexCreationExecutor.retrieveSortedTimeWindows(project.dataStreams().get(DATA_STREAM), project);
            assertThat(result, hasSize(1));
            TransportPastTimeSeriesIndexCreationAction.CoveredTimeWindow timeWindow = result.pop();
            assertThat(timeWindow.start(), is(start1.toEpochMilli()));
            assertThat(timeWindow.end(), is(end.toEpochMilli()));
        }

        // Retrieve all continuous TSDB indices in a single window
        {
            // 3 continuous indices window A
            Instant startA_1 = Instant.parse("2024-01-15T00:00:00Z");
            Instant startA_2 = Instant.parse("2024-01-16T00:00:00Z");
            Instant startA_3 = Instant.parse("2024-01-17T00:00:00Z");
            Instant endA = Instant.parse("2024-01-18T00:00:00Z");

            // 2 continuous indices window B
            Instant startB_1 = Instant.parse("2025-01-15T00:00:00Z");
            Instant startB_2 = Instant.parse("2025-01-16T12:00:00Z");
            Instant endB = Instant.parse("2025-01-18T00:00:00Z");

            // now, will be used for the "write" backing index
            Instant now = Instant.now();

            ProjectMetadata project = stateWithExisting(
                List.of(
                    Tuple.tuple(startA_3, endA),
                    Tuple.tuple(startA_1, startA_2),
                    Tuple.tuple(startB_1, startB_2),
                    Tuple.tuple(startA_2, startA_3),
                    Tuple.tuple(startB_2, endB)
                ),
                now
            ).projectState(projectId).metadata();

            var result = PastTimeSeriesIndexCreationExecutor.retrieveSortedTimeWindows(project.dataStreams().get(DATA_STREAM), project);
            assertThat(result, hasSize(3));
            TransportPastTimeSeriesIndexCreationAction.CoveredTimeWindow timeWindow = result.pop();
            assertThat(timeWindow.start(), is(startA_1.toEpochMilli()));
            assertThat(timeWindow.end(), is(endA.toEpochMilli()));
            timeWindow = result.pop();
            assertThat(timeWindow.start(), is(startB_1.toEpochMilli()));
            assertThat(timeWindow.end(), is(endB.toEpochMilli()));
            timeWindow = result.pop();
            assertThat(timeWindow.start(), is(now.toEpochMilli()));
            assertThat(timeWindow.end(), greaterThan(now.toEpochMilli()));
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

        // Add future timestamp should be skipped.
        {
            Instant futureTimestamp = Instant.now().plus(5, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MILLIS);
            TaskResult result = run(clusterState, futureTimestamp.toEpochMilli());
            assertThat(result.covered, is(empty()));
            assertThat(result.createdNames, is(empty()));
            assertThat(result.rejectedTimestamps.size(), is(1));
            assertThat(result.rejectedTimestamps.keySet(), contains(futureTimestamp));
            assertThat(result.rejectedTimestamps.get(futureTimestamp), containsString("is after the request start time"));
        }
    }

    public void testFillGapBetweenIndices() throws Exception {
        Instant now = Instant.now();
        // Smaller gap than index interval
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

        // Larger gap than an index interval but within the threshold
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

        // Larger gap than the index interval AND the threshold
        {
            Instant previousEndTime = Instant.parse("2024-06-17T02:02:02Z");
            Instant nextStartTime = Instant.parse("2024-06-18T15:02:02Z");
            ClusterState clusterState = stateWithExisting(
                List.of(
                    Tuple.tuple(Instant.parse("2024-06-16T07:02:02Z"), previousEndTime),
                    Tuple.tuple(nextStartTime, Instant.parse("2024-06-19T22:02:02Z"))
                ),
                now
            );
            Instant ts1 = Instant.parse("2024-06-17T02:02:02Z");
            Instant ts2 = Instant.parse("2024-06-18T14:02:02Z");
            TaskResult result = run(clusterState, ts1.toEpochMilli(), ts2.toEpochMilli());
            assertThat(result.covered(), containsInAnyOrder(ts1, ts2));
            assertThat(result.createdNames.size(), equalTo(2));
            IndexMetadata im = result.state().projectState(projectId).metadata().index(result.createdNames.getFirst());
            assertThat(im.getTimeSeriesStart(), is(previousEndTime));
            Instant indexSplit = nextStartTime.minusMillis(INDEX_INTERVAL_MILLIS);
            assertThat(im.getTimeSeriesEnd(), is(indexSplit));
            im = result.state().projectState(projectId).metadata().index(result.createdNames.get(1));
            assertThat(im.getTimeSeriesStart(), is(indexSplit));
            assertThat(im.getTimeSeriesEnd(), is(nextStartTime));
        }
    }

    public void testDataStreamNotFound() {
        ClusterState emptyState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        long ts = Instant.parse("2024-01-15T09:00:00Z").toEpochMilli();
        var task = new PastTsdbIndexCreationTask(
            DATA_STREAM,
            new long[] { ts },
            Set.of(),
            TimeValue.ZERO,
            ActionListener.noop(),
            Instant.now().toEpochMilli()
        );
        expectThrows(
            ResourceNotFoundException.class,
            () -> PastTimeSeriesIndexCreationExecutor.executeTask(
                emptyState,
                projectResolver,
                createDataStreamService,
                task,
                new ArrayList<>(),
                new HashSet<>(),
                new HashMap<>(),
                INDEX_INTERVAL_MILLIS,
                -1,
                Instant.now().toEpochMilli()
            )
        );
    }

    public void testReplicatedDataStreamFails() {
        ClusterState state = stateWithExisting(List.of(), Instant.now());
        ProjectMetadata projectMetadata = state.projectState(projectId).metadata();
        DataStream ds = projectMetadata.dataStreams().get(DATA_STREAM).copy().setReplicated(true).build();
        ClusterState stateWithReplicated = ClusterState.builder(state)
            .putProjectMetadata(ProjectMetadata.builder(projectMetadata).put(ds).build())
            .build();
        long ts = Instant.parse("2024-01-15T09:00:00Z").toEpochMilli();
        var task = new PastTsdbIndexCreationTask(
            DATA_STREAM,
            new long[] { ts },
            Set.of(),
            TimeValue.ZERO,
            ActionListener.noop(),
            Instant.now().toEpochMilli()
        );
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> PastTimeSeriesIndexCreationExecutor.executeTask(
                stateWithReplicated,
                projectResolver,
                createDataStreamService,
                task,
                new ArrayList<>(),
                new HashSet<>(),
                new HashMap<>(),
                INDEX_INTERVAL_MILLIS,
                -1L,
                Instant.now().toEpochMilli()
            )
        );
        assertThat(
            illegalArgumentException.getMessage(),
            containsString("Cannot create past TSDB backing index for replicated data stream")
        );
    }

    public void testNotATsdbFails() {
        ClusterState state = stateWithNoTsdbIndices();
        long ts = Instant.parse("2024-01-15T09:00:00Z").toEpochMilli();
        var task = new PastTsdbIndexCreationTask(
            DATA_STREAM,
            new long[] { ts },
            Set.of(),
            TimeValue.ZERO,
            ActionListener.noop(),
            Instant.now().toEpochMilli()
        );
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> PastTimeSeriesIndexCreationExecutor.executeTask(
                state,
                projectResolver,
                createDataStreamService,
                task,
                new ArrayList<>(),
                new HashSet<>(),
                new HashMap<>(),
                INDEX_INTERVAL_MILLIS,
                -1L,
                Instant.now().toEpochMilli()
            )
        );
        assertThat(illegalArgumentException.getMessage(), containsString(", it needs to be a time series data stream."));
    }

    public void testSystemDataStreamFails() {
        ClusterState state = stateWithExisting(List.of(), Instant.now());
        ProjectMetadata projectMetadata = state.projectState(projectId).metadata();
        DataStream ds = projectMetadata.dataStreams().get(DATA_STREAM).copy().setSystem(true).setHidden(true).build();
        ClusterState stateWithSystem = ClusterState.builder(state)
            .putProjectMetadata(ProjectMetadata.builder(projectMetadata).put(ds).build())
            .build();
        long ts = Instant.parse("2024-01-15T09:00:00Z").toEpochMilli();
        var task = new PastTsdbIndexCreationTask(
            DATA_STREAM,
            new long[] { ts },
            Set.of(),
            TimeValue.ZERO,
            ActionListener.noop(),
            Instant.now().toEpochMilli()
        );
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> PastTimeSeriesIndexCreationExecutor.executeTask(
                stateWithSystem,
                projectResolver,
                createDataStreamService,
                task,
                new ArrayList<>(),
                new HashSet<>(),
                new HashMap<>(),
                INDEX_INTERVAL_MILLIS,
                -1L,
                Instant.now().toEpochMilli()
            )
        );
        assertThat(illegalArgumentException.getMessage(), containsString("Cannot create past TSDB backing index for system data stream"));
    }

    public void testRequestSerialization() throws Exception {
        long now = System.currentTimeMillis();
        String dataStream = randomAlphaOfLength(10);
        List<Instant> timestamps = LongStream.generate(
            () -> now - randomLongBetween(TimeValue.timeValueDays(5).getMillis(), TimeValue.timeValueDays(100).getMillis())
        ).limit(randomIntBetween(1, 20)).mapToObj(Instant::ofEpochMilli).toList();
        long startTime = now + randomLongBetween(0, TimeValue.timeValueHours(2).getMillis());
        var original = new PastTimeSeriesIndexCreationAction.Request(randomPositiveTimeValue(), dataStream, timestamps, startTime);
        var copy = copyWriteable(original, writableRegistry(), PastTimeSeriesIndexCreationAction.Request::new);
        assertThat(copy.masterNodeTimeout(), equalTo(original.masterNodeTimeout()));
        assertThat(copy.ackTimeout(), equalTo(original.ackTimeout()));
        assertThat(copy.dataStreamName(), equalTo(original.dataStreamName()));
        assertThat(copy.timestamps(), equalTo(original.timestamps()));
        assertThat(copy.requestStartTime(), equalTo(original.requestStartTime()));
    }

    public void testResponseSerialization() throws Exception {
        Set<Instant> covered = Set.of(Instant.parse("2024-01-15T09:00:00Z"), Instant.parse("2024-01-16T09:00:00Z"));
        Instant rejected1 = Instant.parse("2024-01-10T09:00:00Z");
        Instant rejected2 = Instant.parse("2024-01-05T09:00:00Z");
        Map<Instant, String> rejections = Map.of(rejected1, "outside write window", rejected2, "index creation failed");
        var original = new PastTimeSeriesIndexCreationAction.Response(true, covered, rejections);
        var copy = copyWriteable(original, writableRegistry(), PastTimeSeriesIndexCreationAction.Response::new);
        assertThat(copy.isAcknowledged(), is(true));
        assertThat(copy.coveredTimestamps(), is(covered));
        assertThat(copy.rejectedTimestamps().size(), is(2));
        assertThat(copy.rejectedTimestamps().get(rejected1), equalTo("outside write window"));
        assertThat(copy.rejectedTimestamps().get(rejected2), equalTo("index creation failed"));
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

    public void testOutsideEligibleWriteWindowFails() throws Exception {
        Instant now = Instant.now();
        ClusterState clusterState = stateWithExisting(List.of(), now);
        // A 7-day write window: timestamps older than 7 days should fail.
        long windowStart = now.minus(7, ChronoUnit.DAYS).toEpochMilli();

        Instant withinWindow = now.minus(6, ChronoUnit.DAYS).truncatedTo(ChronoUnit.MILLIS);
        Instant outsideWindow = now.minus(30, ChronoUnit.DAYS).truncatedTo(ChronoUnit.MILLIS);

        TaskResult result = runWithWindow(clusterState, windowStart, withinWindow.toEpochMilli(), outsideWindow.toEpochMilli());
        assertThat(result.covered(), containsInAnyOrder(withinWindow));
        assertThat(result.rejectedTimestamps().size(), is(1));
        assertThat(result.rejectedTimestamps().containsKey(outsideWindow), is(true));
        assertThat(result.rejectedTimestamps().get(outsideWindow), containsString("is earlier than the lifecycle permits writes"));
    }

    private TaskResult run(ClusterState state, long... timestamps) throws Exception {
        return runWithWindow(state, -1L, timestamps);
    }

    private TaskResult runWithWindow(ClusterState state, long eligibleWriteWindowStart, long... timestamps) throws Exception {
        long[] sorted = timestamps.clone();
        java.util.Arrays.sort(sorted);
        var task = new PastTsdbIndexCreationTask(
            DATA_STREAM,
            sorted,
            Set.of(),
            TimeValue.ZERO,
            ActionListener.noop(),
            System.currentTimeMillis()
        );
        List<String> createdNames = new ArrayList<>();
        Set<Instant> covered = new HashSet<>();
        Map<Instant, String> rejectedTimestamps = new HashMap<>();
        ClusterState updatedClusterState = PastTimeSeriesIndexCreationExecutor.executeTask(
            state,
            projectResolver,
            createDataStreamService,
            task,
            createdNames,
            covered,
            rejectedTimestamps,
            INDEX_INTERVAL_MILLIS,
            eligibleWriteWindowStart,
            Instant.now().toEpochMilli()
        );
        return new TaskResult(updatedClusterState, covered, createdNames, rejectedTimestamps);
    }

    private record TaskResult(
        ClusterState state,
        Set<Instant> covered,
        List<String> createdNames,
        Map<Instant, String> rejectedTimestamps
    ) {}

    private static Instant getTimestampWithinDay(Instant now, int dayOffsets, int hourOffset) {
        return now.minus(dayOffsets, ChronoUnit.DAYS).minus(hourOffset, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);
    }
}
