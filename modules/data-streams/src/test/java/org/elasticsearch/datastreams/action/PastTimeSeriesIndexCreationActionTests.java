/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.datastreams.action.TransportPastTimeSeriesIndexCreationAction.IndexBoundaries;
import org.elasticsearch.datastreams.action.TransportPastTimeSeriesIndexCreationAction.PastTsdbIndexCreationExecutor;
import org.elasticsearch.datastreams.action.TransportPastTimeSeriesIndexCreationAction.PastTsdbIndexCreationTask;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PastTimeSeriesIndexCreationActionTests extends ESTestCase {

    private static final String DATA_STREAM = "my-tsdb";

    private ProjectId projectId;
    private MetadataCreateDataStreamService createDataStreamService;
    private PastTsdbIndexCreationExecutor executor;
    private ProjectResolver projectResolver;
    private SystemIndices systemIndices;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        projectId = randomProjectIdOrDefault();
        createDataStreamService = mock(MetadataCreateDataStreamService.class);
        projectResolver = TestProjectResolvers.singleProject(projectId);
        systemIndices = mock(SystemIndices.class);
    }

    public void testSortAndRetrieve_allNonTsdbIndices() {
        ClusterState state = stateWithNoTsdbIndices();
        DataStream ds = state.projectState(projectId).metadata().dataStreams().get(DATA_STREAM);
        var result = PastTsdbIndexCreationExecutor.sortAndRetrieveExistingBackingIndices(ds, state.projectState(projectId).metadata());
        assertThat(result, empty());
    }

    public void testSortAndRetrieve_mixedIndices() {
        String nonTsdbName = randomIndexName();
        IndexMetadata nonTsdb = IndexMetadata.builder(nonTsdbName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Instant start = Instant.parse("2024-01-16T00:00:00Z");
        Instant end = Instant.parse("2024-01-17T00:00:00Z");
        ProjectMetadata project = DataStreamTestHelper.getProjectWithDataStream(projectId, DATA_STREAM, List.of(Tuple.tuple(start, end)));
        DataStream existingDs = project.dataStreams().get(DATA_STREAM);
        DataStream dsWithMixed = existingDs.addNewBackingIndex(nonTsdb.getIndex());
        // Updated project metadata with a mixed backing index data stream
        project = ProjectMetadata.builder(project).put(nonTsdb, false).put(dsWithMixed).build();

        var result = PastTsdbIndexCreationExecutor.sortAndRetrieveExistingBackingIndices(dsWithMixed, project);
        assertThat(result, hasSize(1));
        var indexBoundaries = result.pop();
        assertThat(indexBoundaries.start(), is(start.toEpochMilli()));
        assertThat(indexBoundaries.end(), is(end.toEpochMilli()));
    }

    public void testSortAndRetrieve_returnsIndicesSortedAscendingByStart() {
        // Pass time slices in descending order; the method must return them sorted ascending.
        Instant start1 = Instant.parse("2024-01-15T00:00:00Z");
        Instant start2 = Instant.parse("2024-01-16T00:00:00Z");
        Instant start3 = Instant.parse("2024-01-17T00:00:00Z");
        ProjectMetadata project = DataStreamTestHelper.getProjectWithDataStream(
            projectId,
            DATA_STREAM,
            List.of(Tuple.tuple(start3, Instant.parse("2024-01-18T00:00:00Z")), Tuple.tuple(start1, start2), Tuple.tuple(start2, start3))
        );
        var result = PastTsdbIndexCreationExecutor.sortAndRetrieveExistingBackingIndices(project.dataStreams().get(DATA_STREAM), project);
        assertThat(result, hasSize(3));
        assertThat(result.pop().start(), is(start1.toEpochMilli()));
        assertThat(result.pop().start(), is(start2.toEpochMilli()));
        assertThat(result.pop().start(), is(start3.toEpochMilli()));
    }

    public void testSingleTimestamp_noExistingTsdbIndices() throws Exception {
        long ts = Instant.parse("2024-01-15T09:00:00Z").toEpochMilli();
        Set<Instant> covered = run(stateWithNoTsdbIndices(), ts);

        var created = captureCreatedRanges(1);
        assertThat(created.getFirst().start(), is(Instant.parse("2024-01-15T00:00:00Z").toEpochMilli()));
        assertThat(created.getFirst().end(), is(Instant.parse("2024-01-16T00:00:00Z").toEpochMilli()));
        assertThat(covered, containsInAnyOrder(Instant.ofEpochMilli(ts)));
    }

    public void testSingleTimestamp_alreadyCovered() throws Exception {
        Instant ts = Instant.parse("2024-01-15T09:00:00Z");
        Instant existingIndexStart = Instant.parse("2024-01-15T03:03:02Z");
        Instant existingIndexEnd = Instant.parse("2024-01-16T09:01:00Z");
        ClusterState state = stateWithExisting(List.of(Tuple.tuple(existingIndexStart, existingIndexEnd)));
        Set<Instant> covered = run(state, ts.toEpochMilli());

        verify(createDataStreamService, never()).createPastBackingIndex(any(), any(), any(), any(), any(), any(), any(), any(), any());
        assertThat(covered, containsInAnyOrder(ts));
    }

    public void testTwoTimestamps_sameDayBothUncovered_oneIndexCreated() throws Exception {
        Instant ts1 = Instant.parse("2024-01-15T09:00:00Z");
        Instant ts2 = Instant.parse("2024-01-15T14:00:00Z");
        Set<Instant> covered = run(stateWithNoTsdbIndices(), ts1.toEpochMilli(), ts2.toEpochMilli());

        var created = captureCreatedRanges(1);
        assertThat(created.getFirst().start(), is(Instant.parse("2024-01-15T00:00:00Z").toEpochMilli()));
        assertThat(created.getFirst().end(), is(Instant.parse("2024-01-16T00:00:00Z").toEpochMilli()));
        assertThat(covered, containsInAnyOrder(ts1, ts2));
    }

    public void testMultipleTimestamps_disjointUncoveredDays() throws Exception {
        Instant ts1 = Instant.parse("2024-01-15T09:00:00Z");
        Instant ts2 = Instant.parse("2024-01-17T14:00:00Z");
        Set<Instant> covered = run(stateWithNoTsdbIndices(), ts1.toEpochMilli(), ts2.toEpochMilli());

        var created = captureCreatedRanges(2);
        assertThat(created.get(0).start(), is(Instant.parse("2024-01-15T00:00:00Z").toEpochMilli()));
        assertThat(created.get(0).end(), is(Instant.parse("2024-01-16T00:00:00Z").toEpochMilli()));
        assertThat(created.get(1).start(), is(Instant.parse("2024-01-17T00:00:00Z").toEpochMilli()));
        assertThat(created.get(1).end(), is(Instant.parse("2024-01-18T00:00:00Z").toEpochMilli()));
        assertThat(covered, containsInAnyOrder(ts1, ts2));
    }

    public void testMultipleTimestamps_partialCoverageAcrossDays() throws Exception {
        Instant ts1 = Instant.parse("2024-01-15T09:00:00Z");
        Instant ts2 = Instant.parse("2024-01-16T14:00:00Z"); // covered by existing index
        Instant ts3 = Instant.parse("2024-01-17T11:00:00Z");
        Instant existingIndexStart = Instant.parse("2024-01-16T00:00:00Z");
        Instant existingIndexEnd = Instant.parse("2024-01-17T00:00:00Z");
        ClusterState state = stateWithExisting(List.of(Tuple.tuple(existingIndexStart, existingIndexEnd)));
        Set<Instant> covered = run(state, ts1.toEpochMilli(), ts2.toEpochMilli(), ts3.toEpochMilli());

        var created = captureCreatedRanges(2);
        assertThat(created.get(0).start(), is(Instant.parse("2024-01-15T00:00:00Z").toEpochMilli()));
        assertThat(created.get(0).end(), is(existingIndexStart.toEpochMilli()));
        assertThat(created.get(1).start(), is(existingIndexEnd.toEpochMilli()));
        assertThat(created.get(1).end(), is(Instant.parse("2024-01-18T00:00:00Z").toEpochMilli()));
        assertThat(covered, containsInAnyOrder(ts1, ts2, ts3));
    }

    // Case 10: timestamp in gap between two non-day-aligned existing indices → bounds clamped on both sides
    public void testTimestamp_inGapBetweenNonAlignedIndices_bothSidesClamped() throws Exception {
        Instant ts = Instant.parse("2024-01-15T14:00:00Z");
        Instant firstIndexEnd = Instant.parse("2024-01-15T12:45:01Z");
        Instant lastIndexStart = Instant.parse("2024-01-15T18:34:01Z");
        ClusterState state = stateWithExisting(
            List.of(
                Tuple.tuple(Instant.parse("2024-01-14T00:00:00Z"), firstIndexEnd), // ends at noon
                Tuple.tuple(lastIndexStart, Instant.parse("2024-01-16T18:00:00Z"))  // starts at 6pm
            )
        );
        run(state, ts.toEpochMilli());

        var created = captureCreatedRanges(1);
        assertThat(created.getFirst().start(), is(firstIndexEnd.toEpochMilli()));
        assertThat(created.getFirst().end(), is(lastIndexStart.toEpochMilli()));
    }

    public void testTimestamp_precedingIndexBleedsIntoCandidateRange() throws Exception {
        Instant ts = Instant.parse("2024-01-15T09:00:00Z");
        Instant previousIndexEndTime = Instant.parse("2024-01-15T06:00:00Z");
        ClusterState state = stateWithExisting(List.of(Tuple.tuple(Instant.parse("2024-01-14T12:00:00Z"), previousIndexEndTime)));
        run(state, ts.toEpochMilli());

        var created = captureCreatedRanges(1);
        assertThat(created.getFirst().start(), is(previousIndexEndTime.toEpochMilli()));
        assertThat(created.getFirst().end(), is(Instant.parse("2024-01-16T00:00:00Z").toEpochMilli()));
    }

    public void testTimestamp_followingIndexStartsInsideCandidateRange() throws Exception {
        Instant ts = Instant.parse("2024-01-15T09:00:00Z");
        Instant nextIndexStartTime = Instant.parse("2024-01-15T18:00:00Z");
        ClusterState state = stateWithExisting(
            List.of(
                Tuple.tuple(nextIndexStartTime, Instant.parse("2024-01-16T18:00:00Z")) // starts at 6pm
            )
        );
        run(state, ts.toEpochMilli());

        var created = captureCreatedRanges(1);
        assertThat(created.getFirst().start(), is(Instant.parse("2024-01-15T00:00:00Z").toEpochMilli()));
        assertThat(created.getFirst().end(), is(nextIndexStartTime.toEpochMilli()));
    }

    public void testTimestamp_exactlyAtIndexStart() throws Exception {
        Instant ts = Instant.parse("2024-01-15T00:00:00Z");
        ClusterState state = stateWithExisting(List.of(Tuple.tuple(ts, Instant.parse("2024-01-16T00:00:00Z"))));
        Set<Instant> covered = run(state, ts.toEpochMilli());

        verify(createDataStreamService, never()).createPastBackingIndex(any(), any(), any(), any(), any(), any(), any(), any(), any());
        assertThat(covered, containsInAnyOrder(ts));
    }

    public void testTimestamp_exactlyAtIndexEnd() throws Exception {
        Instant ts = Instant.parse("2024-01-16T00:00:00Z");
        ClusterState state = stateWithExisting(List.of(Tuple.tuple(Instant.parse("2024-01-15T00:00:00Z"), ts)));
        run(state, ts.toEpochMilli());

        var created = captureCreatedRanges(1);
        assertThat(created.getFirst().start(), is(ts.toEpochMilli()));
        assertThat(created.getFirst().end(), is(Instant.parse("2024-01-17T00:00:00Z").toEpochMilli()));
    }

    // Case 19: two same-day timestamps split by an existing index in the middle → two new indices
    public void testSameDayTimestamps_splitByExistingIndex_twoIndicesCreated() throws Exception {
        Instant ts1 = Instant.parse("2024-01-15T09:00:00Z"); // morning
        Instant ts2 = Instant.parse("2024-01-15T15:00:00Z"); // afternoon
        // Existing index covers only noon–2pm on the 15th
        Instant middleIndexStartTime = Instant.parse("2024-01-15T12:00:00Z");
        Instant middleIndexEndTime = Instant.parse("2024-01-15T14:00:00Z");
        ClusterState state = stateWithExisting(List.of(Tuple.tuple(middleIndexStartTime, middleIndexEndTime)));
        Set<Instant> covered = run(state, ts1.toEpochMilli(), ts2.toEpochMilli());

        var created = captureCreatedRanges(2);
        // First index: [midnight, noon) — end trimmed by existing index's start
        assertThat(created.get(0).start(), is(Instant.parse("2024-01-15T00:00:00Z").toEpochMilli()));
        assertThat(created.get(0).end(), is(middleIndexStartTime.toEpochMilli()));
        // Second index: [2pm, 2pm+1day) — start pushed past existing index's end (lastPoppedEnd=2pm)
        assertThat(created.get(1).start(), is(middleIndexEndTime.toEpochMilli()));
        assertThat(created.get(1).end(), is(Instant.parse("2024-01-16T00:00:00Z").toEpochMilli()));
        assertThat(covered, containsInAnyOrder(ts1, ts2));
    }

    /** Builds a ClusterState with a TSDB data stream whose backing indices cover the given time ranges. */
    private ClusterState stateWithExisting(List<Tuple<Instant, Instant>> timeSlices) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(DataStreamTestHelper.getProjectWithDataStream(projectId, DATA_STREAM, timeSlices))
            .build();
    }

    /** Builds a ClusterState with a data stream that has one non-TSDB (standard) backing index. */
    private ClusterState stateWithNoTsdbIndices() {
        String indexName = DataStream.getDefaultBackingIndexName(DATA_STREAM, 1);
        IndexMetadata nonTsdb = IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DataStream ds = DataStream.builder(DATA_STREAM, List.of(nonTsdb.getIndex())).setGeneration(1).build();
        ProjectMetadata project = ProjectMetadata.builder(projectId).put(nonTsdb, false).put(ds).build();
        return ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build();
    }

    /** Runs executeTask and returns the set of covered timestamps. */
    private Set<Instant> run(ClusterState state, long... timestamps) throws Exception {
        long[] sorted = timestamps.clone();
        java.util.Arrays.sort(sorted);
        var task = new PastTsdbIndexCreationTask(DATA_STREAM, sorted, TimeValue.ZERO, ActionListener.noop());
        List<String> createdNames = new ArrayList<>();
        Set<Instant> covered = new HashSet<>();
        PastTsdbIndexCreationExecutor.executeTask(
            state,
            projectResolver,
            createDataStreamService,
            systemIndices,
            task,
            createdNames,
            covered
        );
        return covered;
    }

    /**
     * Returns the (startTime, endTime) pairs that were passed to createPastBackingIndex, in call order.
     * Also asserts the exact number of calls via Mockito verify.
     */
    private List<IndexBoundaries> captureCreatedRanges(int expectedCount) throws Exception {
        ArgumentCaptor<Instant> startCaptor = ArgumentCaptor.forClass(Instant.class);
        ArgumentCaptor<Instant> endCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(createDataStreamService, times(expectedCount)).createPastBackingIndex(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            startCaptor.capture(),
            endCaptor.capture()
        );
        List<IndexBoundaries> result = new ArrayList<>();
        for (int i = 0; i < expectedCount; i++) {
            result.add(new IndexBoundaries(startCaptor.getAllValues().get(i), endCaptor.getAllValues().get(i)));
        }
        return result;
    }
}
