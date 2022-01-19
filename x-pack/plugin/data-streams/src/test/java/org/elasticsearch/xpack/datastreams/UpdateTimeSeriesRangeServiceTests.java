/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class UpdateTimeSeriesRangeServiceTests extends ESTestCase {

    private UpdateTimeSeriesRangeService instance;

    @Before
    public void createInstance() {
        instance = new UpdateTimeSeriesRangeService(Settings.EMPTY, null, null);
    }

    public void testUpdateTimeSeriesTemporalRange() {
        String dataStreamName = "logs-app1";
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Instant start = now.minus(2, ChronoUnit.HOURS);
        Instant end = now.plus(3, ChronoUnit.HOURS);
        Metadata metadata = DataStreamTestHelper.getClusterStateWithDataStream(
            dataStreamName,
            List.of(new Tuple<>(start.minus(4, ChronoUnit.HOURS), start), new Tuple<>(start, end))
        ).getMetadata();

        // noop, because current end_time isn't passed now + look_a_head_time + poll_interval
        ClusterState in = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        ClusterState result = instance.updateTimeSeriesTemporalRange(in, now);
        assertThat(result, sameInstance(in));
        Instant previousEndTime1 = getEndTime(result, dataStreamName, 0);
        Instant previousEndTime2 = getEndTime(result, dataStreamName, 1);

        // updates end time of most recent backing index only, because current time is passed current end_time + look_a_head_time and
        // poll_interval
        now = now.plus(1, ChronoUnit.HOURS);
        in = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        result = instance.updateTimeSeriesTemporalRange(in, now);
        assertThat(result, not(sameInstance(in)));
        assertThat(getEndTime(result, dataStreamName, 0), equalTo(previousEndTime1));
        assertThat(getEndTime(result, dataStreamName, 1), not(equalTo(previousEndTime2)));
        assertThat(getEndTime(result, dataStreamName, 1), equalTo(now.plus(2, ChronoUnit.HOURS).plus(1, ChronoUnit.MINUTES)));
    }

    public void testUpdateTimeSeriesTemporalRange_NoUpdateBecauseReplicated() {
        String dataStreamName = "logs-app1";
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Instant start = now.minus(2, ChronoUnit.HOURS);
        Instant end = now.plus(3, ChronoUnit.HOURS);
        Metadata metadata = DataStreamTestHelper.getClusterStateWithDataStream(
            dataStreamName,
            List.of(new Tuple<>(start.minus(4, ChronoUnit.HOURS), start), new Tuple<>(start, end))
        ).getMetadata();
        DataStream d = metadata.dataStreams().get(dataStreamName);
        metadata = Metadata.builder(metadata)
            .put(
                new DataStream(
                    d.getName(),
                    d.getTimeStampField(),
                    d.getIndices(),
                    d.getGeneration(),
                    d.getMetadata(),
                    d.isHidden(),
                    true,
                    d.isSystem(),
                    d.isAllowCustomRouting()
                )
            )
            .build();

        now = now.plus(1, ChronoUnit.HOURS);
        ClusterState in = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        ClusterState result = instance.updateTimeSeriesTemporalRange(in, now);
        assertThat(result, sameInstance(in));
    }

    public void testUpdateTimeSeriesTemporalRange_NoUpdateBecauseRegularDataStream() {
        String dataStreamName = "logs-app1";
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Metadata metadata = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>(dataStreamName, 2)), List.of())
            .getMetadata();

        now = now.plus(1, ChronoUnit.HOURS);
        ClusterState in = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        ClusterState result = instance.updateTimeSeriesTemporalRange(in, now);
        assertThat(result, sameInstance(in));
    }

    public void testUpdateTimeSeriesTemporalRangeMultipleDataStream() {
        String dataStreamName1 = "logs-app1";
        String dataStreamName2 = "logs-app2";
        String dataStreamName3 = "logs-app3";
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        Instant start = now.minus(6, ChronoUnit.HOURS);
        Metadata.Builder mbBuilder = new Metadata.Builder();
        for (String dataStreamName : List.of(dataStreamName1, dataStreamName2, dataStreamName3)) {
            Instant end = start.plus(2, ChronoUnit.HOURS);
            DataStreamTestHelper.getClusterStateWithDataStream(mbBuilder, dataStreamName, List.of(new Tuple<>(start, end)));
            start = end;
        }

        now = now.minus(3, ChronoUnit.HOURS);
        ClusterState before = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(mbBuilder).build();
        ClusterState result = instance.updateTimeSeriesTemporalRange(before, now);
        assertThat(result, not(sameInstance(before)));
        assertThat(getEndTime(result, dataStreamName1, 0), equalTo(now.plus(2, ChronoUnit.HOURS).plus(1, ChronoUnit.MINUTES)));
        assertThat(getEndTime(result, dataStreamName2, 0), equalTo(now.plus(2, ChronoUnit.HOURS).plus(1, ChronoUnit.MINUTES)));
        assertThat(getEndTime(result, dataStreamName3, 0), equalTo(start));
    }

    static Instant getEndTime(ClusterState state, String dataStreamName, int index) {
        DataStream dataStream = state.getMetadata().dataStreams().get(dataStreamName);
        Settings indexSettings = state.getMetadata().index(dataStream.getIndices().get(index)).getSettings();
        return IndexSettings.TIME_SERIES_END_TIME.get(indexSettings);
    }

}
