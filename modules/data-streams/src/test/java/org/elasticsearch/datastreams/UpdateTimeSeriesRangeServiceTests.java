/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UpdateTimeSeriesRangeServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private UpdateTimeSeriesRangeService instance;

    @Before
    public void createInstance() {
        ClusterService mockClusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL));
        when(mockClusterService.getClusterSettings()).thenReturn(clusterSettings);
        threadPool = new TestThreadPool(getTestName());
        instance = new UpdateTimeSeriesRangeService(Settings.EMPTY, threadPool, mockClusterService);
    }

    @After
    public void cleanup() throws Exception {
        instance.doClose();
        terminate(threadPool);
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
        Instant previousStartTime1 = getStartTime(result, dataStreamName, 0);
        Instant previousEndTime1 = getEndTime(result, dataStreamName, 0);
        Instant previousStartTime2 = getStartTime(result, dataStreamName, 1);
        Instant previousEndTime2 = getEndTime(result, dataStreamName, 1);

        // updates end time of most recent backing index only, because current time is passed current end_time + look_a_head_time and
        // poll_interval
        now = now.plus(1, ChronoUnit.HOURS);
        in = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        result = instance.updateTimeSeriesTemporalRange(in, now);
        assertThat(result, not(sameInstance(in)));
        assertThat(getStartTime(result, dataStreamName, 0), equalTo(previousStartTime1));
        assertThat(getEndTime(result, dataStreamName, 0), equalTo(previousEndTime1));
        assertThat(getStartTime(result, dataStreamName, 1), equalTo(previousStartTime2));
        assertThat(getEndTime(result, dataStreamName, 1), not(equalTo(previousEndTime2)));
        assertThat(
            getEndTime(result, dataStreamName, 1),
            equalTo(now.plus(2, ChronoUnit.HOURS).plus(5, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS))
        );
    }

    public void testUpdateTimeSeriesTemporalRange_customLookAHeadTime() {
        int lookAHeadTimeMinutes = randomIntBetween(30, 180);
        TemporalAmount lookAHeadTime = Duration.ofMinutes(lookAHeadTimeMinutes);
        int timeSeriesPollIntervalMinutes = randomIntBetween(1, 10);
        TemporalAmount timeSeriesPollInterval = Duration.ofMinutes(timeSeriesPollIntervalMinutes);
        instance.setPollInterval(TimeValue.timeValueMinutes(timeSeriesPollIntervalMinutes));

        String dataStreamName = "logs-app1";
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Instant start = now.minus(2, ChronoUnit.HOURS);
        Instant end = now.plus(1, ChronoUnit.HOURS);
        Metadata metadata = DataStreamTestHelper.getClusterStateWithDataStream(
            dataStreamName,
            List.of(new Tuple<>(start.minus(4, ChronoUnit.HOURS), start), new Tuple<>(start, end))
        ).getMetadata();
        metadata = Metadata.builder(metadata)
            .updateSettings(Settings.builder().put(DataStreamsPlugin.LOOK_AHEAD_TIME.getKey(), lookAHeadTimeMinutes + "m").build())
            .build();

        var in = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        Instant previousStartTime1 = getStartTime(in, dataStreamName, 0);
        Instant previousEndTime1 = getEndTime(in, dataStreamName, 0);
        Instant previousStartTime2 = getStartTime(in, dataStreamName, 1);

        now = now.plus(1, ChronoUnit.HOURS);
        var result = instance.updateTimeSeriesTemporalRange(in, now);
        assertThat(result, not(sameInstance(in)));
        assertThat(getStartTime(result, dataStreamName, 0), equalTo(previousStartTime1));
        assertThat(getEndTime(result, dataStreamName, 0), equalTo(previousEndTime1));
        assertThat(getStartTime(result, dataStreamName, 1), equalTo(previousStartTime2));
        assertThat(
            getEndTime(result, dataStreamName, 1),
            equalTo(now.plus(lookAHeadTime).plus(timeSeriesPollInterval).truncatedTo(ChronoUnit.SECONDS))
        );
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
                    d.getIndices(),
                    d.getGeneration(),
                    d.getMetadata(),
                    d.isHidden(),
                    true,
                    d.isSystem(),
                    d.isAllowCustomRouting(),
                    d.getIndexMode(),
                    d.getLifecycle()
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
        assertThat(
            getEndTime(result, dataStreamName1, 0),
            equalTo(now.plus(2, ChronoUnit.HOURS).plus(5, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS))
        );
        assertThat(
            getEndTime(result, dataStreamName2, 0),
            equalTo(now.plus(2, ChronoUnit.HOURS).plus(5, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS))
        );
        assertThat(getEndTime(result, dataStreamName3, 0), equalTo(start));
    }

    public void testUpdatePollInterval() {
        instance.scheduleTask();
        assertThat(instance.pollInterval, equalTo(TimeValue.timeValueMinutes(5)));
        assertThat(instance.job.toString(), containsString("5m"));
        instance.setPollInterval(TimeValue.timeValueMinutes(1));
        assertThat(instance.pollInterval, equalTo(TimeValue.timeValueMinutes(1)));
        assertThat(instance.job.toString(), containsString("1m"));
    }

    public void testUpdatePollIntervalUnscheduled() {
        assertThat(instance.pollInterval, equalTo(TimeValue.timeValueMinutes(5)));
        assertThat(instance.job, nullValue());
        instance.setPollInterval(TimeValue.timeValueMinutes(1));
        assertThat(instance.pollInterval, equalTo(TimeValue.timeValueMinutes(1)));
        assertThat(instance.job, nullValue());
    }

    static Instant getEndTime(ClusterState state, String dataStreamName, int index) {
        DataStream dataStream = state.getMetadata().dataStreams().get(dataStreamName);
        Settings indexSettings = state.getMetadata().index(dataStream.getIndices().get(index)).getSettings();
        return IndexSettings.TIME_SERIES_END_TIME.get(indexSettings);
    }

    static Instant getStartTime(ClusterState state, String dataStreamName, int index) {
        DataStream dataStream = state.getMetadata().dataStreams().get(dataStreamName);
        Settings indexSettings = state.getMetadata().index(dataStream.getIndices().get(index)).getSettings();
        return IndexSettings.TIME_SERIES_START_TIME.get(indexSettings);
    }

}
