/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.apache.logging.log4j.message.Message;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createIndexMetadata;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UpdateTimeSeriesRangeServiceTests extends ESTestCase {

    static MockAppender appender;
    static Logger testLogger1 = LogManager.getLogger(UpdateTimeSeriesRangeService.class);

    @BeforeClass
    public static void classInit() throws IllegalAccessException {
        appender = new MockAppender("mock_appender");
        appender.start();
        Loggers.addAppender(testLogger1, appender);
    }

    @AfterClass
    public static void classCleanup() {
        Loggers.removeAppender(testLogger1, appender);
        appender.stop();
    }

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
        Instant end = now.plus(40, ChronoUnit.MINUTES);
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
            equalTo(now.plus(30, ChronoUnit.MINUTES).plus(5, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS))
        );
    }

    public void testUpdateTimeSeriesTemporalRange_customLookAHeadTime() {
        int lookAHeadTimeMinutes = randomIntBetween(30, 120);
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
        Instant end = now.plus(31, ChronoUnit.MINUTES);
        Metadata metadata = DataStreamTestHelper.getClusterStateWithDataStream(
            dataStreamName,
            List.of(new Tuple<>(start.minus(4, ChronoUnit.HOURS), start), new Tuple<>(start, end))
        ).getMetadata();
        DataStream d = metadata.dataStreams().get(dataStreamName);
        metadata = Metadata.builder(metadata)
            .put(d.copy().setReplicated(true).setBackingIndices(d.getDataComponent().copy().setRolloverOnWrite(false).build()).build())
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

        Instant start = now.minus(90, ChronoUnit.MINUTES);
        Metadata.Builder mbBuilder = new Metadata.Builder();
        for (String dataStreamName : List.of(dataStreamName1, dataStreamName2, dataStreamName3)) {
            Instant end = start.plus(30, ChronoUnit.MINUTES);
            DataStreamTestHelper.getClusterStateWithDataStream(mbBuilder, dataStreamName, List.of(new Tuple<>(start, end)));
            start = end;
        }

        now = now.minus(45, ChronoUnit.MINUTES);
        ClusterState before = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(mbBuilder).build();
        ClusterState result = instance.updateTimeSeriesTemporalRange(before, now);
        assertThat(result, not(sameInstance(before)));
        assertThat(
            getEndTime(result, dataStreamName1, 0),
            equalTo(now.plus(30, ChronoUnit.MINUTES).plus(5, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS))
        );
        assertThat(
            getEndTime(result, dataStreamName2, 0),
            equalTo(now.plus(30, ChronoUnit.MINUTES).plus(5, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS))
        );
        assertThat(getEndTime(result, dataStreamName3, 0), equalTo(start));
    }

    public void testUpdateTimeSeriesTemporalOneBadDataStream() {
        String dataStreamName1 = "logs-app1";
        String dataStreamName2 = "logs-app2-broken";
        String dataStreamName3 = "logs-app3";
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        Instant start = now.minus(90, ChronoUnit.MINUTES);
        Instant end = start.plus(30, ChronoUnit.MINUTES);
        Metadata.Builder mbBuilder = new Metadata.Builder();
        for (String dataStreamName : List.of(dataStreamName1, dataStreamName2, dataStreamName3)) {
            DataStreamTestHelper.getClusterStateWithDataStream(mbBuilder, dataStreamName, List.of(new Tuple<>(start, end)));
        }

        Settings settings = Settings.builder().put("index.mode", "logsdb").build();
        var im = createIndexMetadata(getDefaultBackingIndexName(dataStreamName2, 2, start.toEpochMilli()), true, settings, 0);
        mbBuilder.put(im, true);
        var ds2 = mbBuilder.dataStreamMetadata().dataStreams().get(dataStreamName2);
        var ds2Indices = new ArrayList<>(ds2.getIndices());
        ds2Indices.add(im.getIndex());
        var copy = new HashMap<>(mbBuilder.dataStreamMetadata().dataStreams());
        copy.put(
            dataStreamName2,
            new DataStream(
                ds2.getName(),
                ds2Indices,
                2,
                ds2.getMetadata(),
                ds2.getSettings(),
                ds2.isHidden(),
                ds2.isReplicated(),
                ds2.isSystem(),
                ds2.isAllowCustomRouting(),
                ds2.getIndexMode(),
                ds2.getDataLifecycle(),
                ds2.getDataStreamOptions(),
                ds2.getFailureIndices(),
                ds2.rolloverOnWrite(),
                ds2.getAutoShardingEvent()
            )
        );
        mbBuilder.dataStreams(copy, Map.of());

        now = now.minus(45, ChronoUnit.MINUTES);
        ClusterState before = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(mbBuilder).build();
        ClusterState result = instance.updateTimeSeriesTemporalRange(before, now);
        assertThat(result, not(sameInstance(before)));
        final var expectedEndTime = now.plus(35, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS);
        assertThat(getEndTime(result, dataStreamName1, 0), equalTo(expectedEndTime));
        assertThat(getEndTime(result, dataStreamName2, 0), equalTo(end)); // failed to update end_time, because broken data stream
        assertThat(getEndTime(result, dataStreamName3, 0), equalTo(expectedEndTime));

        String message = appender.getLastEventAndReset().getMessage().getFormattedMessage();
        assertThat(
            message,
            equalTo(
                "unable to update [index.time_series.end_time] for data stream [logs-app2-broken] and "
                    + "backing index ["
                    + im.getIndex().getName()
                    + "]"
            )
        );
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

    static class MockAppender extends AbstractAppender {
        public LogEvent lastEvent;

        MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null, false);
        }

        @Override
        public void append(LogEvent event) {
            lastEvent = event.toImmutable();
        }

        Message lastMessage() {
            return lastEvent.getMessage();
        }

        public LogEvent getLastEventAndReset() {
            LogEvent toReturn = lastEvent;
            lastEvent = null;
            return toReturn;
        }
    }

}
