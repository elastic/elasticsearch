/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.datastreams.DataStreamsActionUtil;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TransportGetDataStreamsActionTests extends ESTestCase {

    private final IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();
    private final SystemIndices systemIndices = new SystemIndices(List.of());
    private final DataStreamGlobalRetentionSettings dataStreamGlobalRetentionSettings = DataStreamGlobalRetentionSettings.create(
        ClusterSettings.createBuiltInClusterSettings()
    );

    public void testGetTimeSeriesDataStream() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String dataStream1 = "ds-1";
        String dataStream2 = "ds-2";
        Instant sixHoursAgo = now.minus(6, ChronoUnit.HOURS);
        Instant fourHoursAgo = now.minus(4, ChronoUnit.HOURS);
        Instant twoHoursAgo = now.minus(2, ChronoUnit.HOURS);
        Instant twoHoursAhead = now.plus(2, ChronoUnit.HOURS);

        ClusterState state;
        {
            var mBuilder = new Metadata.Builder();
            DataStreamTestHelper.getClusterStateWithDataStream(
                mBuilder,
                dataStream1,
                List.of(
                    new Tuple<>(sixHoursAgo, fourHoursAgo),
                    new Tuple<>(fourHoursAgo, twoHoursAgo),
                    new Tuple<>(twoHoursAgo, twoHoursAhead)
                )
            );
            DataStreamTestHelper.getClusterStateWithDataStream(
                mBuilder,
                dataStream2,
                List.of(
                    new Tuple<>(sixHoursAgo, fourHoursAgo),
                    new Tuple<>(fourHoursAgo, twoHoursAgo),
                    new Tuple<>(twoHoursAgo, twoHoursAhead)
                )
            );
            state = ClusterState.builder(new ClusterName("_name")).metadata(mBuilder).build();
        }

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = getResponse(state, req);
        assertThat(
            response.getDataStreams(),
            contains(
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream1)),
                    transformedMatch(d -> d.getTimeSeries().temporalRanges(), contains(new Tuple<>(sixHoursAgo, twoHoursAhead)))
                ),
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream2)),
                    transformedMatch(d -> d.getTimeSeries().temporalRanges(), contains(new Tuple<>(sixHoursAgo, twoHoursAhead)))
                )
            )
        );

        // Remove the middle backing index first data stream, so that there is time gap in the data stream:
        {
            Metadata.Builder mBuilder = Metadata.builder(state.getMetadata());
            DataStream dataStream = state.getMetadata().dataStreams().get(dataStream1);
            mBuilder.put(dataStream.removeBackingIndex(dataStream.getIndices().get(1)));
            mBuilder.remove(dataStream.getIndices().get(1).getName());
            state = ClusterState.builder(state).metadata(mBuilder).build();
        }
        response = getResponse(state, req);
        assertThat(
            response.getDataStreams(),
            contains(
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream1)),
                    transformedMatch(
                        d -> d.getTimeSeries().temporalRanges(),
                        contains(new Tuple<>(sixHoursAgo, fourHoursAgo), new Tuple<>(twoHoursAgo, twoHoursAhead))
                    )
                ),
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream2)),
                    transformedMatch(d -> d.getTimeSeries().temporalRanges(), contains(new Tuple<>(sixHoursAgo, twoHoursAhead)))
                )
            )
        );
    }

    public void testGetTimeSeriesDataStreamWithOutOfOrderIndices() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String dataStream = "ds-1";
        Instant sixHoursAgo = now.minus(6, ChronoUnit.HOURS);
        Instant fourHoursAgo = now.minus(4, ChronoUnit.HOURS);
        Instant twoHoursAgo = now.minus(2, ChronoUnit.HOURS);
        Instant twoHoursAhead = now.plus(2, ChronoUnit.HOURS);

        ClusterState state;
        {
            var mBuilder = new Metadata.Builder();
            DataStreamTestHelper.getClusterStateWithDataStream(
                mBuilder,
                dataStream,
                List.of(
                    new Tuple<>(fourHoursAgo, twoHoursAgo),
                    new Tuple<>(sixHoursAgo, fourHoursAgo),
                    new Tuple<>(twoHoursAgo, twoHoursAhead)
                )
            );
            state = ClusterState.builder(new ClusterName("_name")).metadata(mBuilder).build();
        }

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = getResponse(state, req);
        assertThat(
            response.getDataStreams(),
            contains(
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream)),
                    transformedMatch(d -> d.getTimeSeries().temporalRanges(), contains(new Tuple<>(sixHoursAgo, twoHoursAhead)))
                )
            )
        );
    }

    public void testGetTimeSeriesMixedDataStream() {
        Instant instant = Instant.parse("2023-06-06T14:00:00.000Z").truncatedTo(ChronoUnit.SECONDS);
        String dataStream1 = "ds-1";
        Instant twoHoursAgo = instant.minus(2, ChronoUnit.HOURS);
        Instant twoHoursAhead = instant.plus(2, ChronoUnit.HOURS);

        ClusterState state;
        {
            var mBuilder = new Metadata.Builder();
            DataStreamTestHelper.getClusterStateWithDataStreams(
                mBuilder,
                List.of(Tuple.tuple(dataStream1, 2)),
                List.of(),
                instant.toEpochMilli(),
                Settings.EMPTY,
                0,
                false,
                false
            );
            DataStreamTestHelper.getClusterStateWithDataStream(mBuilder, dataStream1, List.of(new Tuple<>(twoHoursAgo, twoHoursAhead)));
            state = ClusterState.builder(new ClusterName("_name")).metadata(mBuilder).build();
        }

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = getResponse(state, req);

        var name1 = DataStream.getDefaultBackingIndexName("ds-1", 1, instant.toEpochMilli());
        var name2 = DataStream.getDefaultBackingIndexName("ds-1", 2, instant.toEpochMilli());
        var name3 = DataStream.getDefaultBackingIndexName("ds-1", 3, twoHoursAgo.toEpochMilli());
        assertThat(
            response.getDataStreams(),
            contains(
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream1)),
                    transformedMatch(
                        d -> d.getDataStream().getIndices().stream().map(Index::getName).toList(),
                        contains(name1, name2, name3)
                    ),
                    transformedMatch(d -> d.getTimeSeries().temporalRanges(), contains(new Tuple<>(twoHoursAgo, twoHoursAhead)))
                )
            )
        );
    }

    public void testPassingGlobalRetention() {
        ClusterState state;
        {
            var mBuilder = new Metadata.Builder();
            DataStreamTestHelper.getClusterStateWithDataStreams(
                mBuilder,
                List.of(Tuple.tuple("data-stream-1", 2)),
                List.of(),
                System.currentTimeMillis(),
                Settings.EMPTY,
                0,
                false,
                false
            );
            state = ClusterState.builder(new ClusterName("_name")).metadata(mBuilder).build();
        }

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = getResponse(state, req);
        assertThat(response.getGlobalRetention(), nullValue());
        DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(
            TimeValue.timeValueDays(randomIntBetween(1, 5)),
            TimeValue.timeValueDays(randomIntBetween(5, 10))
        );
        DataStreamGlobalRetentionSettings withGlobalRetentionSettings = DataStreamGlobalRetentionSettings.create(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder()
                    .put(
                        DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(),
                        globalRetention.defaultRetention()
                    )
                    .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), globalRetention.maxRetention())
                    .build()
            )
        );
        response = getResponse(state, req, withGlobalRetentionSettings);
        assertThat(response.getGlobalRetention(), equalTo(globalRetention));
    }

    private GetDataStreamAction.Response getResponse(ClusterState state, GetDataStreamAction.Request req) {
        return getResponse(state, req, dataStreamGlobalRetentionSettings);
    }

    private GetDataStreamAction.Response getResponse(
        ClusterState state,
        GetDataStreamAction.Request req,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        List<String> dataStreamNames = DataStreamsActionUtil.getDataStreamNames(resolver, state, req.getNames(), req.indicesOptions());
        return TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            dataStreamNames,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            globalRetentionSettings,
            null
        );
    }
}
