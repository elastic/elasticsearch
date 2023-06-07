/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.getClusterStateWithDataStreams;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class GetDataStreamsTransportActionTests extends ESTestCase {

    private final IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();
    private final SystemIndices systemIndices = new SystemIndices(List.of());

    public void testGetDataStream() {
        final String dataStreamName = "my-data-stream";
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(dataStreamName, 1)), List.of());
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(new String[] { dataStreamName });
        List<DataStream> dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamName));
    }

    public void testGetDataStreamsWithWildcards() {
        final String[] dataStreamNames = { "my-data-stream", "another-data-stream" };
        ClusterState cs = getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamNames[0], 1), new Tuple<>(dataStreamNames[1], 1)),
            List.of()
        );

        GetDataStreamAction.Request req = new GetDataStreamAction.Request(new String[] { dataStreamNames[1].substring(0, 5) + "*" });
        List<DataStream> dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));

        req = new GetDataStreamAction.Request(new String[] { "*" });
        dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams.size(), equalTo(2));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));
        assertThat(dataStreams.get(1).getName(), equalTo(dataStreamNames[0]));

        req = new GetDataStreamAction.Request((String[]) null);
        dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams.size(), equalTo(2));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));
        assertThat(dataStreams.get(1).getName(), equalTo(dataStreamNames[0]));

        req = new GetDataStreamAction.Request(new String[] { "matches-none*" });
        dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams.size(), equalTo(0));
    }

    public void testGetDataStreamsWithoutWildcards() {
        final String[] dataStreamNames = { "my-data-stream", "another-data-stream" };
        ClusterState cs = getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamNames[0], 1), new Tuple<>(dataStreamNames[1], 1)),
            List.of()
        );

        GetDataStreamAction.Request req = new GetDataStreamAction.Request(new String[] { dataStreamNames[0], dataStreamNames[1] });
        List<DataStream> dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams.size(), equalTo(2));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));
        assertThat(dataStreams.get(1).getName(), equalTo(dataStreamNames[0]));

        req = new GetDataStreamAction.Request(new String[] { dataStreamNames[1] });
        dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));

        req = new GetDataStreamAction.Request(new String[] { dataStreamNames[0] });
        dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[0]));

        GetDataStreamAction.Request req2 = new GetDataStreamAction.Request(new String[] { "foo" });
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> GetDataStreamsTransportAction.getDataStreams(cs, resolver, req2)
        );
        assertThat(e.getMessage(), containsString("no such index [foo]"));
    }

    public void testGetNonexistentDataStream() {
        final String dataStreamName = "my-data-stream";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(new String[] { dataStreamName });
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> GetDataStreamsTransportAction.getDataStreams(cs, resolver, req)
        );
        assertThat(e.getMessage(), containsString("no such index [" + dataStreamName + "]"));
    }

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

        var req = new GetDataStreamAction.Request(new String[] {});
        var response = GetDataStreamsTransportAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings()
        );
        assertThat(response.getDataStreams(), hasSize(2));
        assertThat(response.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStream1));
        assertThat(response.getDataStreams().get(0).getTimeSeries().temporalRanges(), contains(new Tuple<>(sixHoursAgo, twoHoursAhead)));
        assertThat(response.getDataStreams().get(1).getDataStream().getName(), equalTo(dataStream2));
        assertThat(response.getDataStreams().get(1).getTimeSeries().temporalRanges(), contains(new Tuple<>(sixHoursAgo, twoHoursAhead)));

        // Remove the middle backing index first data stream, so that there is time gap in the data stream:
        {
            Metadata.Builder mBuilder = Metadata.builder(state.getMetadata());
            DataStream dataStream = state.getMetadata().dataStreams().get(dataStream1);
            mBuilder.put(dataStream.removeBackingIndex(dataStream.getIndices().get(1)));
            mBuilder.remove(dataStream.getIndices().get(1).getName());
            state = ClusterState.builder(state).metadata(mBuilder).build();
        }
        response = GetDataStreamsTransportAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings()
        );
        assertThat(response.getDataStreams(), hasSize(2));
        assertThat(response.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStream1));
        assertThat(
            response.getDataStreams().get(0).getTimeSeries().temporalRanges(),
            contains(new Tuple<>(sixHoursAgo, fourHoursAgo), new Tuple<>(twoHoursAgo, twoHoursAhead))
        );
        assertThat(response.getDataStreams().get(1).getDataStream().getName(), equalTo(dataStream2));
        assertThat(response.getDataStreams().get(1).getTimeSeries().temporalRanges(), contains(new Tuple<>(sixHoursAgo, twoHoursAhead)));
    }


    public void testGetTimeSeriesMixedDataStream() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String dataStream1 = "ds-1";
        Instant twoHoursAgo = now.minus(2, ChronoUnit.HOURS);
        Instant twoHoursAhead = now.plus(2, ChronoUnit.HOURS);

        ClusterState state;
        {
            var mBuilder = new Metadata.Builder();
            DataStreamTestHelper.getClusterStateWithDataStreams(
                mBuilder,
                List.of(Tuple.tuple(dataStream1, 2)),
                List.of(),
                now.toEpochMilli(),
                Settings.EMPTY,
                0,
                false
            );
            DataStreamTestHelper.getClusterStateWithDataStream(mBuilder, dataStream1, List.of(new Tuple<>(twoHoursAgo, twoHoursAhead)));
            state = ClusterState.builder(new ClusterName("_name")).metadata(mBuilder).build();
        }

        var req = new GetDataStreamAction.Request(new String[] {});
        var response = GetDataStreamsTransportAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings()
        );
        assertThat(
            response.getDataStreams(),
            contains(
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream1)),
                    transformedMatch(
                        d -> d.getDataStream().getIndices().stream().map(Index::getName).toList(),
                        contains(".ds-ds-1-2023.06.06-000001", ".ds-ds-1-2023.06.06-000002", ".ds-ds-1-2023.06.06-000003")
                    ),
                    transformedMatch(d -> d.getTimeSeries().temporalRanges(), contains(new Tuple<>(twoHoursAgo, twoHoursAhead)))
                )
            )
        );
    }
}
