/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.datastreams.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;

import java.util.List;

import static org.elasticsearch.cluster.DataStreamTestHelper.getClusterStateWithDataStreams;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GetDataStreamsTransportActionTests extends ESTestCase {

    public void testGetDataStream() {
        final String dataStreamName = "my-data-stream";
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(dataStreamName, 1)), List.of());
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(new String[] { dataStreamName });
        List<DataStream> dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, new IndexNameExpressionResolver(), req);
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
        List<DataStream> dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, new IndexNameExpressionResolver(), req);
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));

        req = new GetDataStreamAction.Request(new String[] { "*" });
        dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, new IndexNameExpressionResolver(), req);
        assertThat(dataStreams.size(), equalTo(2));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));
        assertThat(dataStreams.get(1).getName(), equalTo(dataStreamNames[0]));

        req = new GetDataStreamAction.Request((String[]) null);
        dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, new IndexNameExpressionResolver(), req);
        assertThat(dataStreams.size(), equalTo(2));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));
        assertThat(dataStreams.get(1).getName(), equalTo(dataStreamNames[0]));

        req = new GetDataStreamAction.Request(new String[] { "matches-none*" });
        dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, new IndexNameExpressionResolver(), req);
        assertThat(dataStreams.size(), equalTo(0));
    }

    public void testGetDataStreamsWithoutWildcards() {
        final String[] dataStreamNames = { "my-data-stream", "another-data-stream" };
        ClusterState cs = getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamNames[0], 1), new Tuple<>(dataStreamNames[1], 1)),
            List.of()
        );

        GetDataStreamAction.Request req = new GetDataStreamAction.Request(new String[] { dataStreamNames[0], dataStreamNames[1] });
        List<DataStream> dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, new IndexNameExpressionResolver(), req);
        assertThat(dataStreams.size(), equalTo(2));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));
        assertThat(dataStreams.get(1).getName(), equalTo(dataStreamNames[0]));

        req = new GetDataStreamAction.Request(new String[] { dataStreamNames[1] });
        dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, new IndexNameExpressionResolver(), req);
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));

        req = new GetDataStreamAction.Request(new String[] { dataStreamNames[0] });
        dataStreams = GetDataStreamsTransportAction.getDataStreams(cs, new IndexNameExpressionResolver(), req);
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[0]));

        GetDataStreamAction.Request req2 = new GetDataStreamAction.Request(new String[] { "foo" });
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> GetDataStreamsTransportAction.getDataStreams(cs, new IndexNameExpressionResolver(), req2)
        );
        assertThat(e.getMessage(), containsString("no such index [foo]"));
    }

    public void testGetNonexistentDataStream() {
        final String dataStreamName = "my-data-stream";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(new String[] { dataStreamName });
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> GetDataStreamsTransportAction.getDataStreams(cs, new IndexNameExpressionResolver(), req)
        );
        assertThat(e.getMessage(), containsString("no such index [" + dataStreamName + "]"));
    }

}
