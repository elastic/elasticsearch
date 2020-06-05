/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.datastream;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.indices.datastream.GetDataStreamAction.Request;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GetDataStreamsRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        final String searchParameter;
        switch (randomIntBetween(1, 4)) {
            case 1:
                searchParameter = randomAlphaOfLength(8);
                break;
            case 2:
                searchParameter = randomAlphaOfLength(8) + "*";
                break;
            case 3:
                searchParameter = "*";
                break;
            default:
                searchParameter = null;
                break;
        }
        return new Request(searchParameter);
    }

    public void testGetDataStream() {
        final String dataStreamName = "my-data-stream";
        IndexMetadata idx = DataStreamTestHelper.createFirstBackingIndex(dataStreamName).build();
        DataStream existingDataStream = new DataStream(dataStreamName, "timestamp", List.of(idx.getIndex()));
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().dataStreams(Map.of(dataStreamName, existingDataStream)).build()).build();
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(dataStreamName);
        List<DataStream> dataStreams = GetDataStreamAction.TransportAction.getDataStreams(cs, req);
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamName));
    }

    public void testGetDataStreamsWithWildcards() {
        final String[] dataStreamNames = {"my-data-stream", "another-data-stream"};
        IndexMetadata idx1 = DataStreamTestHelper.createFirstBackingIndex(dataStreamNames[0]).build();
        IndexMetadata idx2 = DataStreamTestHelper.createFirstBackingIndex(dataStreamNames[1]).build();

        DataStream ds1 = new DataStream(dataStreamNames[0], "timestamp", List.of(idx1.getIndex()));
        DataStream ds2 = new DataStream(dataStreamNames[1], "timestamp", List.of(idx2.getIndex()));
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().dataStreams(
                Map.of(dataStreamNames[0], ds1, dataStreamNames[1], ds2)).build())
            .build();

        GetDataStreamAction.Request req = new GetDataStreamAction.Request(dataStreamNames[1].substring(0, 5) + "*");
        List<DataStream> dataStreams = GetDataStreamAction.TransportAction.getDataStreams(cs, req);
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));

        req = new GetDataStreamAction.Request("*");
        dataStreams = GetDataStreamAction.TransportAction.getDataStreams(cs, req);
        assertThat(dataStreams.size(), equalTo(2));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));
        assertThat(dataStreams.get(1).getName(), equalTo(dataStreamNames[0]));

        req = new GetDataStreamAction.Request((String) null);
        dataStreams = GetDataStreamAction.TransportAction.getDataStreams(cs, req);
        assertThat(dataStreams.size(), equalTo(2));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));
        assertThat(dataStreams.get(1).getName(), equalTo(dataStreamNames[0]));

        req = new GetDataStreamAction.Request("matches-none*");
        dataStreams = GetDataStreamAction.TransportAction.getDataStreams(cs, req);
        assertThat(dataStreams.size(), equalTo(0));
    }

    public void testGetNonexistentDataStream() {
        final String dataStreamName = "my-data-stream";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(dataStreamName);
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class,
            () -> GetDataStreamAction.TransportAction.getDataStreams(cs, req));
        assertThat(e.getMessage(), containsString("data_stream matching [" + dataStreamName + "] not found"));
    }

}
