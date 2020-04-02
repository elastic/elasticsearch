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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.datastream.GetDataStreamsAction.Request;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class GetDataStreamsRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(generateRandomStringArray(8, 8, false));
    }

    public void testValidateRequest() {
        GetDataStreamsAction.Request req = new GetDataStreamsAction.Request(new String[]{});
        ActionRequestValidationException e = req.validate();
        assertNull(e);
    }

    public void testGetDataStreams() {
        final String dataStreamName = "my-data-stream";
        DataStream existingDataStream = new DataStream(dataStreamName, "timestamp", Collections.emptyList());
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().dataStreams(Map.of(dataStreamName, existingDataStream)).build()).build();
        GetDataStreamsAction.Request req = new GetDataStreamsAction.Request(new String[]{dataStreamName});
        List<DataStream> dataStreams = GetDataStreamsAction.TransportAction.getDataStreams(cs, req);
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamName));
    }

    public void testGetNonexistentDataStream() {
        final String dataStreamName = "my-data-stream";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        GetDataStreamsAction.Request req = new GetDataStreamsAction.Request(new String[]{dataStreamName});
        List<DataStream> dataStreams = GetDataStreamsAction.TransportAction.getDataStreams(cs, req);
        assertThat(dataStreams.size(), equalTo(0));
    }
}
