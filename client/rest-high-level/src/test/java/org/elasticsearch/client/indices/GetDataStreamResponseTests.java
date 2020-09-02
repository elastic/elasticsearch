/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction.Response.DataStreamInfo;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;

public class GetDataStreamResponseTests extends AbstractResponseTestCase<GetDataStreamAction.Response, GetDataStreamResponse> {

    private static List<Index> randomIndexInstances() {
        int numIndices = randomIntBetween(0, 128);
        List<Index> indices = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            indices.add(new Index(randomAlphaOfLength(10).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random())));
        }
        return indices;
    }

    private static DataStreamInfo randomInstance() {
        List<Index> indices = randomIndexInstances();
        long generation = indices.size() + randomLongBetween(1, 128);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        indices.add(new Index(getDefaultBackingIndexName(dataStreamName, generation), UUIDs.randomBase64UUID(random())));
        DataStream dataStream = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices, generation);
        return new DataStreamInfo(dataStream, ClusterHealthStatus.YELLOW, randomAlphaOfLengthBetween(2, 10),
            randomAlphaOfLengthBetween(2, 10));
    }

    @Override
    protected GetDataStreamAction.Response createServerTestInstance(XContentType xContentType) {
        ArrayList<DataStreamInfo> dataStreams = new ArrayList<>();
        int count = randomInt(10);
        for (int i = 0; i < count; i++) {
            dataStreams.add(randomInstance());
        }
        return new GetDataStreamAction.Response(dataStreams);
    }

    @Override
    protected GetDataStreamResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetDataStreamResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(GetDataStreamAction.Response serverTestInstance, GetDataStreamResponse clientInstance) {
        assertEquals(serverTestInstance.getDataStreams().size(), clientInstance.getDataStreams().size());
        Iterator<DataStreamInfo> serverIt = serverTestInstance.getDataStreams().iterator();

        Iterator<org.elasticsearch.client.indices.DataStream> clientIt = clientInstance.getDataStreams().iterator();
        while (serverIt.hasNext()) {
            org.elasticsearch.client.indices.DataStream client = clientIt.next();
            DataStream server = serverIt.next().getDataStream();
            assertEquals(server.getName(), client.getName());
            assertEquals(server.getIndices().stream().map(Index::getName).collect(Collectors.toList()), client.getIndices());
            assertEquals(server.getTimeStampField().getName(), client.getTimeStampField());
            assertEquals(server.getGeneration(), client.getGeneration());
        }
    }
}
