/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction.Response.DataStreamInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Collectors;

public class GetDataStreamResponseTests extends AbstractResponseTestCase<GetDataStreamAction.Response, GetDataStreamResponse> {

    private static DataStreamInfo randomInstance() {
        DataStream dataStream = DataStreamTestHelper.randomInstance();
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
