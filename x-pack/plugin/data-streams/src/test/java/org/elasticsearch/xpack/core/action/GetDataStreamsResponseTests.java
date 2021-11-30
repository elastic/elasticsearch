/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.GetDataStreamAction.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GetDataStreamsResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        int numDataStreams = randomIntBetween(0, 8);
        List<Response.DataStreamInfo> dataStreams = new ArrayList<>();
        for (int i = 0; i < numDataStreams; i++) {
            DataStream dataStream = DataStreamTestHelper.randomInstance();
            dataStreams.add(
                new Response.DataStreamInfo(
                    dataStream,
                    ClusterHealthStatus.GREEN,
                    randomAlphaOfLengthBetween(2, 10),
                    randomAlphaOfLengthBetween(2, 10),
                    dataStream.getIndices().stream().map(GetDataStreamsResponseTests::createIndexMetadata).collect(Collectors.toList())
                )
            );
        }
        return new Response(dataStreams);
    }

    private static IndexMetadata createIndexMetadata(Index index) {
        return new IndexMetadata.Builder(index.getName()).settings(
            settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
        ).numberOfShards(1).numberOfReplicas(0).build();
    }
}
