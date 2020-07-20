/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.cluster.DataStreamTestHelper;
import org.elasticsearch.xpack.core.action.GetDataStreamAction.Response;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.List;

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
            dataStreams.add(
                new Response.DataStreamInfo(
                    DataStreamTestHelper.randomInstance(),
                    ClusterHealthStatus.GREEN,
                    randomAlphaOfLengthBetween(2, 10),
                    randomAlphaOfLengthBetween(2, 10)
                )
            );
        }
        return new Response(dataStreams);
    }
}
