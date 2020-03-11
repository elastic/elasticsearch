/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.logstash;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.logstash.action.DeletePipelineRequest;
import org.elasticsearch.xpack.logstash.action.DeletePipelineResponse;
import org.elasticsearch.xpack.logstash.action.GetPipelineRequest;
import org.elasticsearch.xpack.logstash.action.GetPipelineResponse;
import org.elasticsearch.xpack.logstash.action.PutPipelineRequest;
import org.elasticsearch.xpack.logstash.action.PutPipelineResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PipelineRequestResponseSerializationTests extends ESTestCase {

    public void testGetPipelineRequestSerialization() throws IOException {
        GetPipelineRequest request = new GetPipelineRequest(randomList(0, 50, () -> randomAlphaOfLengthBetween(2, 10)));
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        GetPipelineRequest serialized = new GetPipelineRequest(out.bytes().streamInput());
        assertEquals(request.ids(), serialized.ids());
    }

    public void testGetPipelineResponseSerialization() throws IOException {
        final int numPipelines = randomIntBetween(1, 10);
        final Map<String, BytesReference> map = new HashMap<>(numPipelines);
        for (int i = 0; i < numPipelines; i++) {
            final String name = randomAlphaOfLengthBetween(2, 10);
            final BytesReference ref = new BytesArray(randomByteArrayOfLength(randomIntBetween(1, 16)));
            map.put(name, ref);
        }
        GetPipelineResponse response = new GetPipelineResponse(map);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        GetPipelineResponse serializedResponse = new GetPipelineResponse(out.bytes().streamInput());
        assertEquals(response.pipelines(), serializedResponse.pipelines());
    }

    public void testPutPipelineRequestSerialization() throws IOException {
        PutPipelineRequest request = new PutPipelineRequest(
            randomAlphaOfLength(2),
            randomAlphaOfLengthBetween(10, 100),
            randomFrom(XContentType.values())
        );
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        PutPipelineRequest serializedRequest = new PutPipelineRequest(out.bytes().streamInput());
        assertEquals(request.id(), serializedRequest.id());
        assertEquals(request.source(), serializedRequest.source());
        assertEquals(request.xContentType(), serializedRequest.xContentType());
    }

    public void testPutPipelineResponseSerialization() throws IOException {
        PutPipelineResponse response = new PutPipelineResponse(randomFrom(RestStatus.OK, RestStatus.CREATED));
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        PutPipelineResponse serializedResponse = new PutPipelineResponse(out.bytes().streamInput());
        assertEquals(response.status(), serializedResponse.status());
    }

    public void testDeletePipelineRequestSerialization() throws IOException {
        DeletePipelineRequest request = new DeletePipelineRequest(randomAlphaOfLengthBetween(2, 10));
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        DeletePipelineRequest serializedRequest = new DeletePipelineRequest(out.bytes().streamInput());
        assertEquals(request.id(), serializedRequest.id());
    }

    public void testDeletePipelineResponseSerialization() throws IOException {
        DeletePipelineResponse response = new DeletePipelineResponse(randomBoolean());
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        DeletePipelineResponse serializedResponse = new DeletePipelineResponse(out.bytes().streamInput());
        assertEquals(response.isDeleted(), serializedResponse.isDeleted());
    }
}
