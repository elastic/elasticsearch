/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.ingest.IngestPipelineTestUtils.putJsonPipelineRequest;

public class PutPipelineRequestTests extends ESTestCase {

    public void testSerializationWithXContent() throws IOException {
        PutPipelineRequest request = putJsonPipelineRequest("1", "{}");
        assertEquals(XContentType.JSON, request.getXContentType());

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);

        PutPipelineRequest serialized = new PutPipelineRequest(in);
        assertEquals(XContentType.JSON, serialized.getXContentType());
        assertEquals("{}", serialized.getSource().utf8ToString());
    }

    public void testToXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder pipelineBuilder = XContentBuilder.builder(xContentType.xContent());
        pipelineBuilder.startObject().field(Pipeline.DESCRIPTION_KEY, "some random set of processors");
        pipelineBuilder.startArray(Pipeline.PROCESSORS_KEY);
        // Start first processor
        pipelineBuilder.startObject();
        pipelineBuilder.startObject("set");
        pipelineBuilder.field("field", "foo");
        pipelineBuilder.field("value", "bar");
        pipelineBuilder.endObject();
        pipelineBuilder.endObject();
        // End first processor
        pipelineBuilder.endArray();
        pipelineBuilder.endObject();
        PutPipelineRequest request = new PutPipelineRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "1",
            BytesReference.bytes(pipelineBuilder),
            xContentType
        );
        XContentBuilder requestBuilder = XContentBuilder.builder(xContentType.xContent());
        BytesReference actualRequestBody = BytesReference.bytes(request.toXContent(requestBuilder, ToXContent.EMPTY_PARAMS));
        assertEquals(BytesReference.bytes(pipelineBuilder), actualRequestBody);
    }
}
