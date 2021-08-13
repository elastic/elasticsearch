/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.equalTo;

public class SimulatePipelineRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        SimulatePipelineRequest request = new SimulatePipelineRequest(new BytesArray(""), XContentType.JSON);
        // Sometimes we set an id
        if (randomBoolean()) {
            request.setId(randomAlphaOfLengthBetween(1, 10));
        }

        // Sometimes we explicitly set a boolean (with whatever value)
        if (randomBoolean()) {
            request.setVerbose(randomBoolean());
        }

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        SimulatePipelineRequest otherRequest = new SimulatePipelineRequest(streamInput);

        assertThat(otherRequest.getId(), equalTo(request.getId()));
        assertThat(otherRequest.isVerbose(), equalTo(request.isVerbose()));
    }

    public void testSerializationWithXContent() throws IOException {
        SimulatePipelineRequest request =
            new SimulatePipelineRequest(new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
        assertEquals(XContentType.JSON, request.getXContentType());

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);

        SimulatePipelineRequest serialized = new SimulatePipelineRequest(in);
        assertEquals(XContentType.JSON, serialized.getXContentType());
        assertEquals("{}", serialized.getSource().utf8ToString());
    }
}
