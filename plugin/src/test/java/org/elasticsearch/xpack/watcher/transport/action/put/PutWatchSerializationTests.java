/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.put;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchRequest;

import java.io.IOException;
import java.util.Base64;

import static org.hamcrest.Matchers.is;

public class PutWatchSerializationTests extends ESTestCase {

    // https://github.com/elastic/x-plugins/issues/2490
    public void testPutWatchSerialization() throws Exception {
        PutWatchRequest request = new PutWatchRequest();
        request.setId(randomAlphaOfLength(10));
        request.setActive(randomBoolean());
        request.setSource(
                new BytesArray(JsonXContent.contentBuilder().startObject().field("foo", randomAlphaOfLength(20)).endObject().string()),
                XContentType.JSON);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        request.writeTo(streamOutput);

        PutWatchRequest readRequest = new PutWatchRequest();
        readRequest.readFrom(streamOutput.bytes().streamInput());
        assertThat(readRequest.isActive(), is(request.isActive()));
        assertThat(readRequest.getId(), is(request.getId()));
        assertThat(readRequest.getSource(), is(request.getSource()));
        assertThat(readRequest.xContentType(), is(request.xContentType()));
    }

    public void testPutWatchSerializationXContent() throws Exception {
        PutWatchRequest request = new PutWatchRequest();
        request.setId(randomAlphaOfLength(10));
        request.setActive(randomBoolean());
        request.setSource(
                new BytesArray(JsonXContent.contentBuilder().startObject().field("foo", randomAlphaOfLength(20)).endObject().string()),
                XContentType.JSON);
        assertEquals(XContentType.JSON, request.xContentType());

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        request.writeTo(streamOutput);

        PutWatchRequest readRequest = new PutWatchRequest();
        StreamInput input = streamOutput.bytes().streamInput();
        readRequest.readFrom(input);
        assertThat(readRequest.isActive(), is(request.isActive()));
        assertThat(readRequest.getId(), is(request.getId()));
        assertThat(readRequest.getSource(), is(request.getSource()));
        assertThat(readRequest.xContentType(), is(XContentType.JSON));
    }

    public void testPutWatchSerializationXContentBwc() throws IOException {
        final byte[] data = Base64.getDecoder().decode("ADwDAmlkDXsiZm9vIjoiYmFyIn0BAAAA");
        final Version version = randomFrom(Version.V_5_0_0, Version.V_5_0_1, Version.V_5_0_2,
                Version.V_5_0_3_UNRELEASED, Version.V_5_1_1_UNRELEASED, Version.V_5_1_2_UNRELEASED, Version.V_5_2_0_UNRELEASED);
        try (StreamInput in = StreamInput.wrap(data)) {
            in.setVersion(version);
            PutWatchRequest request = new PutWatchRequest();
            request.readFrom(in);
            assertEquals(XContentType.JSON, request.xContentType());
            assertEquals("id", request.getId());
            assertTrue(request.isActive());
            assertEquals("{\"foo\":\"bar\"}", request.getSource().utf8ToString());

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                request.writeTo(out);
                assertArrayEquals(data, out.bytes().toBytesRef().bytes);
            }
        }
    }
}
