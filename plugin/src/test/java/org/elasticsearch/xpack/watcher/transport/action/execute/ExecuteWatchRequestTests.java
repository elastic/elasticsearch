/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.execute;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ExecuteWatchRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        ExecuteWatchRequest request = new ExecuteWatchRequest("1");
        request.setWatchSource(new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
        assertEquals(XContentType.JSON, request.getXContentType());

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        ExecuteWatchRequest serialized = new ExecuteWatchRequest();
        serialized.readFrom(in);
        assertEquals(XContentType.JSON, serialized.getXContentType());
        assertEquals("{}", serialized.getWatchSource().utf8ToString());
    }

    public void testSerializationBwc() throws IOException {
        final byte[] data = Base64.getDecoder().decode("ADwDAAAAAAAAAAAAAAAAAAABDnsid2F0Y2giOiJtZSJ9AAAAAAAAAA==");
        final Version version = randomFrom(Version.V_5_0_0, Version.V_5_0_1, Version.V_5_0_2,
                Version.V_5_0_3_UNRELEASED, Version.V_5_1_1_UNRELEASED, Version.V_5_1_2_UNRELEASED, Version.V_5_2_0_UNRELEASED);
        try (StreamInput in = StreamInput.wrap(data)) {
            in.setVersion(version);
            ExecuteWatchRequest request = new ExecuteWatchRequest();
            request.readFrom(in);
            assertEquals(XContentType.JSON, request.getXContentType());
            assertEquals("{\"watch\":\"me\"}", request.getWatchSource().utf8ToString());

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                request.writeTo(out);
                assertArrayEquals(data, out.bytes().toBytesRef().bytes);
            }
        }
    }
}
