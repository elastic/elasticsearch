/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.action.execute;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ExecuteWatchRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        ExecuteWatchRequest request = new ExecuteWatchRequest("1");
        request.setWatchSource(new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
        assertEquals(XContentType.JSON, request.getXContentType());

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        ExecuteWatchRequest serialized = new ExecuteWatchRequest(in);
        assertEquals(XContentType.JSON, serialized.getXContentType());
        assertEquals("{}", serialized.getWatchSource().utf8ToString());
    }
}
