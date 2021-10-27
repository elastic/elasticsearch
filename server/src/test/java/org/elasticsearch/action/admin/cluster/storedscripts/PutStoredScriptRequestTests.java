/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;

public class PutStoredScriptRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        PutStoredScriptRequest storedScriptRequest = new PutStoredScriptRequest(
            "bar",
            "context",
            new BytesArray("{}"),
            XContentType.JSON,
            new StoredScriptSource("foo", "bar", Collections.emptyMap())
        );

        assertEquals(XContentType.JSON, storedScriptRequest.xContentType());
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            storedScriptRequest.writeTo(output);

            try (StreamInput in = output.bytes().streamInput()) {
                PutStoredScriptRequest serialized = new PutStoredScriptRequest(in);
                assertEquals(XContentType.JSON, serialized.xContentType());
                assertEquals(storedScriptRequest.id(), serialized.id());
                assertEquals(storedScriptRequest.context(), serialized.context());
            }
        }
    }

    public void testToXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        builder.startObject();
        builder.startObject("script").field("lang", "painless").field("source", "Math.log(_score * 2) + params.multiplier").endObject();
        builder.endObject();

        BytesReference expectedRequestBody = BytesReference.bytes(builder);

        PutStoredScriptRequest request = new PutStoredScriptRequest();
        request.id("test1");
        request.content(expectedRequestBody, xContentType);

        XContentBuilder requestBuilder = XContentBuilder.builder(xContentType.xContent());
        requestBuilder.startObject();
        request.toXContent(requestBuilder, ToXContent.EMPTY_PARAMS);
        requestBuilder.endObject();

        BytesReference actualRequestBody = BytesReference.bytes(requestBuilder);

        assertEquals(expectedRequestBody, actualRequestBody);
    }
}
