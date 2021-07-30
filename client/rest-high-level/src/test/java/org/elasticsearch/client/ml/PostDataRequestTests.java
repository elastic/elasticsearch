/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class PostDataRequestTests extends AbstractXContentTestCase<PostDataRequest> {

    @Override
    protected PostDataRequest createTestInstance() {
        String jobId = randomAlphaOfLength(10);
        XContentType contentType = randomFrom(XContentType.JSON, XContentType.SMILE);

        PostDataRequest request = new PostDataRequest(jobId, contentType, new byte[0]);
        if (randomBoolean()) {
           request.setResetEnd(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            request.setResetStart(randomAlphaOfLength(10));
        }

        return request;
    }

    @Override
    protected PostDataRequest doParseInstance(XContentParser parser) {
        return PostDataRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testJsonBuilder() throws IOException {

        String jobId = randomAlphaOfLength(10);
        PostDataRequest.JsonBuilder builder = new PostDataRequest.JsonBuilder();

        Map<String, Object> obj1 = new HashMap<>();
        obj1.put("entry1", "value1");
        obj1.put("entry2", "value2");
        builder.addDoc(obj1);

        builder.addDoc("{\"entry3\":\"value3\"}");
        builder.addDoc("{\"entry4\":\"value4\"}".getBytes(StandardCharsets.UTF_8));

        PostDataRequest request = new PostDataRequest(jobId, builder);

        assertEquals("{\"entry1\":\"value1\",\"entry2\":\"value2\"}{\"entry3\":\"value3\"}{\"entry4\":\"value4\"}",
            request.getContent().utf8ToString());
        assertEquals(XContentType.JSON, request.getXContentType());
        assertEquals(jobId, request.getJobId());
    }

    public void testFromByteArray() {
        String jobId = randomAlphaOfLength(10);
        PostDataRequest request = new PostDataRequest(jobId,
            XContentType.JSON,
            "{\"others\":{\"foo\":100}}".getBytes(StandardCharsets.UTF_8));

        assertEquals("{\"others\":{\"foo\":100}}", request.getContent().utf8ToString());
        assertEquals(XContentType.JSON, request.getXContentType());
        assertEquals(jobId, request.getJobId());
    }
}
