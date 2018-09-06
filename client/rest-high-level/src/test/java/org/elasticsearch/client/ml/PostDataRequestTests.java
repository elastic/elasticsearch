/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
