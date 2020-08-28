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

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

public class PutStoredScriptRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        PutStoredScriptRequest storedScriptRequest = new PutStoredScriptRequest("bar", "context", new BytesArray("{}"), XContentType.JSON,
                new StoredScriptSource("foo", "bar", Collections.emptyMap()));

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
        builder.startObject("script")
            .field("lang", "painless")
            .field("source", "Math.log(_score * 2) + params.multiplier")
            .endObject();
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
