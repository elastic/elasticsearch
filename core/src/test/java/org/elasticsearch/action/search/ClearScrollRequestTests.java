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

package org.elasticsearch.action.search;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.startsWith;

public class ClearScrollRequestTests extends ESTestCase {

    public void testFromXContent() throws Exception {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        if (randomBoolean()) {
            //test that existing values get overridden
            clearScrollRequest = createClearScrollRequest();
        }
        try (XContentParser parser = createParser(XContentFactory.jsonBuilder()
                .startObject()
                .array("scroll_id", "value_1", "value_2")
                .endObject())) {
            clearScrollRequest.fromXContent(parser);
        }
        assertThat(clearScrollRequest.scrollIds(), contains("value_1", "value_2"));
    }

    public void testFromXContentWithoutArray() throws Exception {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        if (randomBoolean()) {
            //test that existing values get overridden
            clearScrollRequest = createClearScrollRequest();
        }
        try (XContentParser parser = createParser(XContentFactory.jsonBuilder()
                .startObject()
                .field("scroll_id", "value_1")
                .endObject())) {
            clearScrollRequest.fromXContent(parser);
        }
        assertThat(clearScrollRequest.scrollIds(), contains("value_1"));
    }

    public void testFromXContentWithUnknownParamThrowsException() throws Exception {
        XContentParser invalidContent = createParser(XContentFactory.jsonBuilder()
                .startObject()
                .array("scroll_id", "value_1", "value_2")
                .field("unknown", "keyword")
                .endObject());
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();

        Exception e = expectThrows(IllegalArgumentException.class, () -> clearScrollRequest.fromXContent(invalidContent));
        assertThat(e.getMessage(), startsWith("Unknown parameter [unknown]"));
    }

    public void testToXContent() throws IOException {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId("SCROLL_ID");
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            clearScrollRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("{\"scroll_id\":[\"SCROLL_ID\"]}", builder.string());
        }
    }

    public void testFromAndToXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        ClearScrollRequest originalRequest = createClearScrollRequest();
        BytesReference originalBytes = toShuffledXContent(originalRequest, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        ClearScrollRequest parsedRequest = new ClearScrollRequest();
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedRequest.fromXContent(parser);
        }
        assertEquals(originalRequest.scrollIds(), parsedRequest.scrollIds());
        BytesReference parsedBytes = XContentHelper.toXContent(parsedRequest, xContentType, randomBoolean());
        assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);
    }

    public static ClearScrollRequest createClearScrollRequest() {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        int numScrolls = randomIntBetween(1, 10);
        for (int i = 0; i < numScrolls; i++) {
            clearScrollRequest.addScrollId(randomAlphaOfLengthBetween(3, 10));
        }
        return clearScrollRequest;
    }
}
