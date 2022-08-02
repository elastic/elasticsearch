/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.startsWith;

public class ClearScrollRequestTests extends ESTestCase {

    public void testFromXContent() throws Exception {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        if (randomBoolean()) {
            // test that existing values get overridden
            clearScrollRequest = createClearScrollRequest();
        }
        try (
            XContentParser parser = createParser(
                XContentFactory.jsonBuilder().startObject().array("scroll_id", "value_1", "value_2").endObject()
            )
        ) {
            clearScrollRequest.fromXContent(parser);
        }
        assertThat(clearScrollRequest.scrollIds(), contains("value_1", "value_2"));
    }

    public void testFromXContentWithoutArray() throws Exception {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        if (randomBoolean()) {
            // test that existing values get overridden
            clearScrollRequest = createClearScrollRequest();
        }
        try (XContentParser parser = createParser(XContentFactory.jsonBuilder().startObject().field("scroll_id", "value_1").endObject())) {
            clearScrollRequest.fromXContent(parser);
        }
        assertThat(clearScrollRequest.scrollIds(), contains("value_1"));
    }

    public void testFromXContentWithUnknownParamThrowsException() throws Exception {
        XContentParser invalidContent = createParser(
            XContentFactory.jsonBuilder().startObject().array("scroll_id", "value_1", "value_2").field("unknown", "keyword").endObject()
        );
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();

        Exception e = expectThrows(IllegalArgumentException.class, () -> clearScrollRequest.fromXContent(invalidContent));
        assertThat(e.getMessage(), startsWith("Unknown parameter [unknown]"));
    }

    public void testToXContent() throws IOException {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId("SCROLL_ID");
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            clearScrollRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("{\"scroll_id\":[\"SCROLL_ID\"]}", Strings.toString(builder));
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
