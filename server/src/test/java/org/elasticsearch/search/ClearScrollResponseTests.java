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

package org.elasticsearch.search;

import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class ClearScrollResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        ClearScrollResponse clearScrollResponse = new ClearScrollResponse(true, 10);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            clearScrollResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        assertEquals(true, clearScrollResponse.isSucceeded());
        assertEquals(10, clearScrollResponse.getNumFreed());
    }

    public void testToAndFromXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        ClearScrollResponse originalResponse = createTestItem();
        BytesReference originalBytes = toShuffledXContent(originalResponse, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        ClearScrollResponse parsedResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedResponse = ClearScrollResponse.fromXContent(parser);
        }
        assertEquals(originalResponse.isSucceeded(), parsedResponse.isSucceeded());
        assertEquals(originalResponse.getNumFreed(), parsedResponse.getNumFreed());
        BytesReference parsedBytes = XContentHelper.toXContent(parsedResponse, xContentType, randomBoolean());
        assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);
    }

    private static ClearScrollResponse createTestItem() {
        return new ClearScrollResponse(randomBoolean(), randomIntBetween(0, Integer.MAX_VALUE));
    }
}
