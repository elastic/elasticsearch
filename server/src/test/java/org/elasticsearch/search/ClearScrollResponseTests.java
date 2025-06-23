/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ClearScrollResponseTests extends ESTestCase {

    private static final ConstructingObjectParser<ClosePointInTimeResponse, Void> PARSER = new ConstructingObjectParser<>(
        "clear_scroll",
        true,
        a -> new ClosePointInTimeResponse((boolean) a[0], (int) a[1])
    );
    static {
        PARSER.declareField(
            constructorArg(),
            (parser, context) -> parser.booleanValue(),
            ClearScrollResponse.SUCCEEDED,
            ObjectParser.ValueType.BOOLEAN
        );
        PARSER.declareField(
            constructorArg(),
            (parser, context) -> parser.intValue(),
            ClearScrollResponse.NUMFREED,
            ObjectParser.ValueType.INT
        );
    }

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
            parsedResponse = PARSER.parse(parser, null);
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
