/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.reindex.PaginatedHitSource;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Base64;

import static org.elasticsearch.reindex.remote.RemoteResponseParsers.OPEN_PIT_PARSER;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertArrayEquals;

public class RemoteResponseParsersTests extends ESTestCase {

    /**
     * Check that we can parse shard search failures without index information.
     */
    public void testFailureWithoutIndex() throws IOException {
        ShardSearchFailure failure = new ShardSearchFailure(new EsRejectedExecutionException("exhausted"));
        XContentBuilder builder = jsonBuilder();
        failure.toXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser parser = createParser(builder)) {
            PaginatedHitSource.SearchFailure parsed = RemoteResponseParsers.SEARCH_FAILURE_PARSER.parse(parser, null);
            assertNotNull(parsed.getReason());
            assertThat(parsed.getReason().getMessage(), Matchers.containsString("exhausted"));
            assertThat(parsed.getReason(), Matchers.instanceOf(EsRejectedExecutionException.class));
        }
    }

    /**
     * Verifies that OPEN_PIT_PARSER extracts and base64-url-decodes the id field from a valid open PIT response,
     * regardless of field order.
     */
    public void testOpenPitParserValidResponse() throws IOException {
        byte[] pitIdBytes = randomByteArrayOfLength(between(1, 64));
        String base64Id = Base64.getUrlEncoder().encodeToString(pitIdBytes);
        int fieldsBefore = between(0, 3);
        int fieldsAfter = between(0, 3);

        XContentBuilder builder = jsonBuilder().startObject();
        // Randomly generates some fields to come before the ID
        for (int i = 0; i < fieldsBefore; i++) {
            builder.field("before_" + i + "_" + randomAlphaOfLength(between(1, 5)), randomAlphaOfLength(between(1, 10)));
        }
        builder.field("id", base64Id);
        // Randomly generates some fields to come after the ID
        for (int i = 0; i < fieldsAfter; i++) {
            builder.field("after_" + i + "_" + randomAlphaOfLength(between(1, 5)), randomAlphaOfLength(between(1, 10)));
        }
        builder.endObject();
        try (XContentParser parser = createParser(builder)) {
            BytesReference result = OPEN_PIT_PARSER.apply(parser, XContentType.JSON);
            assertArrayEquals(pitIdBytes, BytesReference.toBytes(result));
        }
    }

    /**
     * Verifies that OPEN_PIT_PARSER throws when the response is an empty object with no id field.
     */
    public void testOpenPitParserMissingId() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        try (XContentParser parser = createParser(builder)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> OPEN_PIT_PARSER.apply(parser, XContentType.JSON)
            );
            assertThat(e.getMessage(), Matchers.containsString("open point-in-time response must contain [id] field"));
        }
    }

    /**
     * Verifies that OPEN_PIT_PARSER throws when the id field is present but empty.
     */
    public void testOpenPitParserEmptyId() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().field("id", "").endObject();
        try (XContentParser parser = createParser(builder)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> OPEN_PIT_PARSER.apply(parser, XContentType.JSON)
            );
            assertThat(e.getMessage(), Matchers.containsString("open point-in-time response must contain [id] field"));
        }
    }

    /**
     * Verifies that OPEN_PIT_PARSER throws when the response is not a JSON object (e.g. an array).
     */
    public void testOpenPitParserNotAnObject() throws IOException {
        XContentBuilder builder = jsonBuilder().startArray().value("a").endArray();
        try (XContentParser parser = createParser(builder)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> OPEN_PIT_PARSER.apply(parser, XContentType.JSON)
            );
            assertThat(e.getMessage(), Matchers.containsString("open point-in-time response must be an object"));
        }
    }

    /**
     * Verifies that OPEN_PIT_PARSER throws when the id field contains invalid base64-url data.
     */
    public void testOpenPitParserInvalidBase64() throws IOException {
        String invalidBase64 = randomAlphaOfLength(between(1, 20)) + "!!!";
        XContentBuilder builder = jsonBuilder().startObject().field("id", invalidBase64).endObject();
        try (XContentParser parser = createParser(builder)) {
            expectThrows(IllegalArgumentException.class, () -> OPEN_PIT_PARSER.apply(parser, XContentType.JSON));
        }
    }
}
