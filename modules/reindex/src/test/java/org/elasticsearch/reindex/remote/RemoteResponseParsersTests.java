/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.reindex.PaginatedHitSource;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.elasticsearch.reindex.remote.RemoteResponseParsers.HIT_PARSER;
import static org.elasticsearch.reindex.remote.RemoteResponseParsers.OPEN_PIT_PARSER;
import static org.elasticsearch.reindex.remote.RemoteResponseParsers.RESPONSE_PARSER;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class RemoteResponseParsersTests extends ESTestCase {

    /**
     * Check that we can parse shard search failures without index information.
     */
    public void testFailureWithoutIndex() throws IOException {
        ShardSearchFailure failure = new ShardSearchFailure(new EsRejectedExecutionException("exhausted"));
        XContentBuilder builder = jsonBuilder();
        failure.toXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser parser = createParser(builder)) {
            PaginatedSearchFailure parsed = RemoteResponseParsers.SEARCH_FAILURE_PARSER.parse(parser, null);
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
            Exception e = expectThrows(Exception.class, () -> OPEN_PIT_PARSER.apply(parser, XContentType.JSON));
            assertThat(
                ExceptionsHelper.unwrapCause(e).getMessage(),
                Matchers.containsString("Failed to build [open_pit_response] after last required field arrived")
            );
        }
    }

    /**
     * Verifies that OPEN_PIT_PARSER throws when the id field is present but empty.
     */
    public void testOpenPitParserEmptyId() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().field("id", "").endObject();
        try (XContentParser parser = createParser(builder)) {
            Exception e = expectThrows(Exception.class, () -> OPEN_PIT_PARSER.apply(parser, XContentType.JSON));
            assertThat(ExceptionsHelper.unwrapCause(e).getMessage(), Matchers.containsString("failed to parse field [id]"));
        }
    }

    /**
     * Verifies that OPEN_PIT_PARSER throws when the response is not a JSON object (e.g. an array).
     */
    public void testOpenPitParserNotAnObject() throws IOException {
        XContentBuilder builder = jsonBuilder().startArray().value("a").endArray();
        try (XContentParser parser = createParser(builder)) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> OPEN_PIT_PARSER.apply(parser, XContentType.JSON));
            assertThat(e.getMessage(), Matchers.containsString("Expected START_OBJECT"));
        }
    }

    /**
     * Verifies that OPEN_PIT_PARSER throws when the response is a primitive value rather than an object.
     */
    public void testOpenPitParserNotAnObjectWithPrimitive() throws IOException {
        XContentBuilder builder = randomFrom(jsonBuilder().value("bare_string"), jsonBuilder().value(42), jsonBuilder().value(true));
        try (XContentParser parser = createParser(builder)) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> OPEN_PIT_PARSER.apply(parser, XContentType.JSON));
            assertThat(e.getMessage(), Matchers.containsString("Expected START_OBJECT"));
        }
    }

    /**
     * Verifies that OPEN_PIT_PARSER throws when the id field has the wrong type (e.g. number instead of string).
     */
    public void testOpenPitParserIdWrongType() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().field("id", randomInt()).endObject();
        try (XContentParser parser = createParser(builder)) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> OPEN_PIT_PARSER.apply(parser, XContentType.JSON));
            assertThat(e.getMessage(), Matchers.anyOf(Matchers.containsString("id doesn't support values of type")));
        }
    }

    /**
     * Verifies that OPEN_PIT_PARSER throws when the id field is explicitly null.
     */
    public void testOpenPitParserNullId() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().nullField("id").endObject();
        try (XContentParser parser = createParser(builder)) {
            Exception e = expectThrows(Exception.class, () -> OPEN_PIT_PARSER.apply(parser, XContentType.JSON));
            assertThat(
                ExceptionsHelper.unwrapCause(e).getMessage(),
                Matchers.containsString("id doesn't support values of type: VALUE_NULL")
            );
        }
    }

    /**
     * Verifies that OPEN_PIT_PARSER throws when the id field contains invalid base64-url data.
     */
    public void testOpenPitParserInvalidBase64() throws IOException {
        String invalidBase64 = randomAlphaOfLength(between(1, 20)) + "!!!";
        XContentBuilder builder = jsonBuilder().startObject().field("id", invalidBase64).endObject();
        try (XContentParser parser = createParser(builder)) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> OPEN_PIT_PARSER.apply(parser, XContentType.JSON));
            assertThat(e.getMessage(), Matchers.containsString("failed to parse field [id]"));
        }
    }

    /**
     * Verifies that HIT_PARSER parses sort values including integers.
     */
    public void testHitParserSortValuesInteger() throws IOException {
        int sortValue = randomInt();
        String sortKey = randomAlphaOfLength(between(1, 10));
        XContentBuilder builder = jsonBuilder().startObject()
            .field("_index", "test")
            .field("_id", "doc1")
            .startObject("_source")
            .endObject()
            .startArray("sort")
            .value(sortValue)
            .value(sortKey)
            .endArray()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            PaginatedHitSource.BasicHit hit = HIT_PARSER.parse(parser, XContentType.JSON);
            assertNotNull(hit.getSortValues());
            assertEquals(2, hit.getSortValues().length);
            assertEquals(sortValue, hit.getSortValues()[0]);
            assertEquals(sortKey, hit.getSortValues()[1]);
        }
    }

    /**
     * Verifies that HIT_PARSER parses sort values that exceed int range as Long.
     */
    public void testHitParserSortValuesLong() throws IOException {
        long sortValue = 4294967396L; // shard index 1, doc id 100 - exceeds int range
        String sortKey = randomAlphaOfLength(between(1, 10));
        XContentBuilder builder = jsonBuilder().startObject()
            .field("_index", "test")
            .field("_id", "doc1")
            .startObject("_source")
            .endObject()
            .startArray("sort")
            .value(sortValue)
            .value(sortKey)
            .endArray()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            PaginatedHitSource.BasicHit hit = HIT_PARSER.parse(parser, XContentType.JSON);
            assertNotNull(hit.getSortValues());
            assertEquals(2, hit.getSortValues().length);
            assertEquals(sortValue, ((Number) hit.getSortValues()[0]).longValue());
            assertEquals(sortKey, hit.getSortValues()[1]);
        }
    }

    /**
     * Verifies that HIT_PARSER parses sort values with mixed types (string, number, boolean, null).
     */
    public void testHitParserSortValuesMixedTypes() throws IOException {
        String stringVal = randomAlphaOfLength(between(1, 20));
        int intValue = randomInt();
        XContentBuilder builder = jsonBuilder().startObject()
            .field("_index", "test")
            .field("_id", "doc1")
            .startObject("_source")
            .endObject()
            .startArray("sort")
            .value(stringVal)
            .value(intValue)
            .value(true)
            .nullValue()
            .endArray()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            PaginatedHitSource.BasicHit hit = HIT_PARSER.parse(parser, XContentType.JSON);
            assertNotNull(hit.getSortValues());
            assertEquals(4, hit.getSortValues().length);
            assertEquals(stringVal, hit.getSortValues()[0]);
            assertEquals(intValue, hit.getSortValues()[1]);
            assertEquals(true, hit.getSortValues()[2]);
            assertNull(hit.getSortValues()[3]);
        }
    }

    /**
     * Verifies that HIT_PARSER returns null sort values when sort array is empty.
     */
    public void testHitParserSortValuesEmptyArray() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject()
            .field("_index", "test")
            .field("_id", "doc1")
            .startObject("_source")
            .endObject()
            .startArray("sort")
            .endArray()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            PaginatedHitSource.BasicHit hit = HIT_PARSER.parse(parser, XContentType.JSON);
            assertNull(hit.getSortValues());
        }
    }

    /**
     * Verifies that HIT_PARSER parses hit without sort field (sort values remain null).
     */
    public void testHitParserNoSortField() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject()
            .field("_index", "test")
            .field("_id", "doc1")
            .startObject("_source")
            .endObject()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            PaginatedHitSource.BasicHit hit = HIT_PARSER.parse(parser, XContentType.JSON);
            assertNull(hit.getSortValues());
        }
    }

    /**
     * Verifies that HIT_PARSER throws when sort array contains invalid value type (e.g. object).
     */
    public void testHitParserSortValuesInvalidType() throws IOException {
        int sortValue = randomInt();
        XContentBuilder builder = jsonBuilder().startObject()
            .field("_index", "test")
            .field("_id", "doc1")
            .startObject("_source")
            .endObject()
            .startArray("sort")
            .value(sortValue)
            .startObject()
            .field("nested", "invalid")
            .endObject()
            .endArray()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            Exception e = expectThrows(Exception.class, () -> HIT_PARSER.parse(parser, XContentType.JSON));
            assertThat(e.getCause(), Matchers.notNullValue());
            assertThat(e.getCause().getMessage(), Matchers.containsString("Expected value in sort array"));
        }
    }

    /**
     * Verifies that RESPONSE_PARSER extracts and base64-decodes pit_id from a PIT search response.
     */
    public void testResponseParserPitId() throws IOException {
        byte[] pitIdBytes = randomByteArrayOfLength(between(1, 32));
        String pitIdBase64 = Base64.getUrlEncoder().encodeToString(pitIdBytes);
        XContentBuilder builder = jsonBuilder().startObject()
            .field("pit_id", pitIdBase64)
            .field("timed_out", false)
            .startObject("hits")
            .field("total", 0)
            .startArray("hits")
            .endArray()
            .endObject()
            .startObject("_shards")
            .endObject()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            PaginatedHitSource.Response response = RESPONSE_PARSER.apply(parser, XContentType.JSON);
            assertNotNull(response.getPitId());
            assertArrayEquals(pitIdBytes, BytesReference.toBytes(response.getPitId()));
        }
    }

    /**
     * Verifies that RESPONSE_PARSER returns null pitId when pit_id field is missing.
     */
    public void testResponseParserPitIdMissing() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject()
            .field("timed_out", false)
            .startObject("hits")
            .field("total", 0)
            .startArray("hits")
            .endArray()
            .endObject()
            .startObject("_shards")
            .endObject()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            PaginatedHitSource.Response response = RESPONSE_PARSER.apply(parser, XContentType.JSON);
            assertNull(response.getPitId());
        }
    }

    /**
     * Verifies that RESPONSE_PARSER handles null timed_out (parses successfully with timedOut=false).
     */
    public void testResponseParserTimedOutNull() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject()
            .startObject("hits")
            .field("total", 0)
            .startArray("hits")
            .endArray()
            .endObject()
            .startObject("_shards")
            .endObject()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            PaginatedHitSource.Response response = RESPONSE_PARSER.apply(parser, XContentType.JSON);
            assertFalse(response.isTimedOut());
        }
    }

    /**
     * Verifies that RESPONSE_PARSER parses a full PIT search response with hits and sort values.
     */
    public void testResponseParserFullPitResponse() throws IOException {
        String pitId = randomAlphaOfLength(between(1, 20));
        String pitIdBase64 = Base64.getUrlEncoder().encodeToString(pitId.getBytes(StandardCharsets.UTF_8));
        String index = randomAlphaOfLength(between(1, 10));
        String docId = randomAlphaOfLength(between(1, 24));
        String sortKey = randomAlphaOfLength(between(1, 10));
        int sortValue = randomInt();
        int totalHits = between(1, 100);
        XContentBuilder builder = jsonBuilder().startObject()
            .field("pit_id", pitIdBase64)
            .field("timed_out", false)
            .startObject("hits")
            .field("total", totalHits)
            .startArray("hits")
            .startObject()
            .field("_index", index)
            .field("_id", docId)
            .field("_version", 1)
            .startObject("_source")
            .field("test", "test2")
            .endObject()
            .startArray("sort")
            .value(sortValue)
            .value(sortKey)
            .endArray()
            .endObject()
            .endArray()
            .endObject()
            .startObject("_shards")
            .field("total", 5)
            .field("successful", 5)
            .field("failed", 0)
            .endObject()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            PaginatedHitSource.Response response = RESPONSE_PARSER.apply(parser, XContentType.JSON);
            assertFalse(response.isTimedOut());
            assertNotNull(response.getPitId());
            assertArrayEquals(pitId.getBytes(StandardCharsets.UTF_8), BytesReference.toBytes(response.getPitId()));
            assertEquals(totalHits, response.getTotalHits());
            assertThat(response.getHits(), Matchers.hasSize(1));
            PaginatedHitSource.Hit hit = response.getHits().getFirst();
            assertEquals(index, hit.getIndex());
            assertEquals(docId, hit.getId());
            assertNotNull(hit.getSortValues());
            assertEquals(2, hit.getSortValues().length);
            assertEquals(sortValue, hit.getSortValues()[0]);
            assertEquals(sortKey, hit.getSortValues()[1]);
        }
    }
}
