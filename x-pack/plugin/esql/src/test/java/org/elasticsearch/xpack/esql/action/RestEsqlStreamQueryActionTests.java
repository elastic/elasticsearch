/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class RestEsqlStreamQueryActionTests extends ESTestCase {

    private final RestEsqlStreamQueryAction handler = new RestEsqlStreamQueryAction(new EsqlCapabilities.Builder(false).build());

    public void testRoute() {
        assertThat(handler.getName(), equalTo("esql_stream_query"));
        assertThat(handler.routes().size(), equalTo(1));
        assertThat(handler.routes().get(0).getMethod(), equalTo(RestRequest.Method.POST));
        assertThat(handler.routes().get(0).getPath(), equalTo("/_query/stream"));
    }

    public void testResponseParams() {
        assertThat(handler.responseParams(), hasItems("format", "header", "drop_null_columns"));
        assertThat("batch_size is not part of the stream API", handler.responseParams(), not(hasItems("batch_size")));
        assertThat("streaming is not a parameter of the stream API", handler.responseParams(), not(hasItems("streaming")));
        assertThat("delimiter does not apply to NDJSON", handler.responseParams(), not(hasItems("delimiter")));
    }

    public void testColumnarRejected() {
        EsqlQueryRequest esqlRequest = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        esqlRequest.columnar(true);
        expectThrows(
            IllegalArgumentException.class,
            containsString("columnar"),
            () -> RestEsqlStreamQueryAction.streamingChannelConsumer(esqlRequest, requestWithParams(Map.of()), null)
        );
    }

    public void testFormatMustBeNdjson() {
        EsqlQueryRequest esqlRequest = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        expectThrows(
            IllegalArgumentException.class,
            allOf(containsString("ndjson"), containsString("csv")),
            () -> RestEsqlStreamQueryAction.streamingChannelConsumer(esqlRequest, requestWithParams(Map.of("format", "csv")), null)
        );
    }

    /**
     * Drives the real {@link org.elasticsearch.rest.BaseRestHandler#handleRequest}, which rejects any parameter that is still
     * unconsumed once {@code prepareRequest} returns, before it runs the channel consumer (so no client is needed). The message says
     * {@code parameter} when exactly one is unrecognized and {@code parameters} when there are several, so the singular wording
     * shows that {@code format} and {@code header} were consumed.
     */
    public void testUnrecognizedParametersAreRejected() {
        for (String unrecognized : new String[] { "batch_size", "streaming", "delimiter" }) {
            RestRequest request = requestWithQuery(Map.of("format", "ndjson", "header", "present", unrecognized, "x"));
            expectThrows(
                IllegalArgumentException.class,
                containsString("contains unrecognized parameter: [" + unrecognized + "]"),
                () -> handler.handleRequest(request, new FakeRestChannel(request, true), null)
            );
        }
    }

    public void testAllowPartialResultsParameterIsApplied() {
        boolean allowPartial = randomBoolean();
        EsqlQueryRequest esqlRequest = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        Map<String, String> params = Map.of("allow_partial_results", "" + allowPartial);
        RestEsqlStreamQueryAction.streamingChannelConsumer(esqlRequest, requestWithParams(params), null);
        assertThat(esqlRequest.allowPartialResults(), equalTo(allowPartial));
    }

    public void testFormatAndAcceptDefaultToNdjson() {
        RestEsqlStreamQueryAction.validateFormat(null, null);
        RestEsqlStreamQueryAction.validateFormat("ndjson", null);
        RestEsqlStreamQueryAction.validateFormat("NDJSON", null);
        RestEsqlStreamQueryAction.validateFormat(null, "");
        RestEsqlStreamQueryAction.validateFormat(null, "  ");
    }

    public void testAcceptHeaderThatAllowsNdjson() {
        RestEsqlStreamQueryAction.validateFormat(null, "application/x-ndjson");
        RestEsqlStreamQueryAction.validateFormat(null, "*/*");
        RestEsqlStreamQueryAction.validateFormat(null, "application/*");
        RestEsqlStreamQueryAction.validateFormat(null, "application/x-ndjson; charset=utf-8");
        RestEsqlStreamQueryAction.validateFormat(null, "text/csv, application/x-ndjson;q=0.9");
        RestEsqlStreamQueryAction.validateFormat(null, "application/vnd.elasticsearch+x-ndjson;compatible-with=9");
        RestEsqlStreamQueryAction.validateFormat(null, "application/vnd.elasticsearch+x-ndjson; compatible-with=8");
        RestEsqlStreamQueryAction.validateFormat(null, "Application/X-NDJSON");
        RestEsqlStreamQueryAction.validateFormat(null, "APPLICATION/*");
        RestEsqlStreamQueryAction.validateFormat(null, "text/csv, Application/X-Ndjson;q=0.9");
        RestEsqlStreamQueryAction.validateFormat(null, "Application/Vnd.Elasticsearch+X-NDJSON;compatible-with=9");
        RestEsqlStreamQueryAction.validateFormat(null, "*/*;q=0, application/x-ndjson");
        RestEsqlStreamQueryAction.validateFormat(null, "application/*;q=0, application/x-ndjson;q=0.5");
        RestEsqlStreamQueryAction.validateFormat(null, "application/x-ndjson;q=0.001");
        RestEsqlStreamQueryAction.validateFormat(null, "application/x-ndjson; Q=1");
        RestEsqlStreamQueryAction.validateFormat(
            null,
            "application/x-ndjson;q=0, application/vnd.elasticsearch+x-ndjson;compatible-with=9"
        );
    }

    public void testAcceptHeaderThatDoesNotAllowNdjson() {
        for (String accept : new String[] {
            "text/csv",
            "Text/CSV",
            "application/json",
            "application/vnd.apache.arrow.stream",
            "text/*",
            "application/vnd.elasticsearch+json;compatible-with=9",
            "application/x-ndjson;q=0",
            "application/x-ndjson; q=0.0",
            "application/json, */*;q=0",
            "application/x-ndjson;q=0, */*",
            "application/*;q=0",
            "application/vnd.elasticsearch+x-ndjson;compatible-with=9;q=0" }) {
            expectThrows(
                IllegalArgumentException.class,
                allOf(containsString("application/x-ndjson"), containsString(accept)),
                () -> RestEsqlStreamQueryAction.validateFormat(null, accept)
            );
        }
    }

    public void testAcceptHeaderWithMalformedQuality() {
        for (String accept : new String[] { "application/x-ndjson;q=abc", "application/x-ndjson;q=2" }) {
            expectThrows(IllegalArgumentException.class, () -> RestEsqlStreamQueryAction.validateFormat(null, accept));
        }
    }

    private static RequestXContent.ParsedStreamRequest parseStream(String json) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return RequestXContent.parseStream(parser);
        }
    }

    public void testNoLimitsByDefault() throws IOException {
        assertThat(parseStream("{\"query\": \"FROM idx\"}").options(), sameInstance(StreamOptions.NONE));
        assertThat(parseStream("{\"query\": \"FROM idx\", \"store\": \"none\"}").options(), sameInstance(StreamOptions.NONE));
    }

    public void testLimitsAreParsed() throws IOException {
        RequestXContent.ParsedStreamRequest parsed = parseStream(
            "{\"query\": \"FROM idx\", \"max_rows\": 10, \"max_bytes\": \"1kb\", \"store\": \"none\"}"
        );
        assertThat(parsed.request().query(), equalTo("FROM idx"));
        assertThat(parsed.options(), equalTo(new StreamOptions(StreamOptions.Store.NONE, 10, 1024)));
    }

    public void testStoreIsKeptInTheOptions() throws IOException {
        assertThat(
            parseStream("{\"query\": \"FROM idx\", \"store\": \"none\", \"max_rows\": 10}").options().store(),
            equalTo(StreamOptions.Store.NONE)
        );
        assertThat(parseStream("{\"query\": \"FROM idx\", \"max_rows\": 10}").options().store(), equalTo(StreamOptions.Store.NONE));
    }

    public void testEachLimitCanBeSetOnItsOwn() throws IOException {
        assertThat(
            parseStream("{\"query\": \"FROM idx\", \"max_rows\": 0}").options(),
            equalTo(new StreamOptions(StreamOptions.Store.NONE, 0, StreamOptions.NO_LIMIT))
        );
        assertThat(
            parseStream("{\"query\": \"FROM idx\", \"max_bytes\": \"1mb\"}").options(),
            equalTo(new StreamOptions(StreamOptions.Store.NONE, StreamOptions.NO_LIMIT, 1024 * 1024))
        );
    }

    public void testMaxBytesAsANumberIsABytesCount() throws IOException {
        assertThat(
            parseStream("{\"query\": \"FROM idx\", \"max_bytes\": 2048}").options(),
            equalTo(new StreamOptions(StreamOptions.Store.NONE, StreamOptions.NO_LIMIT, 2048))
        );
    }

    public void testValidStoreValuesAreAccepted() throws IOException {
        for (StreamOptions.Store store : StreamOptions.Store.values()) {
            assertThat(parseStream("{\"query\": \"FROM idx\", \"store\": \"" + store + "\"}").options().store(), equalTo(store));
        }
    }

    public void testUnknownStoreIsRejected() {
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> parseStream("{\"query\": \"FROM idx\", \"store\": \"bogus\"}")
        );
        assertThat(e.getCause().getMessage(), equalTo("[store] must be one of [none, overflow, all], got [bogus]"));
    }

    public void testInvalidLimitsAreRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parseStream("{\"query\": \"FROM idx\", \"max_rows\": -1}")
        );
        assertThat(e.getMessage(), equalTo("[max_rows] must be greater than or equal to 0, got [-1]"));

        for (int maxBytes : new int[] { 0, -1 }) {
            e = expectThrows(
                IllegalArgumentException.class,
                () -> parseStream("{\"query\": \"FROM idx\", \"max_bytes\": " + maxBytes + "}")
            );
            assertThat(e.getMessage(), equalTo("[max_bytes] must be greater than 0, got [" + maxBytes + "]"));
        }
    }

    public void testFractionalLimitsAreRejected() {
        for (String field : new String[] { "max_rows", "max_bytes" }) {
            double fraction = field.equals("max_rows") ? 2.9 : 1.5;
            XContentParseException e = expectThrows(
                XContentParseException.class,
                () -> parseStream("{\"query\": \"FROM idx\", \"" + field + "\": " + fraction + "}")
            );
            assertThat(e.getMessage(), containsString("failed to parse field [" + field + "]"));
            assertThat(e.getCause().getMessage(), containsString("cannot be converted to Long without data loss"));
        }
    }

    public void testWholeNumberWrittenAsDecimalIsAccepted() throws IOException {
        assertThat(
            parseStream("{\"query\": \"FROM idx\", \"max_rows\": 2.0}").options(),
            equalTo(new StreamOptions(StreamOptions.Store.NONE, 2, StreamOptions.NO_LIMIT))
        );
    }

    public void testMaxRowsAsAStringIsRejected() {
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> parseStream("{\"query\": \"FROM idx\", \"max_rows\": \"10\"}")
        );
        assertThat(e.getMessage(), containsString("failed to parse field [max_rows]"));
        assertThat(e.getCause().getMessage(), containsString("Long value passed as String"));
    }

    public void testMaxBytesStringWithoutUnitIsRejected() {
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> parseStream("{\"query\": \"FROM idx\", \"max_bytes\": \"1024\"}")
        );
        assertThat(e.getMessage(), containsString("failed to parse field [max_bytes]"));
        assertThat(e.getCause().getMessage(), containsString("unit is missing or unrecognized"));
    }

    public void testRowLimitIsReachedAtTheLimit() {
        StreamOptions options = new StreamOptions(StreamOptions.Store.NONE, 3, StreamOptions.NO_LIMIT);
        assertFalse(options.reached(2, 1_000_000));
        assertTrue(options.reached(3, 0));
        assertTrue(options.reached(4, 0));
        assertTrue(
            "max_rows 0 is reached before any row",
            new StreamOptions(StreamOptions.Store.NONE, 0, StreamOptions.NO_LIMIT).reached(0, 0)
        );
    }

    public void testByteLimitIsReachedAtTheLimitButNeverBeforeTheFirstRow() {
        StreamOptions options = new StreamOptions(StreamOptions.Store.NONE, StreamOptions.NO_LIMIT, 100);
        assertFalse("the first row is always written, even when the columns line already fills the limit", options.reached(0, 1_000));
        assertFalse(options.reached(1, 99));
        assertTrue(options.reached(1, 100));
        assertTrue(options.reached(5, 101));
    }

    public void testNoLimitsAreNeverReached() {
        assertFalse(StreamOptions.NONE.reached(0, 0));
        assertFalse(StreamOptions.NONE.reached(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    private static RestRequest requestWithParams(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(new HashMap<>(params)).build();
    }

    private static RestRequest requestWithQuery(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(new HashMap<>(params))
            .withContent(new BytesArray("{\"query\": \"FROM idx\"}"), XContentType.JSON)
            .build();
    }
}
