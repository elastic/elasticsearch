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
import org.elasticsearch.xcontent.XContentType;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

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
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestEsqlStreamQueryAction.streamingChannelConsumer(esqlRequest, requestWithParams(Map.of()), null)
        );
        assertThat(e.getMessage(), containsString("columnar"));
    }

    public void testFormatMustBeNdjson() {
        EsqlQueryRequest esqlRequest = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestEsqlStreamQueryAction.streamingChannelConsumer(esqlRequest, requestWithParams(Map.of("format", "csv")), null)
        );
        assertThat(e.getMessage(), containsString("ndjson"));
        assertThat(e.getMessage(), containsString("csv"));
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
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> handler.handleRequest(request, new FakeRestChannel(request, true), null)
            );
            assertThat(e.getMessage(), containsString("contains unrecognized parameter: [" + unrecognized + "]"));
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
    }

    public void testAcceptHeaderThatDoesNotAllowNdjson() {
        for (String accept : new String[] {
            "text/csv",
            "Text/CSV",
            "application/json",
            "application/vnd.apache.arrow.stream",
            "text/*",
            "application/vnd.elasticsearch+json;compatible-with=9" }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestEsqlStreamQueryAction.validateFormat(null, accept)
            );
            assertThat(e.getMessage(), containsString("application/x-ndjson"));
            assertThat(e.getMessage(), containsString(accept));
        }
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
