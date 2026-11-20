/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Parameter parsing of {@code GET /_query/stream/{id}}.
 */
public class RestEsqlGetStreamResultActionTests extends ESTestCase {

    private static RestRequest request(Map<String, String> params) {
        Map<String, String> all = new HashMap<>(params);
        all.put("id", "the-id");
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath("/_query/stream/the-id")
            .withParams(all)
            .build();
    }

    public void testRoute() {
        List<RestHandler.Route> routes = new RestEsqlGetStreamResultAction().routes();
        assertThat(routes, hasSize(1));
        assertThat(routes.get(0).getMethod(), equalTo(RestRequest.Method.GET));
        assertThat(routes.get(0).getPath(), equalTo("/_query/stream/{id}"));
    }

    public void testDefaults() {
        GetStreamResultRequest get = RestEsqlGetStreamResultAction.parseRequest(request(Map.of()));
        assertThat(get.streamId(), equalTo("the-id"));
        assertThat(get.offset(), equalTo(0L));
        assertThat("no max_rows means no limit", get.hasRowLimit(), equalTo(false));
        assertThat("no max_bytes means no limit", get.hasByteLimit(), equalTo(false));
        assertThat("no wait_timeout means no wait", get.waitTimeout().millis() <= 0, equalTo(true));
    }

    public void testValues() {
        GetStreamResultRequest get = RestEsqlGetStreamResultAction.parseRequest(
            request(Map.of("offset", "1000", "max_rows", "50", "max_bytes", "1mb", "wait_timeout", "2s"))
        );
        assertThat(get.offset(), equalTo(1000L));
        assertThat(get.maxRows(), equalTo(50L));
        assertThat(get.maxBytes(), equalTo(1024L * 1024L));
        assertThat(get.waitTimeout(), equalTo(TimeValue.timeValueSeconds(2)));
    }

    public void testZeroLimitsMeanStatusOnly() {
        assertThat(RestEsqlGetStreamResultAction.parseRequest(request(Map.of("max_rows", "0"))).statusOnly(), equalTo(true));
        assertThat(RestEsqlGetStreamResultAction.parseRequest(request(Map.of("max_bytes", "0"))).statusOnly(), equalTo(true));
    }

    public void testInvalidValuesAreBadRequests() {
        for (Map<String, String> params : List.of(
            Map.of("offset", "-1"),
            Map.of("offset", "abc"),
            Map.of("max_rows", "-5"),
            Map.of("max_rows", "x"),
            Map.of("max_bytes", "-1"),
            Map.of("max_bytes", "lots"),
            Map.of("wait_timeout", "soon")
        )) {
            try {
                RestEsqlGetStreamResultAction.parseRequest(request(params));
                fail("expected a bad request for " + params);
            } catch (RuntimeException expected) {
                assertThat("a bad request for " + params, ExceptionsHelper.status(expected), equalTo(RestStatus.BAD_REQUEST));
            }
        }
    }

    public void testKeepAliveIsRejected() {
        RestRequest request = request(Map.of("keep_alive", "1m"));
        // the handler never reads keep_alive, so the request is refused before anything runs and the client is not used
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RestEsqlGetStreamResultAction().handleRequest(request, new FakeRestChannel(request, true), null)
        );
        assertThat(e.getMessage(), containsString("keep_alive"));
    }
}
