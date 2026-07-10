/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.instrumentation;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.instrumentation.api.instrumenter.SpanStatusBuilder;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.List;
import java.util.Map;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static java.util.Map.entry;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class APMHttpServerInstrumentationTests extends ESTestCase {

    final SpanStatusBuilder spanStatusBuilder = mock(SpanStatusBuilder.class);
    final APMTracer tracer = mock(APMTracer.class);
    final APMHttpServerInstrumentation instrumentation = new APMHttpServerInstrumentation(tracer);

    public void test_start_setsRequestAttributes_minimal() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withScheme("https")
            .withPath("/my-index/_search")
            .withHeaders(Map.of("Accept-Encoding", List.of("gzip")))
            .build();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        instrumentation.start(threadContext, request, "/{index}/_search");

        var inOrder = inOrder(tracer);
        inOrder.verify(tracer)
            .startTrace(
                threadContext,
                request,
                "GET /{index}/_search",
                Map.of(
                    "http.method",
                    "GET",
                    "http.url",
                    "/my-index/_search",
                    "http.flavour",
                    "1.1",
                    "http.request.headers.accept_encoding",
                    "gzip"
                )
            );
        inOrder.verify(tracer)
            .setAttributes(
                request,
                Attributes.builder()
                    .put(stringKey("http.request.method"), "GET")
                    .put(stringKey("url.scheme"), "https")
                    .put(stringKey("http.route"), "/{index}/_search")
                    .put(stringKey("url.path"), "/my-index/_search")
                    .build()
            );
        inOrder.verifyNoMoreInteractions();
    }

    public void test_start_setsRequestAttributes_full() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withScheme("http")
            .withPath("/my-index/_search?from=0")
            .withHeaders(
                Map.ofEntries(
                    entry("Accept-Encoding", List.of("gzip")),
                    entry("Forwarded", List.of("for=1.1.1.1;proto=https")),
                    entry("Host", List.of("elastic.co:443")),
                    entry("User-Agent", List.of("Firefox"))
                )
            )
            .build();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        instrumentation.start(threadContext, request, "/{index}/_search");

        var inOrder = inOrder(tracer);
        inOrder.verify(tracer)
            .startTrace(
                threadContext,
                request,
                "GET /{index}/_search",
                Map.of(
                    "http.method",
                    "GET",
                    "http.url",
                    "/my-index/_search?from=0",
                    "http.flavour",
                    "1.1",
                    "http.request.headers.accept_encoding",
                    "gzip",
                    "http.request.headers.forwarded",
                    "for=1.1.1.1;proto=https",
                    "http.request.headers.host",
                    "elastic.co:443",
                    "http.request.headers.user_agent",
                    "Firefox"
                )
            );
        inOrder.verify(tracer)
            .setAttributes(
                request,
                Attributes.builder()
                    .put(stringKey("http.request.method"), "GET")
                    .put(stringKey("http.route"), "/{index}/_search")
                    .put(stringKey("url.scheme"), "https")
                    .put(stringKey("url.path"), "/my-index/_search")
                    .put(stringKey("url.query"), "from=0")
                    .put(stringKey("server.address"), "elastic.co")
                    .put(longKey("server.port"), 443L)
                    .put(stringKey("client.address"), "1.1.1.1")
                    .put(stringKey("user_agent.original"), "Firefox")
                    .build()
            );
        inOrder.verifyNoMoreInteractions();
    }

    public void test_recordException_delegatesToTracer() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/my-index/_search")
            .build();
        var exception = new RuntimeException("test");

        instrumentation.recordException(request, exception);

        var inOrder = inOrder(tracer);
        inOrder.verify(tracer).addError(request, exception);
        inOrder.verifyNoMoreInteractions();
    }

    public void test_end_setsResponseAttributes_minimal() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/my-index/_search")
            .build();
        RestResponse response = new RestResponse(RestStatus.OK, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);

        instrumentation.end(request, response);

        var inOrder = inOrder(tracer, spanStatusBuilder);
        inOrder.verify(tracer).setAttribute(request, "http.status_code", 200L);
        inOrder.verify(tracer)
            .setAttributes(
                request,
                Attributes.builder()
                    .put(longKey("http.response.status_code"), 200L)
                    .put(stringKey("network.protocol.version"), "1.1")
                    .build()
            );
        inOrder.verify(tracer).stopTrace(request);
        inOrder.verifyNoMoreInteractions();
    }

    public void test_end_setsResponseAttributes_full() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/my-index/_search")
            .build();
        RestResponse response = new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);

        when(tracer.spanStatusBuilder(request)).thenReturn(spanStatusBuilder);

        instrumentation.end(request, response);

        var inOrder = inOrder(tracer, spanStatusBuilder);
        inOrder.verify(tracer).setAttribute(request, "http.status_code", 500L);
        inOrder.verify(tracer)
            .setAttributes(
                request,
                Attributes.builder()
                    .put(longKey("http.response.status_code"), 500L)
                    .put(stringKey("error.type"), "500")
                    .put(stringKey("network.protocol.version"), "1.1")
                    .build()
            );
        inOrder.verify(spanStatusBuilder).setStatus(StatusCode.ERROR);
        inOrder.verify(tracer).stopTrace(request);
        inOrder.verifyNoMoreInteractions();
    }
}
