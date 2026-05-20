/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.instrumentation;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link APMHttpServerInstrumentation}.
 * Uses a Mockito mock of {@link APMTracer} to capture tracer calls without requiring a full OTel agent.
 */
public class APMHttpServerInstrumentationTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testStartWritesLegacyAndOtelAttributes() {
        APMTracer tracer = mock(APMTracer.class);
        APMHttpServerInstrumentation inst = new APMHttpServerInstrumentation(tracer);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/search?q=test")
            .withHeaders(Map.of("Host", List.of("myhost:9200")))
            .build();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        inst.start(threadContext, request, "/search");

        // Verify span name was derived from the OTel name extractor (GET /search)
        ArgumentCaptor<String> spanNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map<String, Object>> legacyAttrsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(tracer).startTrace(eq(threadContext), eq(request), spanNameCaptor.capture(), legacyAttrsCaptor.capture());

        assertThat(spanNameCaptor.getValue(), equalTo("GET /search"));

        Map<String, Object> legacy = legacyAttrsCaptor.getValue();
        assertThat(legacy.get("http.method"), equalTo("GET"));
        assertThat(legacy.get("http.url"), equalTo("/search?q=test"));
        assertThat(legacy.get("http.flavour"), equalTo("1.1"));

        // Verify OTel attributes were applied
        ArgumentCaptor<Attributes> otelAttrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(tracer).setAttributes(eq(request), otelAttrsCaptor.capture());

        Attributes otel = otelAttrsCaptor.getValue();
        assertThat(otel.get(AttributeKey.stringKey("http.request.method")), equalTo("GET"));
        assertThat(otel.get(AttributeKey.stringKey("url.path")), equalTo("/search"));
        assertThat(otel.get(AttributeKey.stringKey("url.query")), equalTo("q=test"));
        assertThat(otel.get(AttributeKey.stringKey("url.scheme")), equalTo("http"));
        assertThat(otel.get(AttributeKey.stringKey("server.address")), equalTo("myhost"));
        assertThat(otel.get(AttributeKey.longKey("server.port")), equalTo(9200L));
        // network.protocol.name/version are response-phase attributes set in end(), not start()
    }

    @SuppressWarnings("unchecked")
    public void testStartWritesLegacyRequestHeaders() {
        APMTracer tracer = mock(APMTracer.class);
        APMHttpServerInstrumentation inst = new APMHttpServerInstrumentation(tracer);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/index/_doc")
            .withHeaders(Map.of("Content-Type", List.of("application/json"), "Host", List.of("localhost")))
            .build();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        inst.start(threadContext, request, "/index/_doc");

        ArgumentCaptor<Map<String, Object>> legacyAttrsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(tracer).startTrace(any(), any(RestRequest.class), anyString(), legacyAttrsCaptor.capture());

        Map<String, Object> legacy = legacyAttrsCaptor.getValue();
        assertThat(legacy, hasKey("http.request.headers.content_type"));
    }

    public void testEndWritesLegacyAndOtelResponseAttributes() {
        APMTracer tracer = mock(APMTracer.class);
        APMHttpServerInstrumentation inst = new APMHttpServerInstrumentation(tracer);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET).withPath("/").build();
        RestResponse response = new RestResponse(RestStatus.OK, "test");

        inst.end(request, response);

        // OTel http.response.status_code via setAttributes
        ArgumentCaptor<Attributes> attrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(tracer).setAttributes(eq(request), attrsCaptor.capture());
        Attributes otelAttrs = attrsCaptor.getValue();
        assertThat(otelAttrs.get(AttributeKey.longKey("http.response.status_code")), equalTo(200L));
        // The OTel library omits network.protocol.name when it equals "http" (the implied default for HTTP spans).
        // network.protocol.version is a response-phase attribute set here.
        assertThat(otelAttrs.get(AttributeKey.stringKey("network.protocol.version")), equalTo("1.1"));

        // Legacy http.status_code as long
        verify(tracer).setAttribute(eq(request), eq("http.status_code"), eq(200L));

        // Span stopped
        verify(tracer).stopTrace(eq(request));
    }

    public void testRecordExceptionDelegatesToTracer() {
        APMTracer tracer = mock(APMTracer.class);
        APMHttpServerInstrumentation inst = new APMHttpServerInstrumentation(tracer);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
        RuntimeException error = new RuntimeException("boom");

        inst.recordException(request, error);

        verify(tracer).addError(eq(request), eq(error));
    }

    public void testStartSetsHttpRouteFromMatchedRoute() {
        APMTracer tracer = mock(APMTracer.class);
        APMHttpServerInstrumentation inst = new APMHttpServerInstrumentation(tracer);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/index/_search")
            .withHeaders(Map.of("Host", List.of("localhost")))
            .build();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        inst.start(threadContext, request, "/{index}/_search");

        ArgumentCaptor<Attributes> attrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(tracer).setAttributes(eq(request), attrsCaptor.capture());

        Attributes otelAttrs = attrsCaptor.getValue();
        assertThat(otelAttrs.get(AttributeKey.stringKey("http.route")), equalTo("/{index}/_search"));
    }

    public void testStartDerivesSpanNameFromRoute() {
        APMTracer tracer = mock(APMTracer.class);
        APMHttpServerInstrumentation inst = new APMHttpServerInstrumentation(tracer);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/my-index")
            .withHeaders(Map.of("Host", List.of("localhost")))
            .build();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        inst.start(threadContext, request, "/{index}");

        ArgumentCaptor<String> spanNameCaptor = ArgumentCaptor.forClass(String.class);
        verify(tracer).startTrace(any(), any(RestRequest.class), spanNameCaptor.capture(), any());

        // OTel HttpSpanNameExtractor uses the matched route when available
        assertThat(spanNameCaptor.getValue(), notNullValue());
    }
}
