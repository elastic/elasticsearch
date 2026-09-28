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
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.FilteredRestRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracingService;
import org.elasticsearch.telemetry.apm.internal.tracing.NativeTracingFixture;
import org.elasticsearch.telemetry.tracing.TracingContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** HTTP boundaries use native spans while retaining capture and sanitization policy. */
public class APMHttpServerInstrumentationTests extends ESTestCase {
    public void testModernAttributesAndHeaderCapture() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var instrumentation = new APMHttpServerInstrumentation(fixture.api);
            var request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
                .withScheme("https")
                .withPath("/my-index/_search?pretty=true")
                .withHeaders(Map.of("Accept-Encoding", List.of("gzip", "br"), "Authorization", List.of("secret")))
                .build();
            var context = new ThreadContext(Settings.EMPTY);
            instrumentation.start(context, request, "/{index}/_search");
            try (var scope = TracingContext.activate(context, request.getTraceContext())) {
                fixture.api.getTracer("arbitrary-component").spanBuilder("child").startSpan().end();
            }
            var response = new RestResponse(RestStatus.OK, "text/plain", "ok");
            response.addHeader("X-Debug-Tag", "response-tag");
            response.addHeader("X-Debug-Tag", "second-tag");
            instrumentation.end(request, response);
            var span = fixture.span("GET /{index}/_search");
            assertEquals(SpanKind.SERVER, span.getKind());
            assertEquals("GET", span.getAttributes().get(AttributeKey.stringKey("http.request.method")));
            assertEquals("https", span.getAttributes().get(AttributeKey.stringKey("url.scheme")));
            assertEquals("/{index}/_search", span.getAttributes().get(AttributeKey.stringKey("http.route")));
            assertEquals("/my-index/_search", span.getAttributes().get(AttributeKey.stringKey("url.path")));
            assertEquals("pretty=true", span.getAttributes().get(AttributeKey.stringKey("url.query")));
            assertEquals(Long.valueOf(200), span.getAttributes().get(AttributeKey.longKey("http.response.status_code")));
            assertNull(span.getAttributes().get(AttributeKey.stringKey("http.method")));
            assertNull(span.getAttributes().get(AttributeKey.longKey("http.status_code")));
            assertEquals(
                List.of("gzip", "br"),
                span.getAttributes().get(AttributeKey.stringArrayKey("http.request.header.accept-encoding"))
            );
            assertEquals("[REDACTED]", span.getAttributes().get(AttributeKey.stringKey("http.request.header.authorization")));
            assertEquals(
                List.of("response-tag", "second-tag"),
                span.getAttributes().get(AttributeKey.stringArrayKey("http.response.header.x-debug-tag"))
            );
            assertEquals(span.getSpanId(), fixture.span("child").getParentSpanId());
        }
    }

    public void testLegacySanitizerNamesStillProtectModernAttributes() {
        try (
            var fixture = new NativeTracingFixture(
                Settings.builder()
                    .putList(
                        "telemetry.tracing.sanitize_field_names",
                        "http.request.headers.x_private",
                        "http.response.headers.X-Private",
                        "http.url"
                    )
                    .build()
            )
        ) {
            var instrumentation = new APMHttpServerInstrumentation(fixture.api);
            var request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/sensitive?value=secret")
                .withHeaders(Map.of("X-Private", List.of("secret")))
                .build();
            instrumentation.start(new ThreadContext(Settings.EMPTY), request, "/sensitive");
            var response = new RestResponse(RestStatus.OK, "text/plain", "ok");
            response.addHeader("X-Private", "secret");
            instrumentation.end(request, response);
            var attributes = fixture.exporter.getFinishedSpanItems().getFirst().getAttributes();
            assertEquals("[REDACTED]", attributes.get(AttributeKey.stringKey("http.request.header.x-private")));
            assertEquals("[REDACTED]", attributes.get(AttributeKey.stringKey("http.response.header.x-private")));
            assertEquals("[REDACTED]", attributes.get(AttributeKey.stringKey("url.path")));
            assertEquals("[REDACTED]", attributes.get(AttributeKey.stringKey("url.query")));
        }
    }

    public void testServerErrorAndRepeatedLifecycleCalls() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var instrumentation = new APMHttpServerInstrumentation(fixture.api);
            var context = new ThreadContext(Settings.EMPTY);
            var request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_test").build();
            instrumentation.start(context, request, "/_test");
            instrumentation.start(context, request, "/_test");
            instrumentation.recordException(request, new IllegalArgumentException("failed"));
            var response = new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, "text/plain", "failed");
            instrumentation.end(request, response);
            instrumentation.end(request, response);
            assertEquals(1, fixture.exporter.getFinishedSpanItems().size());
            var span = fixture.exporter.getFinishedSpanItems().getFirst();
            assertEquals(StatusCode.ERROR, span.getStatus().getStatusCode());
            assertEquals(1, span.getEvents().size());
            assertNull(span.getEvents().getFirst().getAttributes().get(AttributeKey.stringKey("exception.stacktrace")));
        }
    }

    public void testAnnotatorFailureStillEndsRequestSpan() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var instrumentation = new APMHttpServerInstrumentation(fixture.api);
            var request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_test").build();
            instrumentation.start(new ThreadContext(Settings.EMPTY), request, "/_test");

            expectThrows(
                IllegalArgumentException.class,
                () -> request.finishTrace(span -> { throw new IllegalArgumentException("failed to annotate"); })
            );
            instrumentation.end(request, new RestResponse(RestStatus.OK, "text/plain", "ok"));
            assertEquals(1, fixture.exporter.getFinishedSpanItems().size());
            assertEquals(
                "/_test",
                fixture.exporter.getFinishedSpanItems().getFirst().getAttributes().get(AttributeKey.stringKey("url.path"))
            );
        }
    }

    /**
     * Malformed requests are rejected before routing, so they reach the end hook without ever reaching the start hook.
     * Nothing may be ended or annotated then, in particular not the inbound parent span the request borrowed.
     */
    public void testResponseWithoutStartEndsAndAnnotatesNothing() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var instrumentation = new APMHttpServerInstrumentation(fixture.api);
            var incoming = fixture.sdk.getTracer("client").spanBuilder("incoming").startSpan();
            var context = new ThreadContext(Settings.EMPTY);
            W3CTraceContextPropagator.getInstance().inject(Context.root().with(incoming), context, ThreadContext::putHeader);
            var request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_test").build();
            request.borrowTraceContext(TracingContext.extract(context));

            instrumentation.recordException(request, new IllegalArgumentException("malformed"));
            instrumentation.end(request, new RestResponse(RestStatus.BAD_REQUEST, "text/plain", "malformed"));

            assertEquals(List.of(), fixture.exporter.getFinishedSpanItems());
            assertTrue(incoming.isRecording());
            incoming.end();
            var borrowed = fixture.exporter.getFinishedSpanItems().getFirst();
            assertEquals(StatusCode.UNSET, borrowed.getStatus().getStatusCode());
            assertEquals(0, borrowed.getEvents().size());
            assertNull(borrowed.getAttributes().get(AttributeKey.longKey("http.response.status_code")));
        }
    }

    /**
     * Handlers may wrap a request at any point, including before instrumentation starts. A wrapper and its original
     * are the same HTTP operation, so a span started through either must be visible and endable through both.
     */
    public void testWrappedRequestSharesTracingStateWithTheOriginal() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var instrumentation = new APMHttpServerInstrumentation(fixture.api);
            var context = new ThreadContext(Settings.EMPTY);
            var request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_test").build();
            // Copied before tracing starts, so it cannot have inherited a span at construction time.
            var wrapper = new FilteredRestRequest(request, Set.of("secret"));
            assertFalse(wrapper.isTraceStarted());

            instrumentation.start(context, request, "/_test");
            assertTrue(wrapper.isTraceStarted());
            assertEquals(request.getTraceContext(), wrapper.getTraceContext());
            // Starting again through the wrapper must not open a second span.
            instrumentation.start(context, wrapper, "/_test");

            instrumentation.end(wrapper, new RestResponse(RestStatus.OK, "text/plain", "ok"));
            // The original now sees the completed span too, so responding twice cannot end it again.
            instrumentation.end(request, new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, "text/plain", "failed"));

            assertEquals(1, fixture.exporter.getFinishedSpanItems().size());
            var span = fixture.exporter.getFinishedSpanItems().getFirst();
            assertEquals(Long.valueOf(200), span.getAttributes().get(AttributeKey.longKey("http.response.status_code")));
            assertEquals(StatusCode.UNSET, span.getStatus().getStatusCode());
        }
    }

    public void testIncomingParentOverridesAmbientSpan() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var instrumentation = new APMHttpServerInstrumentation(fixture.api);
            var incoming = fixture.sdk.getTracer("client").spanBuilder("incoming").startSpan();
            var unrelated = fixture.sdk.getTracer("client").spanBuilder("unrelated").startSpan();
            var context = new ThreadContext(Settings.EMPTY);
            W3CTraceContextPropagator.getInstance().inject(Context.root().with(incoming), context, ThreadContext::putHeader);
            var request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_test").build();
            try (var scope = unrelated.makeCurrent()) {
                instrumentation.start(context, request, "/_test");
                assertEquals(unrelated.getSpanContext(), Span.current().getSpanContext());
                instrumentation.end(request, new RestResponse(RestStatus.OK, "text/plain", "ok"));
            }
            var serverSpan = fixture.exporter.getFinishedSpanItems().getFirst();
            assertEquals(incoming.getSpanContext().getSpanId(), serverSpan.getParentSpanId());
            incoming.end();
            unrelated.end();
        }
    }

    public void testPreExtractedParentIsUsedForServerSpan() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var instrumentation = new APMHttpServerInstrumentation(fixture.api);
            var incoming = fixture.sdk.getTracer("client").spanBuilder("incoming").startSpan();
            var unrelated = fixture.sdk.getTracer("client").spanBuilder("unrelated").startSpan();
            var threadContext = new ThreadContext(Settings.EMPTY);
            W3CTraceContextPropagator.getInstance().inject(Context.root().with(unrelated), threadContext, ThreadContext::putHeader);
            var request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_test").build();
            var parent = Context.root().with(incoming);
            request.borrowTraceContext(parent);

            instrumentation.start(threadContext, request, "/_test", parent);
            instrumentation.end(request, new RestResponse(RestStatus.OK, "text/plain", "ok"));
            assertEquals(incoming.getSpanContext().getSpanId(), fixture.exporter.getFinishedSpanItems().getFirst().getParentSpanId());

            incoming.end();
            unrelated.end();
        }
    }

    public void testUnconfiguredServiceUsesNoExporter() {
        try (var service = new APMTracingService(Settings.EMPTY, MeterProvider::noop)) {
            var instrumentation = new APMHttpServerInstrumentation(service.getOpenTelemetry());
            for (int index = 0; index < 10; index++) {
                var request = new FakeRestRequest.Builder(xContentRegistry()).build();
                instrumentation.start(new ThreadContext(Settings.EMPTY), request, "/");
                assertFalse(Span.fromContext(request.getTraceContext()).isRecording());
                instrumentation.end(request, new RestResponse(RestStatus.OK, "text/plain", "ok"));
            }
        }
    }
}
