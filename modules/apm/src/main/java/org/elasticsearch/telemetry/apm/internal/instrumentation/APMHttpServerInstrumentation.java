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
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.SpanNameExtractor;
import io.opentelemetry.instrumentation.api.semconv.http.HttpServerAttributesExtractor;
import io.opentelemetry.instrumentation.api.semconv.http.HttpSpanNameExtractor;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.telemetry.instrumentation.HttpServerSemConvAttributes;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * APM-backed {@link HttpServerInstrumentation} that uses the OTel {@code instrumentation-api}
 * library to compute spec-compliant HTTP server semconv attributes.
 * <p>
 * The OTel {@code Instrumenter} API is intentionally NOT used because it is incompatible with
 * ES's custom span-lifecycle management in {@link APMTracer}. Instead, only
 * {@link AttributesExtractor} and {@link SpanNameExtractor} are used for attribute computation
 * while span start/stop/error are delegated to the existing {@link APMTracer}.
 * <p>
 * Backward-compatible legacy attributes ({@code http.method}, {@code http.url}, {@code http.flavour},
 * {@code http.status_code}, {@code http.request/response.headers.*}) are dual-written alongside
 * the new OTel semconv attributes so that existing APM dashboards continue to function.
 */
public class APMHttpServerInstrumentation implements HttpServerInstrumentation {

    private final APMTracer tracer;
    private final SpanNameExtractor<RequestAndRoute> spanNameExtractor;
    private final AttributesExtractor<RequestAndRoute, RestResponse> httpServerAttributesExtractor;

    public APMHttpServerInstrumentation(APMTracer tracer) {
        this.tracer = tracer;
        final var getter = new OtelAttributesGetter();
        this.spanNameExtractor = HttpSpanNameExtractor.builder(getter).build();
        this.httpServerAttributesExtractor = HttpServerAttributesExtractor.builder(getter).build();
    }

    @Override
    public void start(ThreadContext threadContext, RestRequest request, String matchedRoute) {
        final var req = new RequestAndRoute(request, matchedRoute);
        final String spanName = spanNameExtractor.extract(req);

        // Legacy attributes — kept for backward compat with existing APM dashboards.
        // These also trigger SpanKind.SERVER detection in APMTracer (keys starting with "http.").
        tracer.startTrace(threadContext, request, spanName, legacyRequestAttributes(request));

        // OTel semconv attributes (url.*, http.request.method, network.protocol.*, server.*, http.route)
        final var otelAttrs = Attributes.builder();
        httpServerAttributesExtractor.onStart(otelAttrs, Context.root(), req);

        // server.address / server.port: HttpServerAttributesGetter does not extend ServerAttributesGetter,
        // so the extractor cannot derive these. Resolve them here with proxy-header priority.
        final String forwarded = HttpServerSemConvAttributes.firstHeaderValue(request, "Forwarded");
        final String serverAddress = HttpServerSemConvAttributes.extractServerAddress(request, request.getHttpChannel(), forwarded);
        if (serverAddress != null) {
            otelAttrs.put(AttributeKey.stringKey("server.address"), serverAddress);
            final Integer serverPort = HttpServerSemConvAttributes.extractServerPort(request, request.getHttpChannel(), forwarded);
            if (serverPort != null) {
                otelAttrs.put(AttributeKey.longKey("server.port"), (long) serverPort);
            }
        }

        tracer.setAttributes(request, otelAttrs.build());
    }

    @Override
    public void recordException(RestRequest request, Throwable t) {
        tracer.addError(request, t);
    }

    @Override
    public void end(RestRequest request, RestResponse response) {
        final var req = new RequestAndRoute(request, null);

        // OTel semconv response attributes (http.response.status_code, http.response.headers.*)
        final var otelAttrs = Attributes.builder();
        httpServerAttributesExtractor.onEnd(otelAttrs, Context.root(), req, response, null);
        tracer.setAttributes(request, otelAttrs.build());

        // Legacy backward-compat attributes
        tracer.setAttribute(request, "http.status_code", (long) response.status().getStatus());
        response.getHeaders()
            .forEach((key, values) -> tracer.setAttribute(request, "http.response.headers." + key, String.join("; ", values)));

        tracer.stopTrace(request);
    }

    /** Builds the legacy attribute map that preserves backward compatibility with APM dashboards. */
    private static Map<String, Object> legacyRequestAttributes(RestRequest req) {
        String method = null;
        try {
            method = req.method().name();
        } catch (IllegalArgumentException e) {
            // invalid method — will be recorded as <unknown>
        }
        final Map<String, Object> attrs = Maps.newMapWithExpectedSize(req.getHeaders().size() + 3);
        req.getHeaders().forEach((key, values) -> {
            final String lowerKey = key.toLowerCase(Locale.ROOT).replace('-', '_');
            attrs.put("http.request.headers." + lowerKey, values == null ? "" : String.join("; ", values));
        });
        attrs.put("http.method", Objects.requireNonNullElse(method, "<unknown>"));
        attrs.put("http.url", Objects.requireNonNullElse(req.uri(), "<unknown>"));
        switch (req.getHttpRequest().protocolVersion()) {
            case HTTP_1_0 -> attrs.put("http.flavour", "1.0");
            case HTTP_1_1 -> attrs.put("http.flavour", "1.1");
        }
        return attrs;
    }
}
