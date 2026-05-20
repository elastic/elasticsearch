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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class APMHttpServerInstrumentation implements HttpServerInstrumentation {

    private final APMTracer tracer;

    private final SpanNameExtractor<RequestAndRoute> spanNameExtractor;
    private final AttributesExtractor<RequestAndRoute, RestResponse> httpServerAttributesExtractor;

    public APMHttpServerInstrumentation(APMTracer tracer) {
        this.tracer = tracer;

        var getter = new OtelAttributesGetter();
        this.spanNameExtractor = HttpSpanNameExtractor.create(getter);
        this.httpServerAttributesExtractor = HttpServerAttributesExtractor.builder(getter)
            .setCapturedRequestHeaders(List.of(/* TODO which headers? */))
            .setCapturedResponseHeaders(List.of(/* TODO which headers? */))
            .build();
    }

    @Override
    public void start(ThreadContext threadContext, RestRequest request, String matchedRoute) {
        var req = new RequestAndRoute(request, matchedRoute);
        tracer.startTrace(threadContext, request, spanNameExtractor.extract(req), oldRequestAttributes(request));

        var attributes = Attributes.builder();
        httpServerAttributesExtractor.onStart(attributes, /* we don't care about the context in this case */ Context.root(), req);
        tracer.setAttributes(request, attributes.build());
    }

    private static Map<String, Object> oldRequestAttributes(RestRequest req) {
        String method = null;
        try {
            method = req.method().name();
        } catch (IllegalArgumentException e) {
            // Invalid methods throw an exception
        }

        final Map<String, Object> attributes = Maps.newMapWithExpectedSize(req.getHeaders().size() + 3);
        req.getHeaders().forEach((key, values) -> {
            final String lowerKey = key.toLowerCase(Locale.ROOT).replace('-', '_');
            attributes.put("http.request.headers." + lowerKey, values == null ? "" : String.join("; ", values));
        });
        attributes.put("http.method", Objects.requireNonNullElse(method, "<unknown>"));
        attributes.put("http.url", Objects.requireNonNullElse(req.uri(), "<unknown>"));
        switch (req.getHttpRequest().protocolVersion()) {
            case HTTP_1_0 -> attributes.put("http.flavour", "1.0");
            case HTTP_1_1 -> attributes.put("http.flavour", "1.1");
        }
        return attributes;
    }

    @Override
    public void recordException(RestRequest request, Throwable t) {
        this.tracer.addError(request, t);
    }

    @Override
    public void end(RestRequest request, RestResponse response) {
        setOldResponseAttributes(request, response);

        var attributes = Attributes.builder();
        httpServerAttributesExtractor.onEnd(
            attributes,
            /* we don't care about the context in this case */ Context.root(),
            new RequestAndRoute(request, /* only needed at start */ null),
            response,
            null
        );
        tracer.setAttributes(request, attributes.build());

        tracer.stopTrace(request);
    }

    private void setOldResponseAttributes(RestRequest request, RestResponse response) {
        tracer.setAttribute(request, "http.status_code", response.status().getStatus());
        response.getHeaders()
            .forEach((key, values) -> tracer.setAttribute(request, "http.response.headers." + key, String.join("; ", values)));
    }
}
