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
import io.opentelemetry.instrumentation.api.instrumenter.SpanStatusExtractor;
import io.opentelemetry.instrumentation.api.semconv.http.HttpServerAttributesExtractor;
import io.opentelemetry.instrumentation.api.semconv.http.HttpServerAttributesGetter;
import io.opentelemetry.instrumentation.api.semconv.http.HttpSpanNameExtractor;
import io.opentelemetry.instrumentation.api.semconv.http.HttpSpanStatusExtractor;

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

    private final HttpServerAttributesGetter<RequestAndRoute, RestResponse> getter;
    private final SpanNameExtractor<RequestAndRoute> spanNameExtractor;
    private final AttributesExtractor<RequestAndRoute, RestResponse> httpServerAttributesExtractor;
    private final SpanStatusExtractor<RequestAndRoute, RestResponse> httpSpanStatusExtractor;

    public APMHttpServerInstrumentation(APMTracer tracer) {
        this.tracer = tracer;

        this.getter = new OtelAttributesGetter();
        this.spanNameExtractor = HttpSpanNameExtractor.create(getter);
        this.httpServerAttributesExtractor = HttpServerAttributesExtractor.builder(getter)
            .setCapturedRequestHeaders(List.of(/* TODO which headers? */))
            .setCapturedResponseHeaders(List.of(/* TODO which headers? */))
            .build();
        this.httpSpanStatusExtractor = HttpSpanStatusExtractor.create(getter);
    }

    @Override
    public void start(ThreadContext threadContext, RestRequest request, String matchedRoute) {
        var req = new RequestAndRoute(request, matchedRoute);
        tracer.startTrace(threadContext, request, spanNameExtractor.extract(req), legacyRequestAttributes(req));

        var attributes = Attributes.builder();
        httpServerAttributesExtractor.onStart(attributes, /* we don't care about the context in this case */ Context.root(), req);
        tracer.setAttributes(request, attributes.build());
    }

    private Map<String, Object> legacyRequestAttributes(RequestAndRoute req) {
        final Map<String, Object> attributes = Maps.newMapWithExpectedSize(req.request().getHeaders().size() + 3);
        req.request().getHeaders().forEach((key, values) -> {
            final String lowerKey = key.toLowerCase(Locale.ROOT).replace('-', '_');
            attributes.put("http.request.headers." + lowerKey, values == null ? "" : String.join("; ", values));
        });
        attributes.put("http.method", Objects.requireNonNullElse(getter.getHttpRequestMethod(req), "<unknown>"));
        attributes.put("http.url", Objects.requireNonNullElse(req.request().uri(), "<unknown>"));
        attributes.put("http.flavour", getter.getNetworkProtocolVersion(req, null));
        return attributes;
    }

    @Override
    public void recordException(RestRequest request, Throwable t) {
        this.tracer.addError(request, t);
    }

    @Override
    public void end(RestRequest request, RestResponse response) {
        setLegacyResponseAttributes(request, response);

        var requestAndRoute = new RequestAndRoute(request, /* only needed at start */ null);
        var attributes = Attributes.builder();
        httpServerAttributesExtractor.onEnd(
            attributes,
            /* we don't care about the context in this case */ Context.root(),
            requestAndRoute,
            response,
            null
        );
        tracer.setAttributes(request, attributes.build());

        httpSpanStatusExtractor.extract(tracer.spanStatusBuilder(request), requestAndRoute, response, null);
        tracer.stopTrace(request);
    }

    private void setLegacyResponseAttributes(RestRequest request, RestResponse response) {
        tracer.setAttribute(request, "http.status_code", response.status().getStatus());
        response.getHeaders()
            .forEach((key, values) -> tracer.setAttribute(request, "http.response.headers." + key, String.join("; ", values)));
    }
}
