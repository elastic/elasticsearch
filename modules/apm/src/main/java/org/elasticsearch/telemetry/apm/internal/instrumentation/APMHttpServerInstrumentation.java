/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.instrumentation;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.SpanNameExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.SpanStatusBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.SpanStatusExtractor;
import io.opentelemetry.instrumentation.api.semconv.http.HttpServerAttributesExtractor;
import io.opentelemetry.instrumentation.api.semconv.http.HttpServerAttributesGetter;
import io.opentelemetry.instrumentation.api.semconv.http.HttpSpanNameExtractor;
import io.opentelemetry.instrumentation.api.semconv.http.HttpSpanStatusExtractor;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.telemetry.tracing.TracingContext;

import java.time.Instant;
import java.util.List;
import java.util.Locale;

public class APMHttpServerInstrumentation implements HttpServerInstrumentation {

    private final Tracer tracer;

    private final HttpServerAttributesGetter<RequestAndRoute, RestResponse> getter;
    private final SpanNameExtractor<RequestAndRoute> spanNameExtractor;
    private final AttributesExtractor<RequestAndRoute, RestResponse> httpServerAttributesExtractor;
    private final SpanStatusExtractor<RequestAndRoute, RestResponse> httpSpanStatusExtractor;

    public APMHttpServerInstrumentation(OpenTelemetry openTelemetry) {
        this.tracer = openTelemetry.getTracer("elasticsearch.http");

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
        if (request.isTraceStarted()) {
            return;
        }
        start(threadContext, request, matchedRoute, TracingContext.extract(threadContext));
    }

    @Override
    public void start(ThreadContext threadContext, RestRequest request, String matchedRoute, Context parent) {
        if (request.isTraceStarted()) {
            return;
        }
        var req = new RequestAndRoute(request, matchedRoute);
        var attributes = Attributes.builder();
        httpServerAttributesExtractor.onStart(attributes, parent, req);
        // TODO: Preserve header capture for compatibility; revisit an explicit allowlist independently of this migration.
        req.request().getHeaders().forEach((key, values) -> {
            attributes.put(
                AttributeKey.stringArrayKey("http.request.header." + key.toLowerCase(Locale.ROOT)),
                values == null ? List.of() : values
            );
        });
        String opaqueId = threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER);
        if (opaqueId != null) {
            attributes.put("es.x-opaque-id", opaqueId);
        }
        String projectId = threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
        if (projectId != null) {
            attributes.put("project.id", projectId);
        }
        var builder = tracer.spanBuilder(spanNameExtractor.extract(req))
            .setParent(parent)
            .setSpanKind(SpanKind.SERVER)
            .setAllAttributes(attributes.build());
        Instant received = threadContext.getTransient(Task.TRACE_START_TIME);
        if (received != null) {
            builder.setStartTimestamp(received);
        }
        // Loses the race harmlessly: startTrace ends the span it rejects.
        request.startTrace(parent, builder.startSpan());
    }

    @Override
    public void recordException(RestRequest request, Throwable t) {
        request.withTraceSpan(span -> span.recordException(t));
    }

    @Override
    public void end(RestRequest request, RestResponse response) {
        // Bad requests can reach this hook without ever reaching start(); annotating and ending is then skipped entirely.
        request.finishTrace(span -> {
            var requestAndRoute = new RequestAndRoute(request, /* only needed at start */ null);
            var attributes = Attributes.builder();
            httpServerAttributesExtractor.onEnd(
                attributes,
                /* we don't care about the context in this case */ Context.root(),
                requestAndRoute,
                response,
                null
            );
            response.getHeaders()
                .forEach(
                    (key, values) -> attributes.put(
                        AttributeKey.stringArrayKey("http.response.header." + key.toLowerCase(Locale.ROOT)),
                        values
                    )
                );
            span.setAllAttributes(attributes.build());
            httpSpanStatusExtractor.extract(new SpanStatusBuilder() {
                @Override
                public SpanStatusBuilder setStatus(StatusCode status, String description) {
                    span.setStatus(status, description);
                    return this;
                }
            }, requestAndRoute, response, null);
        });
    }
}
