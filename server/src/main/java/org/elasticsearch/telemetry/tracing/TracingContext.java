/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Connects native OTel contexts to Elasticsearch execution and wire boundaries without owning spans. */
public final class TracingContext {
    private static final List<String> TRACE_HEADERS = List.of(Task.TRACE_PARENT_HTTP_HEADER, Task.TRACE_STATE, Task.TRACE_ID);
    private static final TextMapGetter<ThreadContext> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(ThreadContext carrier) {
            return TRACE_HEADERS;
        }

        @Override
        public String get(ThreadContext carrier, String key) {
            return carrier.getStoredHeader(key);
        }
    };

    private TracingContext() {}

    /** Incoming work must not inherit context left on a reused worker by another request. */
    public static Context extract(ThreadContext threadContext) {
        return W3CTraceContextPropagator.getInstance().extract(Context.root(), threadContext, GETTER);
    }

    /** Activates a borrowed context and restores headers and scope together, without ending its span. */
    public static Releasable activate(ThreadContext threadContext, Context context) {
        var stored = threadContext.newStoredContextPreservingResponseHeaders(List.of(), TRACE_HEADERS);
        try {
            W3CTraceContextPropagator.getInstance().inject(context, threadContext, ThreadContext::putHeader);
            var spanContext = Span.fromContext(context).getSpanContext();
            if (spanContext.isValid()) {
                threadContext.putHeader(Task.TRACE_ID, spanContext.getTraceId());
            }
            var scope = context.makeCurrent();
            return () -> {
                try {
                    scope.close();
                } finally {
                    stored.close();
                }
            };
        } catch (RuntimeException | Error failure) {
            stored.close();
            throw failure;
        }
    }

    /** Snapshots the active native context at dispatch, including contexts from ordinary component instrumentation. */
    public static Map<String, String> headers(Map<String, String> original) {
        Context context = Context.current();
        var spanContext = Span.fromContext(context).getSpanContext();
        if (spanContext.isValid() == false) {
            return original;
        }
        Map<String, String> headers = new HashMap<>(original);
        TRACE_HEADERS.forEach(headers::remove);
        W3CTraceContextPropagator.getInstance().inject(context, headers, Map::put);
        headers.put(Task.TRACE_ID, spanContext.getTraceId());
        return headers;
    }

    /** Transport envelopes must not turn routine cancellation into a server error. */
    public static void recordFailure(Span span, Throwable failure) {
        Throwable cause = ExceptionsHelper.unwrapCause(failure);
        boolean cancelled = cause instanceof TaskCancelledException;
        span.setAttribute("es.outcome", cancelled ? "cancelled" : "failure");
        span.setAttribute("error.type", cause.getClass().getName());
        if (cancelled == false) {
            span.setStatus(StatusCode.ERROR);
        }
    }
}
