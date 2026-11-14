/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.SpanStatusBuilder;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.tracing.Span;

import static java.util.Objects.requireNonNull;

public abstract class AbstractSpan implements Span, SpanStatusBuilder {

    final APMTracer tracer;
    final Context otelContext;

    AbstractSpan(APMTracer tracer, Context otelContext) {
        this.tracer = tracer;
        this.otelContext = otelContext;
    }

    /** The context that holds the span, which is what its children are started under. */
    Context context() {
        return otelContext;
    }

    static boolean isSpanRecording(Context otelContext) {
        return io.opentelemetry.api.trace.Span.fromContext(otelContext).isRecording();
    }

    public abstract void setAllAttributes(Attributes attributes);

    @Override
    public ThreadContext.StoredContext storeInThreadContext() {
        final ThreadContext threadContext = tracer.threadContext();
        final ThreadContext.StoredContext storedContext = threadContext.newStoredContextPreservingResponseHeaders();
        updateThreadContext(threadContext, otelContext);
        return storedContext;
    }

    private void updateThreadContext(ThreadContext threadContext, Context context) {
        threadContext.putTransientAllowOverwrite(Task.APM_TRACE_CONTEXT, context);

        // the propagator does not write an empty tracestate, so make sure that no stale value is left behind
        threadContext.removeHeaders(Task.TRACE_PARENT_HTTP_HEADER, Task.TRACE_STATE);

        // propagate current trace headers into thread context
        if (tracer.isEnabled()) {
            tracer.openTelemetry()
                .getPropagators()
                .getTextMapPropagator()
                .inject(context, threadContext, (tc, key, value) -> requireNonNull(tc).putHeaderAllowOverwrite(key, value));
        }
    }
}
