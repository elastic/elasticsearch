/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.tracing;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class TracerSpan {

    private record Span(String spanId) implements Traceable {

        @Override
        public String getSpanId() {
            return spanId;
        }

        public static Span create() {
            return new Span(UUIDs.randomBase64UUID());
        }
    }

    public static Releasable span(ThreadPool threadPool, Tracer tracer, String name) {
        return span(threadPool, tracer, name, Map.of());
    }

    /**
     * Creates a new span for a synchronous block of code.
     * @return a span that needs to be released once block of code is completed
     */
    public static Releasable span(ThreadPool threadPool, Tracer tracer, String name, Map<String, Object> attributes) {
        if (tracer.isEnabled() == false) {
            return () -> {};
        }
        var span = Span.create();
        var ctx = threadPool.getThreadContext().newTraceContext();
        tracer.startTrace(threadPool.getThreadContext(), span, name, attributes);

        return () -> {
            tracer.stopTrace(span);
            ctx.restore();
        };
    }

    private static class SubThreadContext implements TraceContext {
        ThreadContext.ThreadContextStruct ctx;

        SubThreadContext(ThreadContext parent) {
            ThreadContext.ThreadContextStruct ctx = parent.getThreadContextStruct();
            final Map<String, String> newRequestHeaders = new HashMap<>(ctx.requestHeaders);
            final Map<String, Object> newTransientHeaders = new HashMap<>(ctx.transientHeaders);

            final String previousTraceParent = newRequestHeaders.remove(Task.TRACE_PARENT_HTTP_HEADER);
            if (previousTraceParent != null) {
                newTransientHeaders.put("parent_" + Task.TRACE_PARENT_HTTP_HEADER, previousTraceParent);
            }

            final String previousTraceState = newRequestHeaders.remove(Task.TRACE_STATE);
            if (previousTraceState != null) {
                newTransientHeaders.put("parent_" + Task.TRACE_STATE, previousTraceState);
            }

            final Object previousTraceContext = newTransientHeaders.remove(Task.APM_TRACE_CONTEXT);
            if (previousTraceContext != null) {
                newTransientHeaders.put("parent_" + Task.APM_TRACE_CONTEXT, previousTraceContext);
            }

            // this is the context when this method returns
            ThreadContext.ThreadContextStruct newContext = new ThreadContext.ThreadContextStruct(
                newRequestHeaders,
                ctx.responseHeaders,
                newTransientHeaders,
                ctx.isSystemContext,
                ctx.warningHeadersSize
            );

            this.ctx = newContext;
        }

        /**
         * Puts all of the given headers into this context
         */
        @Override
        public void putHeader(String key, String value) {
            ctx.putRequest(key, value);
        }


        @Override
        public String getHeader(String key) {
            return ctx.requestHeaders.get(key);
        }

        /**
         * Puts a transient header object into this context
         */
        @Override
        public void putTransient(String key, Object value) {
            ctx.putTransient(key, value);
        }

        /**
         * Returns a transient header object or <code>null</code> if there is no header for the given key
         */
        @SuppressWarnings("unchecked") // (T)object
        public <T> T getTransient(String key) {
            return (T) ctx.transientHeaders.get(key);
        }
    }

    public static Releasable sameThreadContextSpan(ThreadPool threadPool, Tracer tracer, String name) {
        return sameThreadContextSpan(threadPool, tracer, name, Map.of());
    }

    public static Releasable sameThreadContextSpan(ThreadPool threadPool, Tracer tracer, String name, Map<String, Object> attributes) {
        if (tracer.isEnabled() == false) {
            return () -> {};
        }
        var span = Span.create();
        SubThreadContext ctx = new SubThreadContext(threadPool.getThreadContext());
        tracer.startTrace(ctx, span, name, attributes);

        return () -> {
            tracer.stopTrace(span);
        };
    }

    public static <T> void span(
        ThreadPool threadPool,
        Tracer tracer,
        String name,
        ActionListener<T> listener,
        Consumer<ActionListener<T>> action
    ) {
        var span = Span.create();
        try (var ctx = threadPool.getThreadContext().newTraceContext()) {
            var context = threadPool.getThreadContext();
            tracer.startTrace(context, span, name, Map.of());
            action.accept(
                ContextPreservingActionListener.wrapPreservingContext(
                    ActionListener.runAfter(listener, () -> tracer.stopTrace(span)),
                    context
                )
            );
        }
    }
}
