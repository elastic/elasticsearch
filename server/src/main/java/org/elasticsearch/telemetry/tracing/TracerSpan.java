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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.threadpool.ThreadPool;

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
