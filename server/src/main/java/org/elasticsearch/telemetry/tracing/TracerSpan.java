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

    public static void span(ThreadPool threadPool, Tracer tracer, String name, Runnable action) {
        var span = Span.create();
        try (var ctx = threadPool.getThreadContext().newTraceContext()) {
            tracer.startTrace(threadPool.getThreadContext(), span, name, Map.of());
            action.run();
        } finally {
            tracer.stopTrace(span);
        }
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
