/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.tracing;

import org.elasticsearch.common.util.concurrent.ThreadContext;

/**
 * Entry point of the tracing API, used to create {@link Span}s and to interact with tracing-related parts of thread context. Spans are
 * created through a {@link SpanBuilder} obtained from {@link #spanBuilder(String)}. The span that is currently active can be looked up with
 * {@link #currentSpan()}.
 */
public interface Tracer {

    /** Returns a tracer implementation that does nothing. */
    static Tracer noop() {
        return NoopTracer.INSTANCE;
    }

    /**
     * Initiates the process of creating a new span via the returned {@link SpanBuilder}.
     *
     * @param name the name of the span. It should be a low-cardinality description of the operation.
     * @return a {@link SpanBuilder} used to create and start a new span.
     */
    SpanBuilder spanBuilder(String name);

    /**
     * Returns the span that is current in the calling thread's {@link ThreadContext}, or a no-op span if there is none.
     *
     * @return the current span stored in the {@link ThreadContext}.
     */
    Span currentSpan();
}
