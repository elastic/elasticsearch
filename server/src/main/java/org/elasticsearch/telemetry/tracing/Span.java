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
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;

/** Represents a span, a single unit of work, a part of a trace. */
public interface Span {

    /** Returns a span builder implementation that does nothing. */
    static Span noop() {
        return NoopSpan.INSTANCE;
    }

    /**
     * Sets an attribute on this span that will be started. Setting a key that is already set replaces its value.
     *
     * @param key   the attribute key: a stable, low-cardinality identifier
     * @param value the attribute value
     * @return {@code this}
     */
    Span setAttribute(String key, boolean value);

    /**
     * Sets an attribute on this span that will be started. Setting a key that is already set replaces its value.
     *
     * @param key   the attribute key: a stable, low-cardinality identifier
     * @param value the attribute value
     * @return {@code this}
     */
    Span setAttribute(String key, double value);

    /**
     * Sets an attribute on this span that will be started. Setting a key that is already set replaces its value.
     *
     * @param key   the attribute key: a stable, low-cardinality identifier
     * @param value the attribute value
     * @return {@code this}
     */
    Span setAttribute(String key, long value);

    /**
     * Sets an attribute on this span that will be started. Setting a key that is already set replaces its value.
     *
     * @param key   the attribute key: a stable, low-cardinality identifier
     * @param value the attribute value
     * @return {@code this}
     */
    Span setAttribute(String key, String value);

    /**
     * Records an exception that occurred during the span. It is not a terminal operation, and may be called several times.
     *
     * @return {@code this}
     */
    Span recordException(Throwable throwable);

    /**
     * Ends the span, marking it as ready to be exported. Ending a span that has already ended has no effect.
     */
    void end();

    /**
     * Makes this span the current span of the calling thread's {@link ThreadContext}: it will be available through
     * {@link Tracer#currentSpan()}, and will automatically become the parent span (unless manually overridden) of any further spans started
     * while the current thread context is active.
     *
     * <p>The resulting {@link StoredContext} must be closed on the thread that called this method, for example with try-with-resources.
     * Closing it does not end the span.
     *
     * @return the stored context that restores the thread context to what it was before this method was called
     */
    StoredContext storeInThreadContext();
}
