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
 * Builds and starts a {@link Span}.
 * <p>
 * Builders are not thread-safe and each of them can be used to start at most one span.
 */
public interface SpanBuilder {

    /** Returns a span builder implementation that does nothing. */
    static SpanBuilder noop() {
        return NoopSpanBuilder.INSTANCE;
    }

    /**
     * Manually set the parent of the span that will be started. If not called, the span's parent will be automatically inherited from the
     * current {@link ThreadContext}. Pass {@code Span.noop()} to start a completely new trace manually.
     *
     * @param parentSpan the parent of the span that will be started
     * @return {@code this}
     */
    SpanBuilder setParent(Span parentSpan);

    /**
     * Sets an attribute on the span that will be started. Setting a key that is already set replaces its value.
     *
     * <p>Attributes that are known before the span starts should be set here rather than on the {@link Span} itself: this allows them to be
     * used for sampling.
     *
     * @param key   the attribute key: a stable, low-cardinality identifier
     * @param value the attribute value
     * @return {@code this}
     */
    SpanBuilder setAttribute(String key, boolean value);

    /**
     * Sets an attribute on the span that will be started. Setting a key that is already set replaces its value.
     *
     * <p>Attributes that are known before the span starts should be set here rather than on the {@link Span} itself: this allows them to be
     * used for sampling.
     *
     * @param key   the attribute key: a stable, low-cardinality identifier
     * @param value the attribute value
     * @return {@code this}
     */
    SpanBuilder setAttribute(String key, double value);

    /**
     * Sets an attribute on the span that will be started. Setting a key that is already set replaces its value.
     *
     * <p>Attributes that are known before the span starts should be set here rather than on the {@link Span} itself: this allows them to be
     * used for sampling.
     *
     * @param key   the attribute key: a stable, low-cardinality identifier
     * @param value the attribute value
     * @return {@code this}
     */
    SpanBuilder setAttribute(String key, long value);

    /**
     * Sets an attribute on the span that will be started. Setting a key that is already set replaces its value.
     *
     * <p>Attributes that are known before the span starts should be set here rather than on the {@link Span} itself: this allows them to be
     * used for sampling.
     *
     * @param key   the attribute key: a stable, low-cardinality identifier
     * @param value the attribute value
     * @return {@code this}
     */
    SpanBuilder setAttribute(String key, String value);

    /**
     * Starts the span. Users <b>must</b> manually call {@code span.end()} to end the span.
     *
     * <p>Note that this does not modify the current {@link ThreadContext}; that's what {@link Span#storeInThreadContext()} is for.
     *
     * @return a started span
     */
    Span startSpan();
}
