/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.tracing;

enum NoopSpanBuilder implements SpanBuilder {
    INSTANCE;

    @Override
    public SpanBuilder setParent(Span parentSpan) {
        return this;
    }

    @Override
    public SpanBuilder setAttribute(String key, boolean value) {
        return this;
    }

    @Override
    public SpanBuilder setAttribute(String key, double value) {
        return this;
    }

    @Override
    public SpanBuilder setAttribute(String key, long value) {
        return this;
    }

    @Override
    public SpanBuilder setAttribute(String key, String value) {
        return this;
    }

    @Override
    public Span startSpan() {
        return Span.noop();
    }
}
