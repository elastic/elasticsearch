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
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.SpanStatusBuilder;

import org.elasticsearch.telemetry.tracing.Span;

/** A span that does not record anything, but has a context that is propagated like the one of any other span. */
public class PropagatedSpan extends AbstractSpan {

    PropagatedSpan(APMTracer tracer, Context otelContext) {
        super(tracer, otelContext);
    }

    /** Nothing is recorded by this span, even if the span that its context holds is still recording, e.g. as tracing got disabled. */
    @Override
    public boolean isRecording() {
        return false;
    }

    @Override
    public void setAllAttributes(Attributes attributes) {}

    @Override
    public SpanStatusBuilder setStatus(StatusCode statusCode, String description) {
        return this;
    }

    @Override
    public Span setAttribute(String key, boolean value) {
        return this;
    }

    @Override
    public Span setAttribute(String key, double value) {
        return this;
    }

    @Override
    public Span setAttribute(String key, long value) {
        return this;
    }

    @Override
    public Span setAttribute(String key, String value) {
        return this;
    }

    @Override
    public Span recordException(Throwable throwable) {
        return this;
    }

    @Override
    public void end() {}
}
