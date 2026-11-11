/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;

import org.elasticsearch.telemetry.tracing.Span;

public class APMSpan extends AbstractSpan {

    private static final AttributeKey<String> EXCEPTION_TYPE = AttributeKey.stringKey("exception.type");
    private static final AttributeKey<String> EXCEPTION_MESSAGE = AttributeKey.stringKey("exception.message");

    private final io.opentelemetry.api.trace.Span otelSpan;

    APMSpan(APMTracer tracer, Context otelContext) {
        super(tracer, otelContext);
        this.otelSpan = io.opentelemetry.api.trace.Span.fromContext(otelContext);
    }

    @Override
    public boolean isRecording() {
        return otelSpan.isRecording();
    }

    @Override
    public Span setAttribute(String key, boolean value) {
        otelSpan.setAttribute(key, value);
        return this;
    }

    @Override
    public Span setAttribute(String key, double value) {
        otelSpan.setAttribute(key, value);
        return this;
    }

    @Override
    public Span setAttribute(String key, long value) {
        otelSpan.setAttribute(key, value);
        return this;
    }

    @Override
    public Span setAttribute(String key, String value) {
        if (value == null) {
            assert false : "attribute " + key + " is null";
            return this;
        }
        otelSpan.setAttribute(key, value);
        return this;
    }

    @Override
    public void setAllAttributes(Attributes attributes) {
        otelSpan.setAllAttributes(attributes);
    }

    @Override
    public Span recordException(Throwable throwable) {
        if (tracer.recordExceptionStacks()) {
            otelSpan.recordException(throwable);
        } else {
            AttributesBuilder attrs = Attributes.builder().put(EXCEPTION_TYPE, throwable.getClass().getName());
            if (throwable.getMessage() != null) {
                attrs.put(EXCEPTION_MESSAGE, throwable.getMessage());
            }
            otelSpan.addEvent("exception", attrs.build());
        }
        return this;
    }

    @Override
    public APMSpan setStatus(StatusCode statusCode, String description) {
        otelSpan.setStatus(statusCode, description);
        return this;
    }

    @Override
    public void end() {
        otelSpan.end();
    }
}
