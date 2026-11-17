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

enum NoopSpan implements Span {
    INSTANCE;

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

    @Override
    public ThreadContext.StoredContext storeInThreadContext() {
        return () -> {};
    }
}
