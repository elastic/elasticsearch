/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.ImplicitContextKeyed;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/** Counter shared by all spans under the same local root to enforce {@link OtelSdkSettings#TELEMETRY_TRACING_MAX_SPANS}. */
public final class LocalSpanCount implements ImplicitContextKeyed {

    private static final ContextKey<LocalSpanCount> SPAN_LOCAL_COUNT_KEY = ContextKey.named("es.apm.span.local_count");

    private static final AtomicIntegerFieldUpdater<LocalSpanCount> COUNT = AtomicIntegerFieldUpdater.newUpdater(
        LocalSpanCount.class,
        "count"
    );

    @SuppressWarnings("unused")
    private volatile int count;

    public int incrementAndGet() {
        return COUNT.incrementAndGet(this);
    }

    public void decrement() {
        COUNT.decrementAndGet(this);
    }

    @Nullable
    public static LocalSpanCount getOrNull(Context context) {
        return context.get(SPAN_LOCAL_COUNT_KEY);
    }

    @Override
    public Context storeInContext(Context context) {
        return context.with(SPAN_LOCAL_COUNT_KEY, this);
    }
}
