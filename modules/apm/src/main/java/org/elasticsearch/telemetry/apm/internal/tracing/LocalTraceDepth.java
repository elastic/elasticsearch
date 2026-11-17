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

import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;

/** Tracks per-span local depth in Context to enforce {@link OtelSdkSettings#TELEMETRY_TRACING_MAX_DEPTH}. */
public record LocalTraceDepth(int depth) implements ImplicitContextKeyed {

    private static final ContextKey<LocalTraceDepth> SPAN_LOCAL_DEPTH_KEY = ContextKey.named("es.apm.span.local_depth");

    public LocalTraceDepth increment() {
        return new LocalTraceDepth(depth + 1);
    }

    public static LocalTraceDepth get(Context context) {
        var result = context.get(SPAN_LOCAL_DEPTH_KEY);
        return result == null ? new LocalTraceDepth(0) : result;
    }

    @Override
    public Context storeInContext(Context context) {
        return context.with(SPAN_LOCAL_DEPTH_KEY, this);
    }
}
