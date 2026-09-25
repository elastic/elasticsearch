/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.tracing.TraceContext;

import java.util.function.UnaryOperator;

/**
 * Encapsulates getting and updating the OTel {@link Context} from the {@link ThreadContext}, where it's stored as
 * {@value Task#APM_TRACE_CONTEXT}.
 */
public final class OtelContext {

    private OtelContext() {}

    /**
     * Attempts to retrieve value from the OTel contexts first from the current context ({@link Task#APM_TRACE_CONTEXT}), then the parent
     * context ({@link Task#PARENT_APM_TRACE_CONTEXT}) if it's missing in current. That's because every time
     * {@link ThreadContext#newTraceContext()} is called, it moves the current OTel context to the parent key in the transient headers map,
     * effectively dropping the current context and breaking any standard OTel context propagation mechanisms. To go around that we have to
     * try both contexts here.
     */
    @Nullable
    public static <T> T getValueOrNullFromContext(TraceContext threadContext, ContextKey<T> key) {
        T result = getValueOrNull(threadContext.getTransient(Task.APM_TRACE_CONTEXT), key);
        if (result == null) {
            result = getValueOrNull(threadContext.getTransient(Task.PARENT_APM_TRACE_CONTEXT), key);
        }
        return result;
    }

    @Nullable
    private static <T> T getValueOrNull(@Nullable Context contextOrNull, ContextKey<T> key) {
        return contextOrNull == null ? null : contextOrNull.get(key);
    }

    /** Updates the current OTel context ({@link Task#APM_TRACE_CONTEXT}) in {@link ThreadContext} and returns the updated value. */
    public static Context updateAndGet(TraceContext threadContext, UnaryOperator<Context> update) {
        Context previousContextOrNull = threadContext.getTransient(Task.APM_TRACE_CONTEXT);
        Context updated = update.apply(previousContextOrNull == null ? Context.root() : previousContextOrNull);
        threadContext.putTransientAllowOverwrite(Task.APM_TRACE_CONTEXT, updated);
        return updated;
    }
}
