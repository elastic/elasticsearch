/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Captures the activity of an action - its completion, whether successful or not - and hands it to the {@link ActivitySink}s.
 * The context is built once and passed to every enabled sink. The interceptor neither logs nor knows what the sinks do: the
 * query log ({@link QueryLogger}) is one sink among others.
 * Specific details are added in the specific context types for each action, such as search, ESQL query, etc.
 * @param <Context> Logging context type
 */
public final class ActivityInterceptor<Context extends ActivityCaptureContext> {
    private static final Logger logger = LogManager.getLogger(ActivityInterceptor.class);

    private final List<ActivitySink<Context, ?>> sinks;

    public ActivityInterceptor(List<ActivitySink<Context, ?>> sinks) {
        this.sinks = List.copyOf(sinks);
    }

    /**
     * An interceptor for the given sink followed by more sinks, such as the ones contributed by plugins.
     */
    public static <Context extends ActivityCaptureContext> ActivityInterceptor<Context> of(
        ActivitySink<Context, ?> sink,
        List<ActivitySink<Context, ?>> moreSinks
    ) {
        List<ActivitySink<Context, ?>> sinks = new ArrayList<>(moreSinks.size() + 1);
        sinks.add(sink);
        sinks.addAll(moreSinks);
        return new ActivityInterceptor<>(sinks);
    }

    private boolean anySinkEnabled() {
        for (var sink : sinks) {
            if (sink.isEnabled()) {
                return true;
            }
        }
        return false;
    }

    // Accessible for tests
    void logAction(Context context) {
        for (var sink : sinks) {
            if (sink.isEnabled()) {
                try {
                    sink.accept(context);
                } catch (Exception e) {
                    // a sink must never fail the action or starve the other sinks
                    logger.debug(() -> Strings.format("activity sink [%s] failed", sink), e);
                }
            }
        }
    }

    /**
     * Wrap a listener, capturing either outcome of it - success or failure.
     * Both will be passed to the enabled sinks and then propagated to the delegate listener.
     * If no sink is enabled, it's a no-op.
     */
    public <Req, R> ActionListener<R> wrap(
        ActionListener<R> listener,
        final ActivityCaptureContextBuilder<Context, Req, R> contextBuilder
    ) {
        if (anySinkEnabled() == false) {
            return listener;
        }
        return new DelegatingActionListener<>(listener) {
            @Override
            public void onResponse(R r) {
                log(r);
                delegate.onResponse(r);
            }

            @Override
            public void onFailure(Exception e) {
                log(e);
                super.onFailure(e);
            }

            private void log(R r) {
                Context ctx = contextBuilder.build(r);
                logAction(ctx);
            }

            private void log(Exception e) {
                Context ctx = contextBuilder.build(e);
                logAction(ctx);
            }

            @Override
            public String toString() {
                return "ActivityInterceptor listener/" + delegate;
            }
        };
    }

    /**
     * Wrap a runnable using a listener, capturing either outcome of it - success or failure.
     * This is a complement to the wrap method, which is to be used when it is not guaranteed that the underlying code
     * is going to catch all exceptions and convert them to listener failures.
     * Exceptions are passed to the sinks and re-thrown upstream - practically, when used in TransportAction context,
     * they will be caught by TransportAction's exception handling mechanism (e.g., RequestFilterChain.proceed).
     * If no sink is enabled, it's a no-op.
     */
    public <Req, R> void wrapAndRun(
        ActionListener<R> listener,
        ActivityCaptureContextBuilder<Context, Req, R> contextBuilder,
        Consumer<ActionListener<R>> action
    ) {
        if (anySinkEnabled() == false) {
            action.accept(listener);
            return;
        }
        try {
            action.accept(wrap(listener, contextBuilder));
        } catch (Exception e) {
            handleException(e, contextBuilder);
            throw e;
        }
    }

    public <Req, R> void handleException(Exception e, ActivityCaptureContextBuilder<Context, Req, R> contextBuilder) {
        if (anySinkEnabled()) {
            Context ctx = contextBuilder.build(e);
            logAction(ctx);
        }
    }
}
