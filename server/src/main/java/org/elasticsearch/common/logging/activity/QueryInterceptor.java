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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Captures a query action - its completion, whether successful or not - and hands it to the {@link QuerySink}s.
 * The context is built once and passed to every enabled sink. The interceptor neither logs nor knows what the sinks do: the
 * query log ({@link QueryLogger}) is one sink among others. Capturing is best effort: a failure to build the context or in a
 * sink is logged and never changes the outcome of the action.
 * Specific details are added in the specific context types for each action, such as search, ESQL query, etc.
 * @param <Context> Logging context type
 */
// TODO: move these classes out of org.elasticsearch.common.logging.activity. Capturing queries for sinks is no longer logging.
public final class QueryInterceptor<Context extends QueryCaptureContext> {
    private static final Logger logger = LogManager.getLogger(QueryInterceptor.class);

    private final List<QuerySink<Context>> sinks;

    public QueryInterceptor(List<QuerySink<Context>> sinks) {
        this.sinks = List.copyOf(sinks);
    }

    private boolean anySinkEnabled() {
        return sinks.stream().anyMatch(QueryInterceptor::isEnabled);
    }

    /** A sink whose switch fails is treated as disabled: a sink must never fail the action. */
    private static boolean isEnabled(QuerySink<?> sink) {
        try {
            return sink.isEnabled();
        } catch (Exception e) {
            logger.warn(() -> Strings.format("query sink [%s] failed to report whether it is enabled", sink), e);
            return false;
        }
    }

    // Accessible for tests
    void notifySinks(Context context) {
        for (var sink : sinks) {
            if (isEnabled(sink)) {
                try {
                    sink.accept(context);
                } catch (Exception e) {
                    // a sink must never fail the action or starve the other sinks
                    logger.warn(() -> Strings.format("query sink [%s] failed", sink), e);
                }
            }
        }
    }

    /**
     * Build the context and hand it to the sinks. Capturing is best effort: a failure to build the context is logged and
     * swallowed so that it can never change the outcome of the action.
     */
    private void capture(Supplier<Context> contextSupplier) {
        final Context context;
        try {
            context = contextSupplier.get();
        } catch (Exception e) {
            logger.warn("failed to build the query context, the query is not captured", e);
            return;
        }
        notifySinks(context);
    }

    /**
     * Wrap a listener, capturing either outcome of it - success or failure.
     * Both will be passed to the enabled sinks and then propagated to the delegate listener.
     * If no sink is enabled, it's a no-op.
     */
    public <Req, R> ActionListener<R> wrap(ActionListener<R> listener, final QueryCaptureContextBuilder<Context, Req, R> contextBuilder) {
        if (anySinkEnabled() == false) {
            return listener;
        }
        return new DelegatingActionListener<>(listener) {
            @Override
            public void onResponse(R r) {
                capture(() -> contextBuilder.build(r));
                delegate.onResponse(r);
            }

            @Override
            public void onFailure(Exception e) {
                capture(() -> contextBuilder.build(e));
                super.onFailure(e);
            }

            @Override
            public String toString() {
                return "QueryInterceptor listener/" + delegate;
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
        QueryCaptureContextBuilder<Context, Req, R> contextBuilder,
        Consumer<ActionListener<R>> action
    ) {
        if (anySinkEnabled() == false) {
            action.accept(listener);
            return;
        }
        try {
            action.accept(wrap(listener, contextBuilder));
        } catch (Exception e) {
            capture(() -> contextBuilder.build(e));
            throw e;
        }
    }
}
