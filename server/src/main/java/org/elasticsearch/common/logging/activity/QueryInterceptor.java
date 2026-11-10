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
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Queries are intercepted by wrapping the {@code ActionListener} to capture the query response.
 * {@code QueryCaptureContext} binds the request and response and is passed to the downstream {@link QuerySink}s for processing.
 *
 * Interception should be lean and efficient and an error in interception must not fail the intercepted query. {@link QuerySink}s are
 * notified synchronously by default. The overload accepting {@link QueryCaptureService} queues sink processing on a worker
 * and retains the response until that processing completes.
 *
 * @param <Context> Logging context type, specific context types exist for each action, such as search, ESQL query, etc.
 */
public final class QueryInterceptor<Context extends QueryCaptureContext> {
    private static final Logger logger = LogManager.getLogger(QueryInterceptor.class);

    private final List<QuerySink<? super Context>> sinks;

    public QueryInterceptor(List<? extends QuerySink<? super Context>> sinks) {
        this.sinks = List.copyOf(sinks);
    }

    private boolean anySinkEnabled() {
        return sinks.stream().anyMatch(QueryInterceptor::isEnabled);
    }

    /** A sink whose switch fails is treated as disabled: a sink must never fail the intercepted query. */
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
                    // a sink must never fail the query or starve the other sinks
                    logger.warn(() -> Strings.format("query sink [%s] failed", sink), e);
                }
            }
        }
    }

    /**
     * Build the context and hand it to the sinks. Capturing is best effort: a failure to build the context is logged and
     * swallowed so that it can never change the outcome of the query.
     */
    private void capture(Supplier<Context> contextSupplier, Consumer<Context> consumer) {
        final Context context;
        try {
            context = contextSupplier.get();
            consumer.accept(context);
        } catch (Exception e) {
            logger.warn("failed to capture the query", e);
        }
    }

    <Req, R> ActionListener<R> wrap(ActionListener<R> listener, final QueryCaptureContextBuilder<Context, Req, R> contextBuilder) {
        return wrap(listener, contextBuilder, (supplier, response) -> capture(supplier, this::notifySinks));
    }

    private <Req, R> ActionListener<R> wrap(
        ActionListener<R> listener,
        QueryCaptureContextBuilder<Context, Req, R> contextBuilder,
        BiConsumer<Supplier<Context>, R> capture
    ) {
        return new DelegatingActionListener<>(listener) {
            @Override
            public void onResponse(R r) {
                capture.accept(() -> contextBuilder.build(r), r);
                delegate.onResponse(r);
            }

            @Override
            public void onFailure(Exception e) {
                capture.accept(() -> contextBuilder.build(e), null);
                super.onFailure(e);
            }

            @Override
            public String toString() {
                return "QueryInterceptor listener/" + delegate;
            }
        };
    }

    /**
     * Wrap the listener to capture the query response and execute the {@code action}.
     * Sink processing is
     * If no sink is enabled the action is run without any wrapping
     */
    public <Req, R> void wrapAndRun(
        ActionListener<R> listener,
        QueryCaptureContextBuilder<Context, Req, R> contextBuilder,
        Consumer<ActionListener<R>> action
    ) {
        internalWrapAndRun(listener, contextBuilder, action, (supplier, response) -> capture(supplier, this::notifySinks));
    }

    /**
     * Wrap the listener, execute the {@code action} and hand off to {@code captureService} to
     * process the response.
     * Responses are retained while queued or executing.
     */
    public <Req, R extends RefCounted> void wrapAndRun(
        ActionListener<R> listener,
        QueryCaptureContextBuilder<Context, Req, R> contextBuilder,
        Consumer<ActionListener<R>> action,
        QueryCaptureService captureService
    ) {
        internalWrapAndRun(
            listener,
            contextBuilder,
            action,
            (supplier, response) -> capture(supplier, context -> captureService.execute(() -> notifySinks(context), response))
        );
    }

    private <Req, R> void internalWrapAndRun(
        ActionListener<R> listener,
        QueryCaptureContextBuilder<Context, Req, R> contextBuilder,
        Consumer<ActionListener<R>> action,
        BiConsumer<Supplier<Context>, R> capture
    ) {
        if (anySinkEnabled() == false) {
            action.accept(listener);
            return;
        }
        try {
            action.accept(wrap(listener, contextBuilder, capture));
        } catch (Exception e) {
            capture.accept(() -> contextBuilder.build(e), null);
            throw e;
        }
    }
}
