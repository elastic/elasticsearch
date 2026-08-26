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
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.index.ActionLoggingFieldsContext;
import org.elasticsearch.index.ActionLoggingFieldsProvider;
import org.elasticsearch.logging.Level;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Generic wrapper to log completion (whether successful or not) of any action, with necessary details.
 * Specific details are added in the specific context types for each action, such as search, ESQL query, etc.
 * @param <Context> Logging context type
 */
public class ActivityLogger<Context extends ActivityLoggerContext> {
    private final ActivityLogProducer<Context> producer;
    private final ActivityLogWriter writer;
    private final ActionLoggingFields additionalFields;
    protected final Consumer<Boolean> setincludeUserInformation;

    protected boolean enabled = false;
    protected long threshold = -1;
    protected Level logLevel = Level.INFO;

    public ActivityLogger(
        ActivityLogProducer<Context> producer,
        ActivityLogWriterProvider writerProvider,
        ActionLoggingFieldsProvider fieldsProvider
    ) {
        this.producer = producer;
        this.writer = writerProvider.getWriter(producer.loggerName());
        var context = new ActionLoggingFieldsContext(true);
        this.additionalFields = fieldsProvider.create(context);
        this.setincludeUserInformation = context::setIncludeUserInformation;
    }

    protected void setLogLevel(Level level) {
        if (level.equals(Level.ERROR) || level.equals(Level.FATAL)) {
            throw new IllegalStateException("Log level can not be " + level.name());
        }
        logLevel = level;
    }

    // For tests
    public Level getLogLevel() {
        return logLevel;
    }

    protected boolean shouldLog(Context context) {
        return enabled != false && (threshold <= 0 || context.getTookInNanos() >= threshold);
    }

    // Accessible for tests
    void logAction(Context context) {
        if (shouldLog(context) == false) {
            return;
        }
        Optional<ESLogMessage> event = producer.produce(context, additionalFields);
        event.ifPresent(logMessage -> addFields(context, logMessage));
        event.ifPresent(logMessage -> writer.write(logLevel, logMessage));
    }

    protected void addFields(Context context, ESLogMessage logMessage) {}

    /**
     * Wrap a listener, adding logging to either outcome of it - success or failure.
     * Both will be logged (if enabled) and then propagated to the delegate listener.
     * If logging is disabled, it's a no-op.
     */
    public <Req, R> ActionListener<R> wrap(ActionListener<R> listener, final ActivityLoggerContextBuilder<Context, Req, R> contextBuilder) {
        if (enabled == false) {
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
                return "ActivityLogger listener/" + delegate;
            }
        };
    }

    /**
     * Wrap a runnable using a listener, adding logging to either outcome of it - success or failure.
     * This is a complement to the wrap method, which is to be used when it is not guaranteed that the underlying code
     * is going to catch all exceptions and convert them to listener failures.
     * Exceptions are logged and re-thrown upstream - practically, when used in TransportAction context,
     * they will be caught by TransportAction's exception handling mechanism (e.g., RequestFilterChain.proceed).
     * If logging is disabled, it's a no-op.
     */
    public <Req, R> void wrapAndRun(
        ActionListener<R> listener,
        ActivityLoggerContextBuilder<Context, Req, R> contextBuilder,
        Consumer<ActionListener<R>> action
    ) {
        if (enabled == false) {
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

    public <Req, R> void handleException(Exception e, ActivityLoggerContextBuilder<Context, Req, R> contextBuilder) {
        if (enabled) {
            Context ctx = contextBuilder.build(e);
            logAction(ctx);
        }
    }
}
