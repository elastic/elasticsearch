/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import java.util.function.Supplier;

/**
 * A downstream consumer of the completed context of a query action handled by a {@link QueryInterceptor}. The query log is one sink,
 * a plugin that samples queries can be another. Each sink has its own switch, so any of them can be enabled or disabled
 * independently.
 * <p>
 * Implementations decide what to do with the context in {@link #accept}: for example turn it into a message and write it,
 * or skip it. Any message type is private to the implementation.
 * <p>
 * Sinks run synchronously on the thread that completes the action, before the response is passed on. They must be fast,
 * must copy anything they need from the context (the response may be released once the call returns), and an exception they
 * throw is swallowed by the {@link QueryInterceptor} so that it can never fail the action or starve the other sinks.
 *
 * @param <C> the query context type this sink handles
 */
public abstract class QuerySink<C extends QueryCaptureContext> {
    private final Supplier<String> description;

    /**
     * @param description describes the sink in {@link #toString()}, which is only used when reporting a failure of the sink, so it
     *                 is evaluated lazily
     */
    protected QuerySink(Supplier<String> description) {
        this.description = description;
    }

    /**
     * Whether the sink currently wants to be called. This is read for every action, so it must be cheap, typically a volatile
     * read. While no sink is enabled the action is not wrapped at all. Implementations should not throw: a sink whose switch
     * throws is treated as disabled.
     * @return true if enabled.
     */
    public abstract boolean isEnabled();

    /**
     * Called once when the action completes, successfully or not, if the sink is enabled.
     */
    public abstract void accept(C context);

    @Override
    public String toString() {
        return description.get();
    }
}
