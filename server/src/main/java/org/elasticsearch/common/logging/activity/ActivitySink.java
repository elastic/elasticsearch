/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A downstream consumer of the completed context of an action handled by an {@link ActivityInterceptor}. The query log is one sink,
 * a plugin that samples queries can be another. Each sink has its own switch, so any of them can be enabled or disabled
 * independently.
 * <p>
 * A sink is a producer and a writer that only have to agree with each other on the message type {@code M}: the producer
 * turns the context into a message, or decides there is nothing to do by returning empty, and the writer puts the message
 * wherever it belongs. The message type is private to the sink, so it does not have to be a public class.
 * <p>
 * Sinks run synchronously on the thread that completes the action, before the response is passed on. They must be fast,
 * must copy anything they need from the context (the response may be released once the call returns), and an exception they
 * throw is swallowed by the {@link ActivityInterceptor} so that it can never fail the action or starve the other sinks.
 *
 * @param <C> the logging context type this sink handles
 * @param <M> the message passed from the producer to the writer
 */
public final class ActivitySink<C extends ActivityCaptureContext, M> {
    private final Supplier<String> description;
    private final BooleanSupplier enabled;
    private final Function<C, Optional<M>> producer;
    private final Consumer<M> writer;

    /**
     * @param enabled  whether the sink currently wants to be called. This is read for every action, so it must be cheap,
     *                 typically a volatile read. While no sink is enabled the action is not wrapped at all.
     * @param producer turns the context into a message, or returns empty to skip it
     * @param writer   receives the messages of the producer
     * @param description describes the sink in {@link #toString()}, which is only used when reporting a failure of the sink, so it
     *                 is evaluated lazily
     */
    public ActivitySink(BooleanSupplier enabled, Function<C, Optional<M>> producer, Consumer<M> writer, Supplier<String> description) {
        this.enabled = enabled;
        this.producer = producer;
        this.writer = writer;
        this.description = description;
    }

    public boolean isEnabled() {
        return enabled.getAsBoolean();
    }

    /**
     * Called once when the action completes, successfully or not.
     */
    public void accept(C context) {
        producer.apply(context).ifPresent(writer);
    }

    @Override
    public String toString() {
        return description.get();
    }
}
