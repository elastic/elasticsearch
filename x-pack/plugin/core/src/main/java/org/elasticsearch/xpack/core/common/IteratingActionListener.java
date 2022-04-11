/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This action listener wraps another listener and provides a framework for iteration over a List while calling an asynchronous function
 * for each. The listener calls the {@link BiConsumer} with the current element in the list and a {@link ActionListener}. This function
 * is expected to call the listener in case of success or failure due to an exception. If there is a failure due to an exception the wrapped
 * listener's {@link ActionListener#onFailure(Exception)} method is called. If the consumer calls {@link #onResponse(Object)} with a
 * non-null object, iteration will cease and the wrapped listener will be called with the response. In the case of a null value being passed
 * to {@link #onResponse(Object)} then iteration will continue by applying the {@link BiConsumer} to the next item in the list; if the list
 * has no more elements then the wrapped listener will be called with a null value, unless an optional {@link Supplier} is provided
 * that supplies the response to send for {@link ActionListener#onResponse(Object)}.
 *
 * After creation, iteration is started by calling {@link #run()}
 */
public final class IteratingActionListener<T, U> implements ActionListener<T>, Runnable {

    private final List<U> consumables;
    private final ActionListener<T> delegate;
    private final BiConsumer<U, ActionListener<T>> consumer;
    private final ThreadContext threadContext;
    private final Function<T, T> finalResultFunction;
    private final Predicate<T> iterationPredicate;

    private int position = 0;

    /**
     * Constructs an {@link IteratingActionListener}.
     *
     * @param delegate the delegate listener to call when all consumables have finished executing
     * @param consumer the consumer that is executed for each consumable instance
     * @param consumables the instances that can be consumed to produce a response which is ultimately sent on the delegate listener
     * @param threadContext the thread context for the thread pool that created the listener
     */
    public IteratingActionListener(
        ActionListener<T> delegate,
        BiConsumer<U, ActionListener<T>> consumer,
        List<U> consumables,
        ThreadContext threadContext
    ) {
        this(delegate, consumer, consumables, threadContext, Function.identity());
    }

    /**
     * Constructs an {@link IteratingActionListener}.
     *
     * @param delegate the delegate listener to call when all consumables have finished executing
     * @param consumer the consumer that is executed for each consumable instance
     * @param consumables the instances that can be consumed to produce a response which is ultimately sent on the delegate listener
     * @param threadContext the thread context for the thread pool that created the listener
     * @param finalResultFunction a function that maps the response which terminated iteration to a response that will be sent to the
     *                            delegate listener. This is useful if the delegate listener should receive some other value (perhaps
     *                            a concatenation of the results of all the called consumables).
     */
    public IteratingActionListener(
        ActionListener<T> delegate,
        BiConsumer<U, ActionListener<T>> consumer,
        List<U> consumables,
        ThreadContext threadContext,
        Function<T, T> finalResultFunction
    ) {
        this(delegate, consumer, consumables, threadContext, finalResultFunction, Objects::isNull);
    }

    /**
     * Constructs an {@link IteratingActionListener}.
     *
     * @param delegate the delegate listener to call when all consumables have finished executing
     * @param consumer the consumer that is executed for each consumable instance
     * @param consumables the instances that can be consumed to produce a response which is ultimately sent on the delegate listener
     * @param threadContext the thread context for the thread pool that created the listener
     * @param finalResultFunction a function that maps the response which terminated iteration to a response that will be sent to the
     *                            delegate listener. This is useful if the delegate listener should receive some other value (perhaps
     *                            a concatenation of the results of all the called consumables).
     * @param iterationPredicate a {@link Predicate} that checks if iteration should continue based on the returned result
     */
    public IteratingActionListener(
        ActionListener<T> delegate,
        BiConsumer<U, ActionListener<T>> consumer,
        List<U> consumables,
        ThreadContext threadContext,
        Function<T, T> finalResultFunction,
        Predicate<T> iterationPredicate
    ) {
        this.delegate = delegate;
        this.consumer = consumer;
        this.consumables = Collections.unmodifiableList(consumables);
        this.threadContext = threadContext;
        this.finalResultFunction = finalResultFunction;
        this.iterationPredicate = iterationPredicate;
    }

    @Override
    public void run() {
        if (consumables.isEmpty()) {
            onResponse(null);
        } else if (position < 0 || position >= consumables.size()) {
            onFailure(new IllegalStateException("invalid position [" + position + "]. List size [" + consumables.size() + "]"));
        } else {
            try (ThreadContext.StoredContext ignore = threadContext.newStoredContext(false)) {
                consumer.accept(consumables.get(position++), this);
            } catch (Exception e) {
                onFailure(e);
            }
        }
    }

    @Override
    public void onResponse(T response) {
        // we need to store the context here as there is a chance that this method is called from a thread outside of the ThreadPool
        // like a LDAP connection reader thread and we can pollute the context in certain cases
        try (ThreadContext.StoredContext ignore = threadContext.newStoredContext(false)) {
            final boolean continueIteration = iterationPredicate.test(response);
            if (continueIteration) {
                if (position == consumables.size()) {
                    delegate.onResponse(finalResultFunction.apply(response));
                } else {
                    try {
                        consumer.accept(consumables.get(position++), this);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }
            } else {
                delegate.onResponse(finalResultFunction.apply(response));
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        // we need to store the context here as there is a chance that this method is called from a thread outside of the ThreadPool
        // like a LDAP connection reader thread and we can pollute the context in certain cases
        try (ThreadContext.StoredContext ignore = threadContext.newStoredContext(false)) {
            delegate.onFailure(e);
        }
    }
}
