/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common;

import org.elasticsearch.action.ActionListener;

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * This action listener wraps another listener and provides a framework for iteration over a List while calling an asynchronous function
 * for each. The listener calls the {@link BiConsumer} with the current element in the list and a {@link ActionListener}. This function
 * is expected to call the listener in case of success or failure due to an exception. If there is a failure due to an exception the wrapped
 * listener's {@link ActionListener#onFailure(Exception)} method is called. If the consumer calls {@link #onResponse(Object)} with a
 * non-null object, iteration will cease and the wrapped listener will be called with the response. In the case of a null value being passed
 * to {@link #onResponse(Object)} then iteration will continue by applying the {@link BiConsumer} to the next item in the list; if the list
 * has no more elements then the wrapped listener will be called with a null value.
 *
 * After creation, iteration is started by calling {@link #run()}
 */
public final class IteratingActionListener<T, U> implements ActionListener<T>, Runnable {

    private final List<U> consumables;
    private final ActionListener<T> delegate;
    private final BiConsumer<U, ActionListener<T>> consumer;

    private int position = 0;

    public IteratingActionListener(ActionListener<T> delegate, BiConsumer<U, ActionListener<T>> consumer, List<U> consumables) {
        this.delegate = delegate;
        this.consumer = consumer;
        this.consumables = Collections.unmodifiableList(consumables);
    }

    @Override
    public void run() {
        if (consumables.isEmpty()) {
            onResponse(null);
        } else if (position < 0 || position >= consumables.size()) {
            onFailure(new IllegalStateException("invalid position [" + position + "]. List size [" + consumables.size() + "]"));
        } else {
            consumer.accept(consumables.get(position++), this);
        }
    }

    @Override
    public void onResponse(T response) {
        if (response == null) {
            if (position == consumables.size()) {
                delegate.onResponse(null);
            } else {
                consumer.accept(consumables.get(position++), this);
            }
        } else {
            delegate.onResponse(response);
        }
    }

    @Override
    public void onFailure(Exception e) {
        delegate.onFailure(e);
    }
}
