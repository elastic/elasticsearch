/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Static utility for fanning out asynchronous work over a collection and gathering the results in
 * iteration order — the order-preserving counterpart to {@link GroupedActionListener}, which collects
 * results in completion order. Use this when the consumer of the result performs an order-sensitive
 * reduction (e.g. a non-commutative fold).
 */
public final class OrderedGroupedActionListener {

    private OrderedGroupedActionListener() {}

    /**
     * Asynchronously applies {@code task} to each element of {@code items} and invokes {@code delegate}
     * exactly once with the results in {@code items}'s iteration order, regardless of the order in which
     * the per-item tasks complete.
     *
     * <p>Failure semantics: the first failure is propagated to {@code delegate} with subsequent failures
     * attached via {@link Exception#addSuppressed}; the delegate is invoked once, after every per-item
     * task has completed (success or failure).
     *
     * <p>If {@code items} is empty, {@code delegate} is invoked immediately with an empty list.
     *
     * @param items    the elements to process; iteration order determines result order
     * @param task     starts the asynchronous work for one element and completes the supplied
     *                 {@link ActionListener} when done
     * @param delegate receives the ordered results (or the first failure)
     */
    public static <T, R> void forEach(Collection<T> items, BiConsumer<T, ActionListener<R>> task, ActionListener<List<R>> delegate) {
        if (items.isEmpty()) {
            delegate.onResponse(List.of());
            return;
        }
        final int size = items.size();
        final AtomicArray<R> results = new AtomicArray<>(size);
        final CountDown countDown = new CountDown(size);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        int idx = 0;
        for (T item : items) {
            final int slot = idx++;
            task.accept(item, new ActionListener<>() {
                @Override
                public void onResponse(R value) {
                    results.setOnce(slot, value);
                    if (countDown.countDown()) {
                        finish(results, failure, delegate);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    final Exception previous = failure.compareAndExchange(null, e);
                    if (previous != null && previous != e) {
                        previous.addSuppressed(e);
                    }
                    if (countDown.countDown()) {
                        finish(results, failure, delegate);
                    }
                }
            });
        }
    }

    private static <R> void finish(AtomicArray<R> results, AtomicReference<Exception> failure, ActionListener<List<R>> delegate) {
        final Exception e = failure.get();
        if (e != null) {
            delegate.onFailure(e);
        } else {
            delegate.onResponse(results.asList());
        }
    }
}
