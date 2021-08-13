/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

/**
 * A utility that allows chained (serial) execution of a number of tasks
 * in async manner.
 */
public class TypedChainTaskExecutor<T> {

    public interface ChainTask <T> {
        void run(ActionListener<T> listener);
    }

    private final ExecutorService executorService;
    private final LinkedList<ChainTask<T>> tasks = new LinkedList<>();
    private final Predicate<Exception> failureShortCircuitPredicate;
    private final Predicate<T> continuationPredicate;
    private final List<T> collectedResponses;

    /**
     * Creates a new TypedChainTaskExecutor.
     * Each chainedTask is executed in order serially and after each execution the continuationPredicate is tested.
     *
     * On failures the failureShortCircuitPredicate is tested.
     *
     * @param executorService The service where to execute the tasks
     * @param continuationPredicate The predicate to test on whether to execute the next task or not.
     *                              {@code true} means continue on to the next task.
     *                              Must be able to handle null values.
     * @param failureShortCircuitPredicate The predicate on whether to short circuit execution on a give exception.
     *                                     {@code true} means that no more tasks should execute and the listener::onFailure should be
     *                                     called.
     */
    public TypedChainTaskExecutor(ExecutorService executorService,
                                  Predicate<T> continuationPredicate,
                                  Predicate<Exception> failureShortCircuitPredicate) {
        this.executorService = Objects.requireNonNull(executorService);
        this.continuationPredicate = continuationPredicate;
        this.failureShortCircuitPredicate = failureShortCircuitPredicate;
        this.collectedResponses = new ArrayList<>();
    }

    public synchronized void add(ChainTask<T> task) {
        tasks.add(task);
    }

    private synchronized void execute(T previousValue, ActionListener<List<T>> listener) {
        collectedResponses.add(previousValue);
        if (continuationPredicate.test(previousValue)) {
            if (tasks.isEmpty()) {
                // noinspection Java9CollectionFactory (because the list can contain null entries)
                listener.onResponse(Collections.unmodifiableList(new ArrayList<>(collectedResponses)));
               return;
            }
            ChainTask<T> task = tasks.pop();
            executorService.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    if (failureShortCircuitPredicate.test(e)) {
                        listener.onFailure(e);
                    } else {
                        execute(null, listener);
                    }
                }

                @Override
                protected void doRun() {
                    task.run(ActionListener.wrap(value -> execute(value, listener), this::onFailure));
                }
            });
        } else {
            // noinspection Java9CollectionFactory (because the list can contain null entries)
            listener.onResponse(Collections.unmodifiableList(new ArrayList<>(collectedResponses)));
        }
    }

    /**
     * Execute all the chained tasks serially, notify listener when completed
     *
     * @param listener The ActionListener to notify when all executions have been completed,
     *                 or when no further tasks should be executed.
     *                 The resulting list COULD contain null values depending on if execution is continued
     *                 on exceptions or not.
     */
    public synchronized void execute(ActionListener<List<T>> listener) {
        if (tasks.isEmpty()) {
            listener.onResponse(Collections.emptyList());
            return;
        }
        collectedResponses.clear();
        ChainTask<T> task = tasks.pop();
        executorService.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (failureShortCircuitPredicate.test(e)) {
                    listener.onFailure(e);
                } else {
                    execute(null, listener);
                }
            }

            @Override
            protected void doRun() {
                task.run(ActionListener.wrap(value -> execute(value, listener), this::onFailure));
            }
        });
    }

    public synchronized List<T> getCollectedResponses() {
        return List.copyOf(collectedResponses);
    }
}
