/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 *
 * Wraps an async action that consumes an {@link ActionListener} such that multiple invocations of {@link #execute(ActionListener)} can
 * share the result from a single call to the wrapped action. This implementation is similar to {@link ResultDeduplicator} but offers
 * stronger guarantees of not seeing a stale result ever. Concretely, every invocation of {@link #execute(ActionListener)} is guaranteed to
 * be resolved with a response that has been computed at a time after the call to {@code execute} has been made. This allows this class to
 * be used to deduplicate results from actions that produce results that change over time transparently.
 *
 * @param <T> Result type
 */
public final class SingleResultDeduplicator<T> {

    private final ThreadContext threadContext;

    /**
     * List of listeners waiting for the execution after the current in-progress execution. If {@code null} then no execution is in
     * progress currently, otherwise an execution is in progress and will trigger another execution that will resolve any listeners queued
     * up here once done.
     */
    private List<ActionListener<T>> waitingListeners;

    private final Consumer<ActionListener<T>> executeAction;

    public SingleResultDeduplicator(ThreadContext threadContext, Consumer<ActionListener<T>> executeAction) {
        this.threadContext = threadContext;
        this.executeAction = executeAction;
    }

    /**
     * Execute the action for the given {@code listener}.
     * @param listener listener to resolve with execution result
     */
    public void execute(ActionListener<T> listener) {
        synchronized (this) {
            if (waitingListeners == null) {
                // no queued up listeners, just execute this one directly without deduplication and instantiate the list so that
                // subsequent executions will wait
                waitingListeners = new ArrayList<>();
            } else {
                // already running an execution, queue this one up
                waitingListeners.add(ContextPreservingActionListener.wrapPreservingContext(listener, threadContext));
                return;
            }
        }
        doExecute(listener);
    }

    private void doExecute(ActionListener<T> listener) {
        final ActionListener<T> wrappedListener = ActionListener.runBefore(listener, () -> {
            final List<ActionListener<T>> listeners;
            synchronized (this) {
                if (waitingListeners.isEmpty()) {
                    // no listeners were queued up while this execution ran, so we just reset the state to not having a running execution
                    waitingListeners = null;
                    return;
                } else {
                    // we have queued up listeners, so we create a fresh list for the next execution and execute once to handle the
                    // listeners currently queued up
                    listeners = waitingListeners;
                    waitingListeners = new ArrayList<>();
                }
            }
            doExecute(new ActionListener<T>() {
                @Override
                public void onResponse(T response) {
                    ActionListener.onResponse(listeners, response);
                }

                @Override
                public void onFailure(Exception e) {
                    ActionListener.onFailure(listeners, e);
                }
            });
        });
        try {
            executeAction.accept(wrappedListener);
        } catch (Exception e) {
            wrappedListener.onFailure(e);
        }
    }
}
