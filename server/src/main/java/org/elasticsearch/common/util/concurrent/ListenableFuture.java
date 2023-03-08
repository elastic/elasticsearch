/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * An {@link ActionListener} which allows for the result to fan out to a (dynamic) collection of other listeners, added using {@link
 * #addListener}. Listeners added before completion are retained until completion; listeners added after completion are completed
 * immediately.
 *
 * Similar to {@link ListenableActionFuture}, with the following differences:
 *
 * <ul>
 * <li>This listener must not be completed more than once, whereas {@link ListenableActionFuture} will silently ignore additional
 * completions.
 * <li>This listener optionally allows for the completions of any retained listeners to be dispatched to an executor rather than handled
 * directly by the completing thread, whilst still allowing listeners to be completed immediately on the subscribing thread. In contrast,
 * when using {@link ListenableActionFuture} you must use {@link ThreadedActionListener} if dispatching is needed, and this will always
 * dispatch.
 * <li>This listener optionally allows for the retained listeners to be completed in the thread context of the subscribing thread, captured
 * at subscription time. This is often useful when subscribing listeners from several different contexts. In contrast, when using {@link
 * ListenableActionFuture} you must remember to use {@link ContextPreservingActionListener} to capture the thread context yourself.
 * </ul>
 */
// The name {@link ListenableFuture} dates back a long way and could be improved - TODO find a better name
public final class ListenableFuture<V> extends PlainActionFuture<V> {

    private volatile boolean done = false;
    private List<ActionListener<V>> listeners;

    /**
     * Adds a listener to this future. If the future has not yet completed, the listener will be notified of a response or exception on the
     * thread completing this future. If the future has completed, the listener will be notified immediately without forking to a different
     * thread.
     */
    public void addListener(ActionListener<V> listener) {
        addListener(listener, EsExecutors.DIRECT_EXECUTOR_SERVICE, null);
    }

    /**
     * Adds a listener to this future. If the future has not yet completed, the listener will be notified of a response or exception in a
     * runnable submitted to the {@link Executor} provided. If the future has completed, the listener will be notified immediately without
     * forking to a different thread.
     *
     * It will restore the provided {@link ThreadContext} (if not null) when completing the listener.
     */
    public void addListener(ActionListener<V> listener, Executor executor, @Nullable ThreadContext threadContext) {
        if (done || addListenerIfIncomplete(listener, executor, threadContext) == false) {
            // run the callback directly, we don't hold the lock and don't need to fork!
            notifyListener(listener);
        }
    }

    @Override
    protected void done(boolean ignored) {
        final var existingListeners = acquireExistingListeners();
        if (existingListeners != null) {
            for (final var listener : existingListeners) {
                notifyListener(listener);
            }
        }
    }

    private synchronized boolean addListenerIfIncomplete(ActionListener<V> listener, Executor executor, ThreadContext threadContext) {
        // check done under lock since it could have been modified; also protect modifications to the list under lock
        if (done) {
            return false;
        }
        if (threadContext != null) {
            listener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
        }
        if (executor != EsExecutors.DIRECT_EXECUTOR_SERVICE) {
            listener = new ThreadedActionListener<>(executor, listener);
        }
        if (listeners == null) {
            listeners = new ArrayList<>();
        }
        listeners.add(listener);
        return true;
    }

    private synchronized List<ActionListener<V>> acquireExistingListeners() {
        try {
            done = true;
            return listeners;
        } finally {
            listeners = null;
        }
    }

    private void notifyListener(ActionListener<V> listener) {
        assert done;
        // call get() in a non-blocking fashion as we could be on a network or scheduler thread which we must not block
        ActionListener.completeWith(listener, () -> FutureUtils.get(ListenableFuture.this, 0L, TimeUnit.NANOSECONDS));
    }

    @Override
    public void onResponse(V v) {
        final boolean set = set(v);
        if (set == false) {
            assert false;
            throw new IllegalStateException("did not set value, value or exception already set?");
        }
    }

    @Override
    public void onFailure(Exception e) {
        final boolean set = setException(e);
        if (set == false) {
            assert false;
            throw new IllegalStateException("did not set exception, value already set or exception already set?");
        }
    }
}
