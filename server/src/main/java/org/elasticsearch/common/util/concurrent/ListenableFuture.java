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
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
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
    private List<Tuple<ActionListener<V>, ExecutorService>> listeners;

    /**
     * Adds a listener to this future. If the future has not yet completed, the listener will be
     * notified of a response or exception on the thread completing this future.
     * If the future has completed, the listener will be notified immediately without forking to
     * a different thread.
     */
    public void addListener(ActionListener<V> listener) {
        addListener(listener, EsExecutors.DIRECT_EXECUTOR_SERVICE, null);
    }

    /**
     * Adds a listener to this future. If the future has not yet completed, the listener will be
     * notified of a response or exception in a runnable submitted to the ExecutorService provided.
     * If the future has completed, the listener will be notified immediately without forking to
     * a different thread.
     *
     * It will apply the provided ThreadContext (if not null) when executing the listening.
     */
    public void addListener(ActionListener<V> listener, ExecutorService executor, ThreadContext threadContext) {
        if (done) {
            // run the callback directly, we don't hold the lock and don't need to fork!
            notifyListenerDirectly(listener);
        } else {
            final boolean run;
            // check done under lock since it could have been modified and protect modifications
            // to the list under lock
            synchronized (this) {
                if (done) {
                    run = true;
                } else {
                    final ActionListener<V> wrappedListener;
                    if (threadContext == null) {
                        wrappedListener = listener;
                    } else {
                        wrappedListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
                    }
                    if (listeners == null) {
                        listeners = new ArrayList<>();
                    }
                    listeners.add(new Tuple<>(wrappedListener, executor));
                    run = false;
                }
            }

            if (run) {
                // run the callback directly, we don't hold the lock and don't need to fork!
                notifyListenerDirectly(listener);
            }
        }
    }

    @Override
    protected void done(boolean ignored) {
        final List<Tuple<ActionListener<V>, ExecutorService>> existingListeners;
        synchronized (this) {
            done = true;
            existingListeners = listeners;
            if (existingListeners == null) {
                return;
            }
            listeners = null;
        }
        for (Tuple<ActionListener<V>, ExecutorService> t : existingListeners) {
            final ExecutorService executorService = t.v2();
            final ActionListener<V> listener = t.v1();
            if (executorService == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
                notifyListenerDirectly(listener);
            } else {
                notifyListener(listener, executorService);
            }
        }
    }

    private void notifyListenerDirectly(ActionListener<V> listener) {
        // call get in a non-blocking fashion as we could be on a network thread
        // or another thread like the scheduler, which we should never block!
        assert done;
        ActionListener.completeWith(listener, () -> FutureUtils.get(ListenableFuture.this, 0L, TimeUnit.NANOSECONDS));
    }

    private void notifyListener(ActionListener<V> listener, ExecutorService executorService) {
        ActionListener.run(listener, l -> executorService.execute(new Runnable() {
            @Override
            public void run() {
                notifyListenerDirectly(l);
            }

            @Override
            public String toString() {
                return "ListenableFuture notification";
            }
        }));
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
