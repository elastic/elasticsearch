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
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A future implementation that allows for the result to be passed to listeners waiting for
 * notification. This is useful for cases where a computation is requested many times
 * concurrently, but really only needs to be performed a single time. Once the computation
 * has been performed the registered listeners will be notified by submitting a runnable
 * for execution in the provided {@link ExecutorService}. If the computation has already
 * been performed, a request to add a listener will simply result in execution of the listener
 * on the calling thread.
 */
public final class ListenableFuture<V> extends BaseFuture<V> implements ActionListener<V> {

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
        try {
            // call get in a non-blocking fashion as we could be on a network thread
            // or another thread like the scheduler, which we should never block!
            assert done;
            V value = FutureUtils.get(ListenableFuture.this, 0L, TimeUnit.NANOSECONDS);
            listener.onResponse(value);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void notifyListener(ActionListener<V> listener, ExecutorService executorService) {
        try {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    notifyListenerDirectly(listener);
                }

                @Override
                public String toString() {
                    return "ListenableFuture notification";
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void onResponse(V v) {
        final boolean set = set(v);
        if (set == false) {
            throw new IllegalStateException("did not set value, value or exception already set?");
        }
    }

    @Override
    public void onFailure(Exception e) {
        final boolean set = setException(e);
        if (set == false) {
            throw new IllegalStateException("did not set exception, value already set or exception already set?");
        }
    }
}
