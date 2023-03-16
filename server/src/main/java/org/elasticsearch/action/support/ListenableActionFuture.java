/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

/**
 * An {@link ActionListener} which allows for the result to fan out to a (dynamic) collection of other listeners, added using {@link
 * #addListener}. Listeners added before completion are retained until completion; listeners added after completion are completed
 * immediately.
 *
 * Similar to {@link ListenableFuture}, with the following differences:
 *
 * <ul>
 * <li>This listener will silently ignore additional completions, whereas {@link ListenableFuture} must not be completed more than once.
 * <li>This listener completes the retained listeners on directly the completing thread, so you must use {@link ThreadedActionListener} if
 * dispatching is needed. In contrast, {@link ListenableFuture} allows to dispatch only the retained listeners, while immediately-completed
 * listeners are completed on the subscribing thread.
 * <li>This listener completes the retained listeners in the context of the completing thread, so you must remember to use {@link
 * ContextPreservingActionListener} to capture the thread context yourself if needed. In contrast, {@link ListenableFuture} allows for the
 * thread context to be captured at subscription time.
 * </ul>
 */
// The name {@link ListenableActionFuture} dates back a long way and could be improved - TODO find a better name
public class ListenableActionFuture<T> extends PlainActionFuture<T> {

    private Object listeners;
    private boolean executedListeners = false;

    /**
     * Registers an {@link ActionListener} to be notified when this future is completed. If the future is already completed then the
     * listener is notified immediately, on the calling thread. If not, the listener is notified on the thread that completes the listener.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void addListener(final ActionListener<T> listener) {
        final boolean executeImmediate;
        synchronized (this) {
            executeImmediate = executedListeners;
            if (executeImmediate == false) {
                final Object oldListeners = listeners;
                final Object newListeners;
                if (oldListeners == null) {
                    // adding the first listener
                    newListeners = listener;
                } else if (oldListeners instanceof List) {
                    // adding a listener after the second
                    ((List) oldListeners).add(listener);
                    newListeners = oldListeners;
                } else {
                    // adding the second listener
                    newListeners = new ArrayList<>(2);
                    ((List) newListeners).add(oldListeners);
                    ((List) newListeners).add(listener);
                }
                this.listeners = newListeners;
            }
        }
        if (executeImmediate) {
            executeListener(listener);
        }
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void done(boolean success) {
        super.done(success);
        final Object listenersToExecute;
        synchronized (this) {
            executedListeners = true;
            listenersToExecute = listeners;
            listeners = null;
        }

        if (listenersToExecute != null) {
            if (listenersToExecute instanceof List) {
                for (final Object listener : (List) listenersToExecute) {
                    executeListener((ActionListener<T>) listener);
                }
            } else {
                executeListener((ActionListener<T>) listenersToExecute);
            }
        }
    }

    private void executeListener(final ActionListener<T> listener) {
        // call non-blocking actionResult() as we could be on a network or scheduler thread which we must not block
        ActionListener.completeWith(listener, this::actionResult);
    }

}
