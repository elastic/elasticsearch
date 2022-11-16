/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@code Future} and {@link ActionListener} against which other {@link ActionListener}s can be registered later, to support
 * fanning-out a result to a dynamic collection of listeners.
 */
public class ListenableActionFuture<T> extends AdapterActionFuture<T, T> {

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

    @Override
    protected T convert(T listenerResponse) {
        return listenerResponse;
    }

    private void executeListener(final ActionListener<T> listener) {
        try {
            // we use a timeout of 0 to by pass assertion forbidding to call actionGet() (blocking) on a network thread.
            // here we know we will never block
            listener.onResponse(actionGet(0));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
