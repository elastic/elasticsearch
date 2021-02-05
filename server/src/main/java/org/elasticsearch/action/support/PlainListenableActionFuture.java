/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;

import java.util.ArrayList;
import java.util.List;

public class PlainListenableActionFuture<T> extends AdapterActionFuture<T, T> implements ListenableActionFuture<T> {

    volatile Object listeners;
    boolean executedListeners = false;

    protected PlainListenableActionFuture() {}

    /**
     * This method returns a listenable future. The listeners will be called on completion of the future.
     * The listeners will be executed by the same thread that completes the future.
     *
     * @param <T> the result of the future
     * @return a listenable future
     */
    public static <T> PlainListenableActionFuture<T> newListenableFuture() {
        return new PlainListenableActionFuture<>();
    }

    @Override
    public void addListener(final ActionListener<T> listener) {
        internalAddListener(listener);
    }

    @Override
    protected void done(boolean success) {
        super.done(success);
        synchronized (this) {
            executedListeners = true;
        }
        Object listeners = this.listeners;
        if (listeners != null) {
            if (listeners instanceof List) {
                List list = (List) listeners;
                for (Object listener : list) {
                    executeListener((ActionListener<T>) listener);
                }
            } else {
                executeListener((ActionListener<T>) listeners);
            }
        }
    }

    @Override
    protected T convert(T listenerResponse) {
        return listenerResponse;
    }

    private void internalAddListener(ActionListener<T> listener) {
        boolean executeImmediate = false;
        synchronized (this) {
            if (executedListeners) {
                executeImmediate = true;
            } else {
                Object listeners = this.listeners;
                if (listeners == null) {
                    listeners = listener;
                } else if (listeners instanceof List) {
                    ((List) this.listeners).add(listener);
                } else {
                    Object orig = listeners;
                    listeners = new ArrayList<>(2);
                    ((List) listeners).add(orig);
                    ((List) listeners).add(listener);
                }
                this.listeners = listeners;
            }
        }
        if (executeImmediate) {
            executeListener(listener);
        }
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
