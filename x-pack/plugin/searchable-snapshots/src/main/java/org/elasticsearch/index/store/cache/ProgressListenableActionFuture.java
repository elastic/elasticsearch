/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.store.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.List;

public class ProgressListenableActionFuture<T> extends AdapterActionFuture<T, T> implements ListenableActionFuture<T> {

    private final long start;
    private final long end;

    // modified under 'this' mutex
    private volatile List<Tuple<Long, ActionListener<T>>> listeners;
    private boolean executedListeners;
    private long current;

    private ProgressListenableActionFuture(final long start, final long end) {
        super();
        assert start < end : start + '-' + end;
        this.executedListeners = false;
        this.start = start;
        this.end = end;
        this.current = start;
    }

    public static <T> ProgressListenableActionFuture<T> newProgressListenableActionFuture(long start, long end) {
        return new ProgressListenableActionFuture<>(start, end);
    }

    public void onProgress(final long progress) {
        assert executedListeners == false;
        assert start <= progress : start + "<=" + progress;
        assert progress <= end : progress + "<=" + end;

        List<ActionListener<T>> listenersToExecute = null;
        synchronized (this) {
            current = progress;

            final List<Tuple<Long, ActionListener<T>>> listeners = this.listeners;
            if (listeners == null || listeners.isEmpty()) {
                return;
            }

            List<Tuple<Long, ActionListener<T>>> listenersToKeep = null;
            for (Tuple<Long, ActionListener<T>> listener : listeners) {
                if (listener.v1() <= current) {
                    if (listenersToExecute == null) {
                        listenersToExecute = new ArrayList<>();
                    }
                    listenersToExecute.add(listener.v2());
                } else {
                    if (listenersToKeep == null) {
                        listenersToKeep = new ArrayList<>();
                    }
                    listenersToKeep.add(listener);
                }
            }
            this.listeners = listenersToKeep;
            assert this.listeners == null || this.listeners.stream().allMatch(listener -> this.current < listener.v1());
        }

        if (listenersToExecute != null) {
            listenersToExecute.forEach(this::executeListener);
        }
    }

    @Override
    protected void done() {
        super.done();
        List<Tuple<Long, ActionListener<T>>> listenersToExecute = null;
        synchronized (this) {
            executedListeners = true;
            listenersToExecute = this.listeners;
            this.listeners = null;
        }
        if (listenersToExecute != null) {
            listenersToExecute.stream().map(Tuple::v2).forEach(this::executeListener);
        }
    }

    @Override
    public void addListener(final ActionListener<T> listener) {
        addListener(end, listener);
    }

    public void addListener(long position, ActionListener<T> listener) {
        boolean executeImmediate = false;
        synchronized (this) {
            if (executedListeners) {
                executeImmediate = true;
            } else if (position <= current) {
                executeImmediate = true;
            } else {
                List<Tuple<Long, ActionListener<T>>> listeners = this.listeners;
                if (listeners == null) {
                    listeners = new ArrayList<>();
                }
                listeners.add(Tuple.tuple(position, listener));
                this.listeners = listeners;
            }
        }
        if (executeImmediate) {
            executeListener(listener);
        }
    }

    private void executeListener(final ActionListener<T> listener) {
        try {
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected T convert(T listenerResponse) {
        return listenerResponse;
    }
}
