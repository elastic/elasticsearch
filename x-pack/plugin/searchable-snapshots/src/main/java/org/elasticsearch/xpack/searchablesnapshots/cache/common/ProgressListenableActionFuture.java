/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.common;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * An {@link ActionFuture} that listeners can be attached to. Listeners are executed when the future is completed
 * or when a given progress is reached. Progression is updated using the {@link #onProgress(long)} method.
 *
 * Listeners are executed within the thread that triggers the completion, the failure or the progress update and
 * the progress value passed to the listeners on execution is the last updated value.
 */
class ProgressListenableActionFuture extends AdapterActionFuture<Long, Long> {

    protected final long start;
    protected final long end;

    // modified under 'this' mutex
    private volatile List<Tuple<Long, ActionListener<Long>>> listeners;
    protected volatile long progress;
    private volatile boolean completed;

    /**
     * Creates a {@link ProgressListenableActionFuture} that accepts the progression
     * to be within {@code start} (inclusive) and {@code end} (exclusive) values.
     *
     * @param start the start (inclusive)
     * @param end   the end (exclusive)
     */
    ProgressListenableActionFuture(long start, long end) {
        super();
        this.start = start;
        this.end = end;
        this.progress = start;
        this.completed = false;
        assert invariant();
    }

    private boolean invariant() {
        assert start < end : start + " < " + end;
        synchronized (this) {
            assert completed == false || listeners == null;
            assert start <= progress : start + " <= " + progress;
            assert progress <= end : progress + " <= " + end;
            assert listeners == null || listeners.stream().allMatch(listener -> progress < listener.v1());
        }
        return true;
    }

    /**
     * Updates the progress of the current {@link ActionFuture} with the given value, indicating that the range from {@code start}
     * (inclusive) to {@code progress} (exclusive) is available. Calling this method potentially triggers the execution of one or
     * more listeners that are waiting for the progress to reach a value lower than the one just updated.
     *
     * @param progressValue the new progress value
     */
    public void onProgress(final long progressValue) {
        ensureNotCompleted();

        if (progressValue <= start) {
            assert false : progressValue + " <= " + start;
            throw new IllegalArgumentException("Cannot update progress with a value less than [start=" + start + ']');
        }
        if (end < progressValue) {
            assert false : end + " < " + progressValue;
            throw new IllegalArgumentException("Cannot update progress with a value greater than [end=" + end + ']');
        }

        List<ActionListener<Long>> listenersToExecute = null;
        synchronized (this) {
            assert this.progress < progressValue : this.progress + " < " + progressValue;
            this.progress = progressValue;

            final List<Tuple<Long, ActionListener<Long>>> listenersCopy = this.listeners;
            if (listenersCopy != null) {
                List<Tuple<Long, ActionListener<Long>>> listenersToKeep = null;
                for (Tuple<Long, ActionListener<Long>> listener : listenersCopy) {
                    if (progressValue < listener.v1()) {
                        if (listenersToKeep == null) {
                            listenersToKeep = new ArrayList<>();
                        }
                        listenersToKeep.add(listener);
                    } else {
                        if (listenersToExecute == null) {
                            listenersToExecute = new ArrayList<>();
                        }
                        listenersToExecute.add(listener.v2());
                    }
                }
                this.listeners = listenersToKeep;
            }
        }
        if (listenersToExecute != null) {
            listenersToExecute.forEach(listener -> executeListener(listener, () -> progressValue));
        }
        assert invariant();
    }

    @Override
    public void onResponse(Long result) {
        ensureNotCompleted();
        super.onResponse(result);
    }

    @Override
    public void onFailure(Exception e) {
        ensureNotCompleted();
        super.onFailure(e);
    }

    private void ensureNotCompleted() {
        if (completed) {
            throw new IllegalStateException("Future is already completed");
        }
    }

    @Override
    protected void done(boolean success) {
        super.done(success);
        final List<Tuple<Long, ActionListener<Long>>> listenersToExecute;
        synchronized (this) {
            assert progress == end || success == false;
            completed = true;
            listenersToExecute = this.listeners;
            listeners = null;
        }
        if (listenersToExecute != null) {
            listenersToExecute.stream().map(Tuple::v2).forEach(listener -> executeListener(listener, () -> actionGet(0L)));
        }
        assert invariant();
    }

    /**
     * Attach a {@link ActionListener} to the current future. The listener will be executed once the future is completed or once the
     * progress reaches the given {@code value}, whichever comes first.
     *
     * @param listener the {@link ActionListener} to add
     * @param value    the value
     */
    public void addListener(ActionListener<Long> listener, long value) {
        boolean executeImmediate = false;
        final long progressValue;
        synchronized (this) {
            progressValue = this.progress;
            if (completed || value <= progressValue) {
                executeImmediate = true;
            } else {
                List<Tuple<Long, ActionListener<Long>>> listenersCopy = this.listeners;
                if (listenersCopy == null) {
                    listenersCopy = new ArrayList<>();
                }
                listenersCopy.add(Tuple.tuple(value, listener));
                this.listeners = listenersCopy;
            }
        }
        if (executeImmediate) {
            executeListener(listener, completed ? () -> actionGet(0L) : () -> progressValue);
        }
        assert invariant();
    }

    private static void executeListener(final ActionListener<Long> listener, final Supplier<Long> result) {
        try {
            listener.onResponse(result.get());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected Long convert(Long response) {
        if (response == null || response < start || end < response) {
            assert false : start + " < " + response + " < " + end;
            throw new IllegalArgumentException("Invalid completion value [start=" + start + ",end=" + end + ",response=" + response + ']');
        }
        return response;
    }

    @Override
    public String toString() {
        return "ProgressListenableActionFuture[start="
            + start
            + ", end="
            + end
            + ", progress="
            + progress
            + ", completed="
            + completed
            + ", listeners="
            + (listeners != null ? listeners.size() : 0)
            + ']';
    }
}
