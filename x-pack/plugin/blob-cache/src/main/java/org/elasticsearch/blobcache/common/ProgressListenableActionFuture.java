/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.LongConsumer;

/**
 * An {@link ActionFuture} that listeners can be attached to. Listeners are executed when the future is completed
 * or when a given progress is reached. Progression is updated using the {@link #onProgress(long)} method.
 *
 * Listeners are executed within the thread that triggers the completion, the failure or the progress update and
 * the progress value passed to the listeners on execution is the last updated value.
 */
class ProgressListenableActionFuture extends PlainActionFuture<Long> {

    private static final Logger logger = LogManager.getLogger(ProgressListenableActionFuture.class);

    private record PositionAndListener(long position, ActionListener<Long> listener) {}

    final long start;
    final long end;

    /**
     * A consumer that accepts progress made by this {@link ProgressListenableActionFuture}. The consumer is called before listeners are
     * notified of the updated progress value in {@link #onProgress(long)} if the value is less than the actual end. The consumer can be
     * called with out-of-order progress values.
     */
    @Nullable
    private final LongConsumer progressConsumer;

    private List<PositionAndListener> listeners;
    private long progress;
    private volatile boolean completed;

    /**
     * Creates a {@link ProgressListenableActionFuture} that accepts the progression
     * to be within {@code start} (inclusive) and {@code end} (exclusive) values.
     *
     * @param start             the start (inclusive)
     * @param end               the end (exclusive)
     * @param progressConsumer  a consumer that accepts the progress made by this {@link ProgressListenableActionFuture}
     */
    ProgressListenableActionFuture(long start, long end, @Nullable LongConsumer progressConsumer) {
        super();
        this.start = start;
        this.end = end;
        this.progress = start;
        this.completed = false;
        this.progressConsumer = progressConsumer;
        assert invariant();
    }

    private boolean invariant() {
        assert start < end : start + " < " + end;
        synchronized (this) {
            assert completed == false || listeners == null;
            assert start <= progress : start + " <= " + progress;
            assert progress <= end : progress + " <= " + end;
            assert listeners == null || listeners.stream().allMatch(listener -> progress < listener.position());
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
        if (progressValue == end) {
            return; // reached the end of the range, listeners will be completed by {@link #onResponse(Long)}
        }

        List<ActionListener<Long>> listenersToExecute = null;
        synchronized (this) {
            assert this.progress < progressValue : this.progress + " < " + progressValue;
            this.progress = progressValue;

            final List<PositionAndListener> listenersCopy = this.listeners;
            if (listenersCopy != null) {
                List<PositionAndListener> listenersToKeep = null;
                for (PositionAndListener listener : listenersCopy) {
                    if (progressValue < listener.position()) {
                        if (listenersToKeep == null) {
                            listenersToKeep = new ArrayList<>();
                        }
                        listenersToKeep.add(listener);
                    } else {
                        if (listenersToExecute == null) {
                            listenersToExecute = new ArrayList<>();
                        }
                        listenersToExecute.add(listener.listener());
                    }
                }
                this.listeners = listenersToKeep;
            }
        }
        if (listenersToExecute != null) {
            if (progressConsumer != null) {
                safeAcceptProgress(progressConsumer, progressValue);
            }
            listenersToExecute.forEach(listener -> executeListener(listener, () -> progressValue));
        }
        assert invariant();
    }

    @Override
    public void onResponse(Long result) {
        if (result == null || end != result) {
            assert false : result + " != " + end;
            throw new IllegalArgumentException("Invalid completion value [start=" + start + ",end=" + end + ",response=" + result + ']');
        }
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
        final List<PositionAndListener> listenersToExecute;
        assert invariant();
        synchronized (this) {
            assert completed == false;
            completed = true;
            assert listeners == null || listeners.stream().allMatch(l -> progress < l.position() && l.position() <= end);
            listenersToExecute = this.listeners;
            listeners = null;
        }
        if (listenersToExecute != null) {
            listenersToExecute.forEach(listener -> executeListener(listener.listener(), this::actionResult));
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
                List<PositionAndListener> listenersCopy = this.listeners;
                if (listenersCopy == null) {
                    listenersCopy = new ArrayList<>();
                }
                listenersCopy.add(new PositionAndListener(value, listener));
                this.listeners = listenersCopy;
            }
        }
        if (executeImmediate) {
            executeListener(listener, completed ? this::actionResult : () -> progressValue);
        }
        assert invariant();
    }

    /**
     * Return the result of this future, if it has been completed successfully, or unwrap and throw the exception with which it was
     * completed exceptionally. It is not valid to call this method if the future is incomplete.
     */
    private Long actionResult() throws Exception {
        try {
            return result();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception exCause) {
                throw exCause;
            } else {
                throw e;
            }
        }
    }

    private static void executeListener(final ActionListener<Long> listener, final CheckedSupplier<Long, ?> result) {
        try {
            listener.onResponse(result.get());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static void safeAcceptProgress(LongConsumer consumer, long progress) {
        assert consumer != null;
        try {
            consumer.accept(progress);
        } catch (Exception e) {
            assert false : e;
            logger.warn("Failed to consume progress value", e);
        }
    }

    @Override
    public synchronized String toString() {
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
