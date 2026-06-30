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
import org.elasticsearch.action.support.RefCountingListener;
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

    record PositionAndListener(long position, ActionListener<Long> listener) {}

    final long start;
    final long end;

    /**
     * Called on every {@link #onProgress} update, unconditionally. Used by {@link SparseFileTracker#splitRange}
     * to forward A's byte-level progress to the original completion listener so that its registered listeners
     * fire promptly rather than waiting for the whole half to complete.
     */
    @Nullable
    private final LongConsumer unconditionalProgressConsumer;

    /**
     * Called on {@link #onProgress} only when at least one registered listener fires at that update. Used to
     * advance the {@link SparseFileTracker#complete} pointer lazily (only when someone is actually waiting).
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
        this(start, end, null, progressConsumer);
    }

    /**
     * Creates a {@link ProgressListenableActionFuture} with both an unconditional and a conditional consumer.
     * Private: only called from {@link #split}.
     *
     * @param unconditionalProgressConsumer called on every progress update; may be {@code null}
     * @param progressConsumer              called only when listeners execute; may be {@code null}
     */
    private ProgressListenableActionFuture(
        long start,
        long end,
        @Nullable LongConsumer unconditionalProgressConsumer,
        @Nullable LongConsumer progressConsumer
    ) {
        super();
        this.start = start;
        this.end = end;
        this.progress = start;
        this.completed = false;
        this.unconditionalProgressConsumer = unconditionalProgressConsumer;
        this.progressConsumer = progressConsumer;
        assert invariant();
    }

    /**
     * Splits this future at {@code splitPoint}, returning {@code [lowerFuture, upperFuture]} that together drive
     * this future to completion. Listeners registered on this future receive timely progress notifications:
     * <ul>
     *   <li>Lower's byte-level progress is forwarded unconditionally to this future.</li>
     *   <li>Upper's progress is forwarded once lower has completed.</li>
     *   <li>When lower completes, this future advances to upper's current progress (at least {@code splitPoint})
     *       via {@link #onProgressAtLeast}, catching up to any progress upper made while lower was pending.</li>
     *   <li>When both halves complete, this future completes; on failure the failure propagates.</li>
     * </ul>
     */
    ProgressListenableActionFuture[] split(long splitPoint) {
        assert start < splitPoint : start + " >= " + splitPoint;
        assert splitPoint < end : splitPoint + " >= " + end;

        final LongConsumer originalProgressConsumer = this.progressConsumer;
        final ProgressListenableActionFuture lower = new ProgressListenableActionFuture(
            start,
            splitPoint,
            this::onProgress,
            originalProgressConsumer
        );
        final ProgressListenableActionFuture upper = new ProgressListenableActionFuture(
            splitPoint,
            end,
            p -> { if (lower.isDone()) onProgress(p); },
            originalProgressConsumer == null ? null : p -> {
                if (lower.isDone()) originalProgressConsumer.accept(p);
            }
        );

        // When lower completes we catch up to wherever upper has already progressed, not just splitPoint.
        // upper.progress is always < upper.end (onProgress(end) returns early without updating the field),
        // so passing it to onProgressAtLeast is always within the valid [start+1, end-1] range.
        lower.addListener(ActionListener.wrap(ignored -> onProgressAtLeast(upper.getProgress()), e -> {}), splitPoint);
        try (var bothFiredRef = new RefCountingListener(ActionListener.wrap(v -> onResponse(end), this::onFailure))) {
            lower.addListener(bothFiredRef.acquire(l -> {}), splitPoint);
            upper.addListener(bothFiredRef.acquire(l -> {}), end);
        }

        return new ProgressListenableActionFuture[] { lower, upper };
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
     * (inclusive) to {@code progressValue} (exclusive) is available. Fires any listeners whose threshold has been reached.
     * Calling with {@code progressValue == end} is a no-op; reaching {@code end} is signalled via {@link #onResponse(Long)}.
     *
     * @param progressValue the new progress value; must be strictly greater than the current progress
     */
    public void onProgress(final long progressValue) {
        doOnProgress(progressValue, true);
    }

    /**
     * Like {@link #onProgress(long)} but a no-op if progress has already advanced to or past {@code progressValue}.
     * Unlike {@link #onProgress}, this method is safe to call concurrently with other progress updates that may
     * have already advanced past the given value — it simply returns without asserting ordering.
     */
    void onProgressAtLeast(final long progressValue) {
        assert progressValue < end;
        doOnProgress(progressValue, false);
    }

    /**
     * Shared implementation for {@link #onProgress} and {@link #onProgressAtLeast}.
     *
     * @param strict if {@code true} the current progress must be strictly less than {@code progressValue} (normal
     *               {@link #onProgress} contract); if {@code false} the method is a no-op when progress has already
     *               advanced to or past {@code progressValue}, without asserting ordering.
     */
    private void doOnProgress(final long progressValue, final boolean strict) {
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
            if (strict) {
                assert this.progress < progressValue : this.progress + " < " + progressValue;
            } else if (this.progress >= progressValue) {
                return;
            }
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
        if (unconditionalProgressConsumer != null) {
            safeAcceptProgress(unconditionalProgressConsumer, progressValue);
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

    private synchronized long getProgress() {
        return progress;
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
