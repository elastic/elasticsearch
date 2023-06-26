/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Allows for the registration of listeners that are called when a change becomes visible for search. This functionality is exposed from
 * {@link IndexShard} but kept here so it can be tested without standing up the entire thing.
 *
 * When {@link Closeable#close()}d it will no longer accept listeners and flush any existing listeners.
 */
public final class RefreshListeners implements ReferenceManager.RefreshListener, Closeable {
    private final IntSupplier getMaxRefreshListeners;
    private final Runnable forceRefresh;
    private final Logger logger;
    private final ThreadContext threadContext;
    private final MeanMetric refreshMetric;

    /**
     * Time in nanosecond when beforeRefresh() is called. Used for calculating refresh metrics.
     */
    private long currentRefreshStartTime;

    /**
     * Is this closed? If true then we won't add more listeners and have flushed all pending listeners.
     */
    private volatile boolean closed = false;

    /**
     * Force-refreshes new refresh listeners that are added while {@code >= 0}. Used to prevent becoming blocked on operations waiting for
     * refresh during relocation.
     */
    private int refreshForcers;

    /**
     * List of refresh listeners. Defaults to null and built on demand because most refresh cycles won't need it. Entries are never removed
     * from it, rather, it is nulled and rebuilt when needed again. The (hopefully) rare entries that didn't make the current refresh cycle
     * are just added back to the new list. Both the reference and the contents are always modified while synchronized on {@code this}.
     *
     * We never set this to non-null while closed it {@code true}.
     */
    private volatile List<Tuple<Translog.Location, Consumer<Boolean>>> locationRefreshListeners = null;
    private volatile List<Tuple<Long, ActionListener<Void>>> checkpointRefreshListeners = null;

    /**
     * The translog location that was last made visible by a refresh.
     */
    private volatile Translog.Location lastRefreshedLocation;

    private volatile long lastRefreshedCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;

    public RefreshListeners(
        final IntSupplier getMaxRefreshListeners,
        final Runnable forceRefresh,
        final Logger logger,
        final ThreadContext threadContext,
        final MeanMetric refreshMetric
    ) {
        this.getMaxRefreshListeners = getMaxRefreshListeners;
        this.forceRefresh = forceRefresh;
        this.logger = logger;
        this.threadContext = threadContext;
        this.refreshMetric = refreshMetric;
    }

    /**
     * Force-refreshes newly added listeners and forces a refresh if there are currently listeners registered. See {@link #refreshForcers}.
     */
    public Releasable forceRefreshes() {
        synchronized (this) {
            assert refreshForcers >= 0;
            refreshForcers += 1;
        }
        final Releasable releaseOnce = Releasables.releaseOnce(() -> {
            synchronized (RefreshListeners.this) {
                assert refreshForcers > 0;
                refreshForcers -= 1;
            }
        });
        if (refreshNeeded()) {
            try {
                forceRefresh.run();
            } catch (Exception e) {
                releaseOnce.close();
                throw e;
            }
        }
        assert locationRefreshListeners == null;
        assert checkpointRefreshListeners == null;
        return releaseOnce;
    }

    /**
     * Add a listener for refreshes, calling it immediately if the location is already visible. If this runs out of listener slots then it
     * forces a refresh and calls the listener immediately as well.
     *
     * @param location the location to listen for
     * @param listener for the refresh. Called with true if registering the listener ran it out of slots and forced a refresh. Called with
     *        false otherwise.
     * @return did we call the listener (true) or register the listener to call later (false)?
     */
    public boolean addOrNotify(Translog.Location location, Consumer<Boolean> listener) {
        requireNonNull(listener, "listener cannot be null");
        requireNonNull(location, "location cannot be null");

        if (lastRefreshedLocation != null && lastRefreshedLocation.compareTo(location) >= 0) {
            // Location already visible, just call the listener
            listener.accept(false);
            return true;
        }
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("can't wait for refresh on a closed index");
            }
            List<Tuple<Translog.Location, Consumer<Boolean>>> listeners = locationRefreshListeners;
            final int maxRefreshes = getMaxRefreshListeners.getAsInt();
            if (refreshForcers == 0 && roomForListener(maxRefreshes, listeners, checkpointRefreshListeners)) {
                ThreadContext.StoredContext storedContext = threadContext.newStoredContextPreservingResponseHeaders();
                Consumer<Boolean> contextPreservingListener = forced -> {
                    try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                        storedContext.restore();
                        listener.accept(forced);
                    }
                };
                if (listeners == null) {
                    listeners = new ArrayList<>();
                }
                // We have a free slot so register the listener
                listeners.add(new Tuple<>(location, contextPreservingListener));
                locationRefreshListeners = listeners;
                return false;
            }
        }
        // No free slot so force a refresh and call the listener in this thread
        forceRefresh.run();
        listener.accept(true);
        return true;
    }

    /**
     * Add a listener for refreshes, calling it immediately if the location is already visible. If this runs out of listener slots then it
     * fails the listener immediately. The checkpoint cannot be greater than the processed local checkpoint if
     * {@code allowUnIssuedSequenceNumber} is {@code false}. This method does not respect the forceRefreshes state. It will NEVER force a
     * refresh on the calling thread. Instead, it will simply add listeners or rejected them if too many listeners are already waiting.
     *
     * @param checkpoint the seqNo checkpoint to listen for
     * @param allowUnIssuedSequenceNumber if true allow listening for checkpoint newer than the processed local checkpoint
     * @param listener for the refresh.
     * @return did we call the listener (true) or register the listener to call later (false)?
     */
    public boolean addOrNotify(long checkpoint, boolean allowUnIssuedSequenceNumber, ActionListener<Void> listener) {
        assert checkpoint >= SequenceNumbers.NO_OPS_PERFORMED;
        if (checkpoint <= lastRefreshedCheckpoint) {
            listener.onResponse(null);
            return true;
        }
        if (allowUnIssuedSequenceNumber == false) {
            long maxIssuedSequenceNumber = maxIssuedSeqNoSupplier.getAsLong();
            if (checkpoint > maxIssuedSequenceNumber) {
                IllegalArgumentException e = new IllegalArgumentException(
                    "Cannot wait for unissued seqNo checkpoint [wait_for_checkpoint="
                        + checkpoint
                        + ", max_issued_seqNo="
                        + maxIssuedSequenceNumber
                        + "]"
                );
                listener.onFailure(e);
                return true;
            }
        }

        synchronized (this) {
            if (closed) {
                listener.onFailure(new IllegalStateException("can't wait for refresh on a closed index"));
                return true;
            }
            List<Tuple<Long, ActionListener<Void>>> listeners = checkpointRefreshListeners;
            final int maxRefreshes = getMaxRefreshListeners.getAsInt();
            if (roomForListener(maxRefreshes, locationRefreshListeners, listeners)) {
                addCheckpointListener(checkpoint, listener, listeners);
                return false;
            }
        }
        // No free slot so fail the listener
        listener.onFailure(new IllegalStateException("Too many listeners waiting on refresh, wait listener rejected."));
        return true;
    }

    private void addCheckpointListener(long checkpoint, ActionListener<Void> listener, List<Tuple<Long, ActionListener<Void>>> listeners) {
        assert Thread.holdsLock(this);
        ActionListener<Void> contextPreservingListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);

        if (listeners == null) {
            listeners = new ArrayList<>();
        }
        // We have a free slot so register the listener
        listeners.add(new Tuple<>(checkpoint, contextPreservingListener));
        checkpointRefreshListeners = listeners;
    }

    @Override
    public void close() throws IOException {
        List<Tuple<Translog.Location, Consumer<Boolean>>> oldLocationListeners;
        List<Tuple<Long, ActionListener<Void>>> oldCheckpointListeners;
        synchronized (this) {
            oldLocationListeners = locationRefreshListeners;
            locationRefreshListeners = null;
            oldCheckpointListeners = checkpointRefreshListeners;
            checkpointRefreshListeners = null;
            closed = true;
        }
        // Fire any listeners we might have had
        fireListeners(oldLocationListeners);
        failCheckpointListeners(oldCheckpointListeners, new AlreadyClosedException("shard is closed"));
    }

    /**
     * Returns true if there are pending listeners.
     */
    public boolean refreshNeeded() {
        // A null list doesn't need a refresh. If we're closed we don't need a refresh either.
        return (locationRefreshListeners != null || checkpointRefreshListeners != null) && false == closed;
    }

    /**
     * The total number of pending listeners.
     */
    public synchronized int pendingCount() {
        List<Tuple<Translog.Location, Consumer<Boolean>>> locationListeners = locationRefreshListeners;
        List<Tuple<Long, ActionListener<Void>>> checkpointListeners = checkpointRefreshListeners;
        // A null list means we haven't accumulated any listeners. Otherwise, we need the size.
        return (locationListeners == null ? 0 : locationListeners.size()) + (checkpointListeners == null ? 0 : checkpointListeners.size());
    }

    /**
     * Setup the translog used to find the last refreshed location.
     */
    public void setCurrentRefreshLocationSupplier(Supplier<Translog.Location> currentRefreshLocationSupplier) {
        this.currentRefreshLocationSupplier = currentRefreshLocationSupplier;
    }

    /**
     * Setup the engine used to find the last processed sequence number checkpoint.
     */
    public void setCurrentProcessedCheckpointSupplier(LongSupplier processedCheckpointSupplier) {
        this.processedCheckpointSupplier = processedCheckpointSupplier;
    }

    /**
     * Setup the engine used to find the max issued seqNo.
     */
    public void setMaxIssuedSeqNoSupplier(LongSupplier maxIssuedSeqNoSupplier) {
        this.maxIssuedSeqNoSupplier = maxIssuedSeqNoSupplier;
    }

    /**
     * Snapshot of the translog location before the current refresh if there is a refresh going on or null. Doesn't have to be volatile
     * because when it is used by the refreshing thread.
     */
    private Translog.Location currentRefreshLocation;
    private Supplier<Translog.Location> currentRefreshLocationSupplier;

    /**
     * Snapshot of the local processed checkpoint before the current refresh if there is a refresh going on or null. Doesn't have to be
     * volatile because it is only used by the refreshing thread.
     */
    private long currentRefreshCheckpoint;
    private LongSupplier processedCheckpointSupplier;
    private LongSupplier maxIssuedSeqNoSupplier;

    @Override
    public void beforeRefresh() throws IOException {
        currentRefreshLocation = currentRefreshLocationSupplier.get();
        currentRefreshCheckpoint = processedCheckpointSupplier.getAsLong();
        currentRefreshStartTime = System.nanoTime();
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // Increment refresh metric before communicating to listeners.
        refreshMetric.inc(System.nanoTime() - currentRefreshStartTime);

        /* Set the lastRefreshedLocation so listeners that come in for locations before that will just execute inline without messing
         * around with refreshListeners or synchronizing at all. Note that it is not safe for us to abort early if we haven't advanced the
         * position here because we set and read lastRefreshedLocation outside of a synchronized block. We do that so that waiting for a
         * refresh that has already passed is just a volatile read but the cost is that any check whether or not we've advanced the
         * position will introduce a race between adding the listener and the position check. We could work around this by moving this
         * assignment into the synchronized block below and double checking lastRefreshedLocation in addOrNotify's synchronized block but
         * that doesn't seem worth it given that we already skip this process early if there aren't any listeners to iterate. */
        lastRefreshedLocation = currentRefreshLocation;
        lastRefreshedCheckpoint = currentRefreshCheckpoint;
        /* Grab the current refresh listeners and replace them with null while synchronized. Any listeners that come in after this won't be
         * in the list we iterate over and very likely won't be candidates for refresh anyway because we've already moved the
         * lastRefreshedLocation. */
        List<Tuple<Translog.Location, Consumer<Boolean>>> locationCandidates;
        List<Tuple<Long, ActionListener<Void>>> checkpointCandidates;
        synchronized (this) {
            locationCandidates = locationRefreshListeners;
            checkpointCandidates = checkpointRefreshListeners;
            // No listeners to check so just bail early
            if (locationCandidates == null && checkpointCandidates == null) {
                return;
            }
            locationRefreshListeners = null;
            checkpointRefreshListeners = null;
        }
        // Iterate the list of location listeners, copying the listeners to fire to one list and those to preserve to another list.
        List<Tuple<Translog.Location, Consumer<Boolean>>> locationListenersToFire = null;
        List<Tuple<Translog.Location, Consumer<Boolean>>> preservedLocationListeners = null;
        if (locationCandidates != null) {
            for (Tuple<Translog.Location, Consumer<Boolean>> tuple : locationCandidates) {
                Translog.Location location = tuple.v1();
                if (location.compareTo(currentRefreshLocation) <= 0) {
                    if (locationListenersToFire == null) {
                        locationListenersToFire = new ArrayList<>();
                    }
                    locationListenersToFire.add(tuple);
                } else {
                    if (preservedLocationListeners == null) {
                        preservedLocationListeners = new ArrayList<>();
                    }
                    preservedLocationListeners.add(tuple);
                }
            }
        }

        // Iterate the list of checkpoint listeners, copying the listeners to fire to one list and those to preserve to another list.
        List<Tuple<Long, ActionListener<Void>>> checkpointListenersToFire = null;
        List<Tuple<Long, ActionListener<Void>>> preservedCheckpointListeners = null;
        if (checkpointCandidates != null) {
            for (Tuple<Long, ActionListener<Void>> tuple : checkpointCandidates) {
                long checkpoint = tuple.v1();
                if (checkpoint <= currentRefreshCheckpoint) {
                    if (checkpointListenersToFire == null) {
                        checkpointListenersToFire = new ArrayList<>();
                    }
                    checkpointListenersToFire.add(tuple);
                } else {
                    if (preservedCheckpointListeners == null) {
                        preservedCheckpointListeners = new ArrayList<>();
                    }
                    preservedCheckpointListeners.add(tuple);
                }
            }
        }

        /* Now deal with the listeners that it isn't time yet to fire. We need to do this under lock so we don't miss a concurrent close or
         * newly registered listener. If we're not closed we just add the listeners to the list of listeners we check next time. If we are
         * closed we fire the listeners even though it isn't time for them. */
        List<Tuple<Long, ActionListener<Void>>> checkpointListenersToFail = null;
        if (preservedLocationListeners != null || preservedCheckpointListeners != null) {
            synchronized (this) {
                if (preservedLocationListeners != null) {
                    if (locationRefreshListeners == null) {
                        if (closed) {
                            if (locationListenersToFire == null) {
                                locationListenersToFire = new ArrayList<>();
                            }
                            locationListenersToFire.addAll(preservedLocationListeners);
                        } else {
                            locationRefreshListeners = preservedLocationListeners;
                        }
                    } else {
                        assert closed == false : "Can't be closed and have non-null refreshListeners";
                        locationRefreshListeners.addAll(preservedLocationListeners);
                    }
                }
                if (preservedCheckpointListeners != null) {
                    if (checkpointRefreshListeners == null) {
                        if (closed) {
                            checkpointListenersToFail = new ArrayList<>(preservedCheckpointListeners);
                        } else {
                            checkpointRefreshListeners = preservedCheckpointListeners;
                        }
                    } else {
                        assert closed == false : "Can't be closed and have non-null refreshListeners";
                        checkpointRefreshListeners.addAll(preservedCheckpointListeners);
                    }
                }
            }
        }
        // Lastly, fire the listeners that are ready
        fireListeners(locationListenersToFire);
        fireCheckpointListeners(checkpointListenersToFire);
        failCheckpointListeners(checkpointListenersToFail, new AlreadyClosedException("shard is closed"));
    }

    /**
     * Fire location listeners. Does nothing if the list of listeners is null.
     */
    private void fireListeners(final List<Tuple<Translog.Location, Consumer<Boolean>>> listenersToFire) {
        if (listenersToFire != null) {
            for (final Tuple<Translog.Location, Consumer<Boolean>> listener : listenersToFire) {
                try {
                    listener.v2().accept(false);
                } catch (final Exception e) {
                    logger.warn("error firing location refresh listener", e);
                }
            }
        }
    }

    private static boolean roomForListener(
        final int maxRefreshes,
        final List<Tuple<Translog.Location, Consumer<Boolean>>> locationListeners,
        final List<Tuple<Long, ActionListener<Void>>> checkpointListeners
    ) {
        final int locationListenerCount = locationListeners == null ? 0 : locationListeners.size();
        final int checkpointListenerCount = checkpointListeners == null ? 0 : checkpointListeners.size();
        return (locationListenerCount + checkpointListenerCount) < maxRefreshes;
    }

    /**
     * Fire checkpoint listeners. Does nothing if the list of listeners is null.
     */
    private void fireCheckpointListeners(final List<Tuple<Long, ActionListener<Void>>> listenersToFire) {
        if (listenersToFire != null) {
            for (final Tuple<Long, ActionListener<Void>> listener : listenersToFire) {
                try {
                    listener.v2().onResponse(null);
                } catch (final Exception e) {
                    logger.warn("error firing checkpoint refresh listener", e);
                    assert false;
                }
            }
        }
    }

    /**
     * Fail checkpoint listeners. Does nothing if the list of listeners is null.
     */
    private void failCheckpointListeners(final List<Tuple<Long, ActionListener<Void>>> listenersToFire, Exception exception) {
        if (listenersToFire != null) {
            for (final Tuple<Long, ActionListener<Void>> listener : listenersToFire) {
                try {
                    listener.v2().onFailure(exception);
                } catch (final Exception e) {
                    logger.warn("error firing checkpoint refresh listener", e);
                    assert false;
                }
            }
        }
    }
}
