/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.translog.Translog;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
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
    private volatile List<Tuple<Translog.Location, Consumer<Boolean>>> refreshListeners = null;
    /**
     * The translog location that was last made visible by a refresh.
     */
    private volatile Translog.Location lastRefreshedLocation;

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
        final RunOnce runOnce = new RunOnce(() -> {
            synchronized (RefreshListeners.this) {
                assert refreshForcers > 0;
                refreshForcers -= 1;
            }
        });
        if (refreshNeeded()) {
            try {
                forceRefresh.run();
            } catch (Exception e) {
                runOnce.run();
                throw e;
            }
        }
        assert refreshListeners == null;
        return () -> runOnce.run();
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
            List<Tuple<Translog.Location, Consumer<Boolean>>> listeners = refreshListeners;
            final int maxRefreshes = getMaxRefreshListeners.getAsInt();
            if (refreshForcers == 0 && maxRefreshes > 0 && (listeners == null || listeners.size() < maxRefreshes)) {
                ThreadContext.StoredContext storedContext = threadContext.newStoredContext(true);
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
                refreshListeners = listeners;
                return false;
            }
        }
        // No free slot so force a refresh and call the listener in this thread
        forceRefresh.run();
        listener.accept(true);
        return true;
    }

    @Override
    public void close() throws IOException {
        List<Tuple<Translog.Location, Consumer<Boolean>>> oldListeners;
        synchronized (this) {
            oldListeners = refreshListeners;
            refreshListeners = null;
            closed = true;
        }
        // Fire any listeners we might have had
        fireListeners(oldListeners);
    }

    /**
     * Returns true if there are pending listeners.
     */
    public boolean refreshNeeded() {
        // A null list doesn't need a refresh. If we're closed we don't need a refresh either.
        return refreshListeners != null && false == closed;
    }

    /**
     * The number of pending listeners.
     */
    public int pendingCount() {
        // No need to synchronize here because we're doing a single volatile read
        List<Tuple<Translog.Location, Consumer<Boolean>>> listeners = refreshListeners;
        // A null list means we haven't accumulated any listeners. Otherwise we need the size.
        return listeners == null ? 0 : listeners.size();
    }

    /**
     * Setup the translog used to find the last refreshed location.
     */
    public void setCurrentRefreshLocationSupplier(Supplier<Translog.Location> currentRefreshLocationSupplier) {
        this.currentRefreshLocationSupplier = currentRefreshLocationSupplier;
    }

    /**
     * Snapshot of the translog location before the current refresh if there is a refresh going on or null. Doesn't have to be volatile
     * because when it is used by the refreshing thread.
     */
    private Translog.Location currentRefreshLocation;
    private Supplier<Translog.Location> currentRefreshLocationSupplier;

    @Override
    public void beforeRefresh() throws IOException {
        currentRefreshLocation = currentRefreshLocationSupplier.get();
        currentRefreshStartTime = System.nanoTime();
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // Increment refresh metric before communicating to listeners.
        refreshMetric.inc(System.nanoTime() - currentRefreshStartTime);

        /* We intentionally ignore didRefresh here because our timing is a little off. It'd be a useful flag if we knew everything that made
         * it into the refresh, but the way we snapshot the translog position before the refresh, things can sneak into the refresh that we
         * don't know about. */
        if (null == currentRefreshLocation) {
            /* The translog had an empty last write location at the start of the refresh so we can't alert anyone to anything. This
             * usually happens during recovery. The next refresh cycle out to pick up this refresh. */
            return;
        }
        /* Set the lastRefreshedLocation so listeners that come in for locations before that will just execute inline without messing
         * around with refreshListeners or synchronizing at all. Note that it is not safe for us to abort early if we haven't advanced the
         * position here because we set and read lastRefreshedLocation outside of a synchronized block. We do that so that waiting for a
         * refresh that has already passed is just a volatile read but the cost is that any check whether or not we've advanced the
         * position will introduce a race between adding the listener and the position check. We could work around this by moving this
         * assignment into the synchronized block below and double checking lastRefreshedLocation in addOrNotify's synchronized block but
         * that doesn't seem worth it given that we already skip this process early if there aren't any listeners to iterate. */
        lastRefreshedLocation = currentRefreshLocation;
        /* Grab the current refresh listeners and replace them with null while synchronized. Any listeners that come in after this won't be
         * in the list we iterate over and very likely won't be candidates for refresh anyway because we've already moved the
         * lastRefreshedLocation. */
        List<Tuple<Translog.Location, Consumer<Boolean>>> candidates;
        synchronized (this) {
            candidates = refreshListeners;
            // No listeners to check so just bail early
            if (candidates == null) {
                return;
            }
            refreshListeners = null;
        }
        // Iterate the list of listeners, copying the listeners to fire to one list and those to preserve to another list.
        List<Tuple<Translog.Location, Consumer<Boolean>>> listenersToFire = null;
        List<Tuple<Translog.Location, Consumer<Boolean>>> preservedListeners = null;
        for (Tuple<Translog.Location, Consumer<Boolean>> tuple : candidates) {
            Translog.Location location = tuple.v1();
            if (location.compareTo(currentRefreshLocation) <= 0) {
                if (listenersToFire == null) {
                    listenersToFire = new ArrayList<>();
                }
                listenersToFire.add(tuple);
            } else {
                if (preservedListeners == null) {
                    preservedListeners = new ArrayList<>();
                }
                preservedListeners.add(tuple);
            }
        }
        /* Now deal with the listeners that it isn't time yet to fire. We need to do this under lock so we don't miss a concurrent close or
         * newly registered listener. If we're not closed we just add the listeners to the list of listeners we check next time. If we are
         * closed we fire the listeners even though it isn't time for them. */
        if (preservedListeners != null) {
            synchronized (this) {
                if (refreshListeners == null) {
                    if (closed) {
                        listenersToFire.addAll(preservedListeners);
                    } else {
                        refreshListeners = preservedListeners;
                    }
                } else {
                    assert closed == false : "Can't be closed and have non-null refreshListeners";
                    refreshListeners.addAll(preservedListeners);
                }
            }
        }
        // Lastly, fire the listeners that are ready
        fireListeners(listenersToFire);
    }

    /**
     * Fire some listeners. Does nothing if the list of listeners is null.
     */
    private void fireListeners(final List<Tuple<Translog.Location, Consumer<Boolean>>> listenersToFire) {
        if (listenersToFire != null) {
            for (final Tuple<Translog.Location, Consumer<Boolean>> listener : listenersToFire) {
                try {
                    listener.v2().accept(false);
                } catch (final Exception e) {
                    logger.warn("error firing refresh listener", e);
                }
            }
        }
    }
}
