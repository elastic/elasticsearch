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

import org.apache.lucene.search.ReferenceManager;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig.EngineCreationListener;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import static java.util.Objects.requireNonNull;

/**
 * Allows for the registration of listeners that are called when a change becomes visible for search. This functionality is exposed from
 * {@link IndexShard} but kept here so it can be tested without standing up the entire thing. 
 */
final class RefreshListeners implements EngineCreationListener, ReferenceManager.RefreshListener {
    private final IntSupplier getMaxRefreshListeners;
    private final Runnable forceRefresh;
    private final Executor listenerExecutor;

    /**
     * List of refresh listeners. Defaults to null and built on demand because most refresh cycles won't need it. Entries are never removed
     * from it, rather, it is nulled and rebuilt when needed again. The (hopefully) rare entries that didn't make the current refresh cycle
     * are just added back to the new list. Both the reference and the contents are always modified while synchronized on {@code this}.
     */
    private volatile List<Tuple<Translog.Location, Consumer<Boolean>>> refreshListeners = null;
    /**
     * The translog location that was last made visible by a refresh.
     */
    private volatile Translog.Location lastRefreshedLocation;

    public RefreshListeners(IntSupplier getMaxRefreshListeners, Runnable forceRefresh, Executor listenerExecutor) {
        this.getMaxRefreshListeners = getMaxRefreshListeners;
        this.forceRefresh = forceRefresh;
        this.listenerExecutor = listenerExecutor;
    }

    /**
     * Add a listener for refreshes, calling it immediately if the location is already visible. If this runs out of listener slots then it
     * forces a refresh and calls the listener immediately as well.
     *
     * @param location the location to listen for
     * @param listener for the refresh. Called with true if registering the listener ran it out of slots and forced a refresh. Called with
     *        false otherwise.
     */
    public void addOrNotify(Translog.Location location, Consumer<Boolean> listener) {
        requireNonNull(listener, "listener cannot be null");
        requireNonNull(location, "location cannot be null");

        Translog.Location lastRefresh = lastRefreshedLocation;
        if (lastRefresh != null && lastRefresh.compareTo(location) >= 0) {
            // Location already visible, just call the listener
            listener.accept(false);
            return;
        }
        synchronized (this) {
            if (refreshListeners == null) {
                refreshListeners = new ArrayList<>();
            }
            if (refreshListeners.size() < getMaxRefreshListeners.getAsInt()) {
                // We have a free slot so register the listener
                refreshListeners.add(new Tuple<>(location, listener));
                return;
            }
        }
        // No free slot so force a refresh and call the listener in this thread
        forceRefresh.run();
        listener.accept(true);
    }

    @Override
    public void engineCreated(Engine engine) {
        translog = engine.getTranslog();
    }

    // Implementation of ReferenceManager.RefreshListener that adapts Lucene's RefreshListener into Elasticsearch's refresh listeners.
    private Translog translog;
    /**
     * Snapshot of the translog location before the current refresh if there is a refresh going on or null. Doesn't have to be volatile
     * because when it is used by the refreshing thread.
     */
    private Translog.Location currentRefreshLocation;

    @Override
    public void beforeRefresh() throws IOException {
        currentRefreshLocation = translog.getLastWriteLocation();
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // This intentionally ignores didRefresh so a refresh call over the API can force rechecking the refreshListeners.
        if (null == currentRefreshLocation) {
            /*
             * The translog had an empty last write location at the start of the refresh so we can't alert anyone to anything. This
             * usually happens during recovery. The next refresh cycle out to pick up this refresh.
             */
            return;
        }
        /*
         * First set the lastRefreshedLocation so listeners that come in locations before that will just execute inline without messing
         * around with refreshListeners at all.
         */
        lastRefreshedLocation = currentRefreshLocation;
        /*
         * Grab the current refresh listeners and replace them with a new list while synchronized. Any listeners that come in after this
         * won't be in the list we iterate over and very likely won't be candidates for refresh anyway because we've already moved the
         * lastRefreshedLocation.
         */
        List<Tuple<Translog.Location, Consumer<Boolean>>> candidates;
        synchronized (this) {
            candidates = refreshListeners;
            // No listeners to check so just bail early
            if (candidates == null) {
                return;
            }
            refreshListeners = null;
        }
        /*
         * Iterate the list of listeners, preserving the ones that we couldn't fire in a new list. We expect to fire most of them so this
         * copying should be minimial. Much less overhead than removing all of the fired ones from the list.
         */
        List<Tuple<Translog.Location, Consumer<Boolean>>> preservedListeners = null;
        for (Tuple<Translog.Location, Consumer<Boolean>> tuple : candidates) {
            Translog.Location location = tuple.v1();
            Consumer<Boolean> listener = tuple.v2();
            if (location.compareTo(currentRefreshLocation) <= 0) {
                listenerExecutor.execute(() -> listener.accept(false));
            } else {
                if (preservedListeners == null) {
                    preservedListeners = new ArrayList<>();
                }
                preservedListeners.add(tuple);
            }
        }
        /*
         * Now add any preserved listeners back to the running list of refresh listeners. We'll try them next time.
         */
        if (preservedListeners != null) {
            synchronized (this) {
                if (refreshListeners == null) {
                    refreshListeners = new ArrayList<>();
                }
                refreshListeners.addAll(preservedListeners);
            }
        }
    }
}
