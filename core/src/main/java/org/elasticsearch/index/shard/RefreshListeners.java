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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import static java.util.Objects.requireNonNull;

/**
 * Allows for the registration of listeners that are called when a change becomes visible for search. This functionality is exposed from
 * {@link IndexShard} but kept here so it can be tested without standing up the entire thing. 
 */
final class RefreshListeners {
    /**
     * Refresh listeners. While they are not stored in sorted order they are processed as though they are.
     */
    private final LinkedTransferQueue<Tuple<Translog.Location, Consumer<Boolean>>> refreshListeners = new LinkedTransferQueue<>();

    private final IntSupplier getMaxRefreshListeners;
    private final Runnable forceRefresh;
    private final Executor listenerExecutor;

    /**
     * The estimated size of refreshListenersEstimatedSize used for triggering refresh when the size gets over
     * {@link IndexSettings#MAX_REFRESH_LISTENERS_PER_SHARD}. No effort is made to correct for threading issues in the size calculation
     * beyond it being volatile.
     */
    private volatile int refreshListenersEstimatedSize;
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
        Translog.Location listenerLocation = requireNonNull(location, "location cannot be null");

        Translog.Location lastRefresh = lastRefreshedLocation;
        if (lastRefresh != null && lastRefresh.compareTo(listenerLocation) >= 0) {
            // Location already visible, just call the listener
            listener.accept(false);
            return;
        }
        if (refreshListenersEstimatedSize < getMaxRefreshListeners.getAsInt()) {
            // We have a free slot so register the listener
            refreshListeners.add(new Tuple<>(location, listener));
            refreshListenersEstimatedSize++;
            return;
        }
        // No free slot so force a refresh and call the listener in this thread
        forceRefresh.run();
        listener.accept(true);
    }

    /**
     * Start listening to an engine.
     */
    public void listenTo(Engine engine) {
        engine.registerSearchRefreshListener(new RefreshListenerCallingRefreshListener(engine.getTranslog()));
    }

    /**
     * Listens to Lucene's {@linkplain ReferenceManager.RefreshListener} and fires off listeners added by
     * {@linkplain IndexShard#addRefreshListener(Translog.Location, Consumer)}.
     */
    private class RefreshListenerCallingRefreshListener implements ReferenceManager.RefreshListener {
        private final Translog translog;
        /**
         * Snapshot of the translog location before the current refresh if there is a refresh going on or null. Doesn't have to be volatile
         * because when it is used by the refreshing thread.
         */
        private Translog.Location currentRefreshLocation;

        public RefreshListenerCallingRefreshListener(Translog translog) {
            this.translog = translog;
        }

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
             * Now pop all listeners off the front of refreshListeners that are ready to be called. The listeners won't always be in order
             * but they should be pretty close because you don't listen to times super far in the future. This prevents us from having to
             * iterate over the whole queue on every refresh at the cost of some requests having to wait an extra cycle if they get stuck
             * behind a request that missed the refresh cycle.
             */
            Iterator<Tuple<Translog.Location, Consumer<Boolean>>> itr = refreshListeners.iterator();
            while (itr.hasNext()) {
                Tuple<Translog.Location, Consumer<Boolean>> listener = itr.next();
                if (listener.v1().compareTo(currentRefreshLocation) > 0) {
                    return;
                }
                itr.remove();
                refreshListenersEstimatedSize--;
                listenerExecutor.execute(() -> listener.v2().accept(false));
            }
            refreshListenersEstimatedSize = 0;
        }
    }
}
