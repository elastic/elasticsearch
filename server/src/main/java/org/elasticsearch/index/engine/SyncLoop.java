/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SyncLoop {

    private static final Logger logger = LogManager.getLogger(SyncLoop.class);

    private final ThreadPool threadPool;
    private final Translog translog;
    private final CheckedRunnable<IOException> postSyncCallback;
    private final Semaphore promiseSemaphore = new Semaphore(1);
    private final PriorityQueue<Tuple<Translog.Location, Consumer<Exception>>> listeners;
    private volatile Translog.Location lastSyncedLocation;

    public SyncLoop(ThreadPool threadPool, Translog translog, CheckedRunnable<IOException> postSyncCallback) {
        this.threadPool = threadPool;
        this.translog = translog;
        this.postSyncCallback = postSyncCallback;
        this.lastSyncedLocation = new Translog.Location(0, 0, Integer.MAX_VALUE);
        this.listeners = new PriorityQueue<>(1024, Comparator.comparing(Tuple::v1));
    }

    public void maybeScheduleIncrementalSync() {
        if (translog.shouldIncrementalSync()) {
            final boolean promised = promiseSemaphore.tryAcquire();
            if (promised) {
                threadPool.executor(ThreadPool.Names.FLUSH).execute(this::runSyncLoop);
            }
        }
    }

    public void scheduleSync(Translog.Location location, Consumer<Exception> listener) {
        if (lastSyncedLocation.compareTo(location) > 0) {
            listener.accept(null);
            return;
        }
        synchronized (listeners) {
            listeners.add(new Tuple<>(location, preserveContext(listener)));
        }
        final boolean promised = promiseSemaphore.tryAcquire();
        if (promised) {
            try {
                threadPool.executor(ThreadPool.Names.FLUSH).execute(this::runSyncLoop);
            } catch (Exception e) {
                promiseSemaphore.release();
                throw e;
            }
        }
    }

    private void runSyncLoop() {
        assert promiseSemaphore.availablePermits() == 0;
        doSyncAndNotify();
        while ((isListenersEmpty() == false || translog.shouldIncrementalSync()) && promiseSemaphore.tryAcquire()) {
            doSyncAndNotify();
        }
    }

    private void doSyncAndNotify() {
        assert promiseSemaphore.availablePermits() == 0;
        Exception exception = null;
        ArrayList<Consumer<Exception>> syncedListeners = new ArrayList<>(listenerCount());
        try {
            drainListeners(syncedListeners, lastSyncedLocation);
            completeListeners(syncedListeners, null);
            syncedListeners.clear();
            doSync();
            drainListeners(syncedListeners, lastSyncedLocation);
        } catch (Exception e) {
            drainListeners(syncedListeners, null);
            exception = e;
        } finally {
            promiseSemaphore.release();
        }
        completeListeners(syncedListeners, exception);
    }

    private void doSync() throws Exception {
        translog.sync();
        lastSyncedLocation = translog.getLastSyncedLocation();
        postSyncCallback.run();
    }

    private int listenerCount() {
        synchronized (listeners) {
            return listeners.size();
        }
    }

    private boolean isListenersEmpty() {
        synchronized (listeners) {
            return listeners.isEmpty();
        }
    }

    private void drainListeners(ArrayList<Consumer<Exception>> syncedListeners, Translog.Location syncedLocation) {
        synchronized (listeners) {
            Tuple<Translog.Location, Consumer<Exception>> tuple;
            while ((tuple = listeners.poll()) != null) {
                // If the synced location is null, drain all the listeners
                if (syncedLocation == null || syncedLocation.compareTo(tuple.v1()) > 0) {
                    syncedListeners.add(tuple.v2());
                } else {
                    listeners.add(tuple);
                    break;
                }
            }
        }
    }

    private void completeListeners(ArrayList<Consumer<Exception>> syncedListeners, Exception e) {
        for (Consumer<Exception> consumer : syncedListeners) {
            try {
                consumer.accept(e);
            } catch (Exception ex) {
                logger.warn("failed to notify fsync listener", ex);
            }
        }
    }

    private Consumer<Exception> preserveContext(Consumer<Exception> consumer) {
        Supplier<ThreadContext.StoredContext> restorableContext = threadPool.getThreadContext().newRestorableContext(false);
        return e -> {
            try (ThreadContext.StoredContext ignore = restorableContext.get()) {
                consumer.accept(e);
            }
        };
    }
}
