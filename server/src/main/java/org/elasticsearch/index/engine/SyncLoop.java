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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SyncLoop {

    private static final Logger logger = LogManager.getLogger(SyncLoop.class);
    private static final long ONE_MILLIS = TimeUnit.MILLISECONDS.toNanos(1);

    private final ThreadPool threadPool;
    private final Translog translog;
    private final CheckedRunnable<IOException> postSyncCallback;
    private final Semaphore promiseSemaphore = new Semaphore(1);
    private final PriorityQueue<Tuple<Translog.Location, Consumer<Exception>>> listeners;
    private volatile int listenerCount = 0;
    private volatile Translog.Location lastSyncedLocation;
    private volatile long lastSyncedNanos;

    public SyncLoop(ThreadPool threadPool, Translog translog, CheckedRunnable<IOException> postSyncCallback) {
        this.threadPool = threadPool;
        this.translog = translog;
        this.postSyncCallback = postSyncCallback;
        this.lastSyncedLocation = new Translog.Location(0, 0, Integer.MAX_VALUE);
        this.listeners = new PriorityQueue<>(1024, Comparator.comparing(Tuple::v1));
        this.lastSyncedNanos = System.nanoTime();
    }

    public void maybeScheduleIncrementalFlush() {
        if (translog.shouldIncrementalFlush()) {
            if (promiseSemaphore.tryAcquire()) {
                submitSyncTask(false);
            }
        }
    }

    public void scheduleSync(Translog.Location location, Consumer<Exception> listener) {
        if (locationSynced(lastSyncedLocation, location)) {
            listener.accept(null);
            return;
        }
        synchronized (listeners) {
            ++listenerCount;
            listeners.add(new Tuple<>(location, preserveContext(listener)));
        }
        if (promiseSemaphore.tryAcquire()) {
            long delay = Math.max(ONE_MILLIS - (System.nanoTime() - lastSyncedNanos), 0);
            if (delay == 0) {
                submitSyncTask(true);
            } else {
                threadPool.schedule(syncTask(true), TimeValue.timeValueNanos(delay), ThreadPool.Names.FLUSH);
            }
        }
    }

    private void submitSyncTask(boolean startWithSync) {
        threadPool.executor(ThreadPool.Names.FLUSH).execute(syncTask(startWithSync));
    }

    private AbstractRunnable syncTask(boolean startWithSync) {
        return new AbstractRunnable() {
            @Override
            public void onAfter() {
                promiseSemaphore.release();
                if (isListenersEmpty() == false && promiseSemaphore.tryAcquire()) {
                    submitSyncTask(true);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Uncaught exception on the sync loop", e);
            }

            @Override
            protected void doRun() {
                runSyncLoop(startWithSync);
            }
        };
    }

    private void runSyncLoop(boolean startWithSync) {
        assert promiseSemaphore.availablePermits() == 0;
        lastSyncedLocation = translog.getLastSyncedLocation();
        if (startWithSync) {
            doSyncAndNotify();
        } else {
            try {
                translog.flush();
            } catch (Exception e) {
                logger.debug("Exception when incrementally flushing translog", e);
            }
        }
        while (isListenersEmpty() == false) {
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
            if (isListenersEmpty()) {
                return;
            }
            doSync();
            drainListeners(syncedListeners, lastSyncedLocation);
        } catch (Exception e) {
            drainListeners(syncedListeners, null);
            exception = e;
        }
        completeListeners(syncedListeners, exception);
    }

    private void doSync() throws Exception {
        translog.sync();
        lastSyncedLocation = translog.getLastSyncedLocation();
        lastSyncedNanos = System.nanoTime();
        postSyncCallback.run();
    }

    private int listenerCount() {
        return listenerCount;
    }

    private boolean isListenersEmpty() {
        return listenerCount == 0;
    }

    private void drainListeners(ArrayList<Consumer<Exception>> syncedListeners, Translog.Location syncedLocation) {
        synchronized (listeners) {
            Tuple<Translog.Location, Consumer<Exception>> tuple;
            while ((tuple = listeners.poll()) != null) {
                // If the synced location is null, drain all the listeners
                if (syncedLocation == null || locationSynced(syncedLocation, tuple.v1())) {
                    --listenerCount;
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

    private static boolean locationSynced(Translog.Location synced, Translog.Location location) {
        if (synced.generation > location.generation) {
            return true;
        } else {
            if (synced.generation < location.generation) {
                return false;
            } else {
                return synced.translogLocation >= (location.translogLocation + location.size);
            }
        }
    }
}
