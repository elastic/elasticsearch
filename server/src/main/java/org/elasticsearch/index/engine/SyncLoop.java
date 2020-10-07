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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class SyncLoop {

    private final Logger logger;

    private final ThreadPool threadPool;
    private final Translog translog;
    private final Semaphore promiseSemaphore = new Semaphore(1);
    private final ConcurrentSkipListMap<Translog.Location, Consumer<Exception>> listeners = new ConcurrentSkipListMap<>();
    private volatile Translog.Location lastSyncedLocation;

    public SyncLoop(Logger logger, ThreadPool threadPool, Translog translog) {
        this.logger = logger;
        this.threadPool = threadPool;
        this.translog = translog;
    }

    public void sync() {

    }

    public void scheduleWrite() {
        final boolean promised = promiseSemaphore.tryAcquire();
        if (promised) {
//            translog.flush();
        }
    }

    public void scheduleSync(Translog.Location location, Consumer<Exception> listener) {
        if (lastSyncedLocation.compareTo(location) > 0) {
            listener.accept(null);
            return;
        }
        listeners.put(location, listener);
        final boolean promised = promiseSemaphore.tryAcquire();
        if (promised) {
            try {
                threadPool.executor(ThreadPool.Names.FLUSH).execute(() -> {
                    Exception exception = null;
                    ArrayList<Consumer<Exception>> syncedListeners = new ArrayList<>(this.listeners.size());
                    try {
                        doSync();
                        drainListeners(syncedListeners, lastSyncedLocation);
                    } catch (Exception e) {
                        drainListeners(syncedListeners, null);
                        exception = e;
                    } finally {
                        promiseSemaphore.release();
                    }
                    completeListeners(syncedListeners, exception);
                });
            } catch (Exception e) {
                promiseSemaphore.release();
                throw e;
            }
        }
    }

    private void doSync() throws Exception {
        translog.sync();
        lastSyncedLocation = translog.getLastSyncedLocation();
    }

    private void drainListeners(ArrayList<Consumer<Exception>> syncedListeners, Translog.Location syncedLocation) {
        Map.Entry<Translog.Location, Consumer<Exception>> entry;
        while ((entry = listeners.pollFirstEntry()) != null) {
            // If the synced location is null, drain all the listeners
            if (syncedLocation == null || entry.getKey().compareTo(syncedLocation) > 0) {
                syncedListeners.add(entry.getValue());
            } else {
                listeners.put(entry.getKey(), entry.getValue());
                break;
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
}
