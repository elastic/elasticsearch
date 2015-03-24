/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.util.concurrent.KeyedLock;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class WatchLockService {

    private final KeyedLock<String> watchLocks = new KeyedLock<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public Lock acquire(String name) {
        if (!running.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }

        watchLocks.acquire(name);
        return new Lock(name, watchLocks);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            // init
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            // It can happen we have still ongoing operations and we wait those operations to finish to avoid
            // that watch service or any of its components end up in a illegal state after the state as been set to stopped.
            //
            // For example: A watch action entry may be added while we stopping watcher if we don't wait for
            // ongoing operations to complete. Resulting in once the watch service starts again that more than
            // expected watch records are processed.
            //
            // Note: new operations will fail now because the running has been set to false
            while (watchLocks.hasLockedKeys()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    KeyedLock<String> getWatchLocks() {
        return watchLocks;
    }

    public static class Lock {

        private final String name;
        private final KeyedLock<String> watchLocks;

        private Lock(String name, KeyedLock<String> watchLocks) {
            this.name = name;
            this.watchLocks = watchLocks;

        }

        public void release() {
            watchLocks.release(name);
        }
    }
}
