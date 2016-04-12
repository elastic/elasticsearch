/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.watcher.support.concurrent.FairKeyedLock;
import org.joda.time.PeriodType;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.watcher.support.Exceptions.illegalState;

/**
 *
 */
public class WatchLockService extends AbstractComponent {

    private final FairKeyedLock<String> watchLocks = new FairKeyedLock<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private static final TimeValue DEFAULT_MAX_STOP_TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String DEFAULT_MAX_STOP_TIMEOUT_SETTING = "xpack.watcher.stop.timeout";

    private final TimeValue maxStopTimeout;

    @Inject
    public WatchLockService(Settings settings){
        super(settings);
        maxStopTimeout = settings.getAsTime(DEFAULT_MAX_STOP_TIMEOUT_SETTING, DEFAULT_MAX_STOP_TIMEOUT);
    }

    WatchLockService(TimeValue maxStopTimeout){
        super(Settings.EMPTY);
        this.maxStopTimeout = maxStopTimeout;
    }

    public Lock acquire(String name) {
        if (!running.get()) {
            throw illegalState("cannot acquire lock for watch [{}]. lock service is not running", name);
        }

        watchLocks.acquire(name);
        return new Lock(name, watchLocks);
    }

    public Lock tryAcquire(String name, TimeValue timeout) {
        if (!running.get()) {
            throw illegalState("cannot acquire lock for watch [{}]. lock service is not running", name);
        }
        try {
            if (!watchLocks.tryAcquire(name, timeout.millis(), TimeUnit.MILLISECONDS)) {
                logger.warn("failed to acquire lock on watch [{}] (waited for [{}]). It is possible that for some reason this watch " +
                        "execution is stuck", name, timeout.format(PeriodType.seconds()));
                return null;
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            //todo figure out a better std exception for this
            logger.error("could not acquire lock for watch [{}]", ie, name);
            return null;
        }
        return new Lock(name, watchLocks);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            // init
        }
    }

    /**
     * @throws ElasticsearchTimeoutException if we have waited longer than maxStopTimeout
     */
    public void stop() throws ElasticsearchTimeoutException {
        if (running.compareAndSet(true, false)) {
            // It can happen we have still ongoing operations and we wait those operations to finish to avoid
            // that watch service or any of its components end up in a illegal state after the state as been set to stopped.
            //
            // For example: A watch action entry may be added while we stopping watcher if we don't wait for
            // ongoing operations to complete. Resulting in once the watch service starts again that more than
            // expected watch records are processed.
            //
            // Note: new operations will fail now because the running has been set to false
            long startWait = System.currentTimeMillis();
            while (watchLocks.hasLockedKeys()) {
                TimeValue timeWaiting = new TimeValue(System.currentTimeMillis() - startWait);
                if (timeWaiting.getSeconds() > maxStopTimeout.getSeconds()) {
                    throw new ElasticsearchTimeoutException("timed out waiting for watches to complete, after waiting for [{}]",
                            timeWaiting);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    FairKeyedLock<String> getWatchLocks() {
        return watchLocks;
    }

    public static class Lock {

        private final String name;
        private final FairKeyedLock<String> watchLocks;

        private Lock(String name, FairKeyedLock<String> watchLocks) {
            this.name = name;
            this.watchLocks = watchLocks;

        }

        public void release() {
            watchLocks.release(name);
        }
    }

}
