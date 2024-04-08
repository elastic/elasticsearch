/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.watcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Generic resource watcher service
 *
 * Other elasticsearch services can register their resource watchers with this service using {@link #add(ResourceWatcher)}
 * method. This service will call {@link org.elasticsearch.watcher.ResourceWatcher#checkAndNotify()} method of all
 * registered watcher periodically. The frequency of checks can be specified using {@code resource.reload.interval} setting, which
 * defaults to {@code 60s}. The service can be disabled by setting {@code resource.reload.enabled} setting to {@code false}.
 */
public class ResourceWatcherService implements Closeable {
    private static final Logger logger = LogManager.getLogger(ResourceWatcherService.class);

    public enum Frequency {

        /**
         * Defaults to 5 seconds
         */
        HIGH(TimeValue.timeValueSeconds(5)),

        /**
         * Defaults to 30 seconds
         */
        MEDIUM(TimeValue.timeValueSeconds(30)),

        /**
         * Defaults to 60 seconds
         */
        LOW(TimeValue.timeValueSeconds(60));

        final TimeValue interval;

        Frequency(TimeValue interval) {
            this.interval = interval;
        }
    }

    public static final Setting<Boolean> ENABLED = Setting.boolSetting("resource.reload.enabled", true, Property.NodeScope);
    public static final Setting<TimeValue> RELOAD_INTERVAL_HIGH = Setting.timeSetting(
        "resource.reload.interval.high",
        Frequency.HIGH.interval,
        Property.NodeScope
    );
    public static final Setting<TimeValue> RELOAD_INTERVAL_MEDIUM = Setting.timeSetting(
        "resource.reload.interval.medium",
        Setting.timeSetting("resource.reload.interval", Frequency.MEDIUM.interval),
        Property.NodeScope
    );
    public static final Setting<TimeValue> RELOAD_INTERVAL_LOW = Setting.timeSetting(
        "resource.reload.interval.low",
        Frequency.LOW.interval,
        Property.NodeScope
    );

    private final boolean enabled;

    final ResourceMonitor lowMonitor;
    final ResourceMonitor mediumMonitor;
    final ResourceMonitor highMonitor;

    private final Cancellable lowFuture;
    private final Cancellable mediumFuture;
    private final Cancellable highFuture;

    public ResourceWatcherService(Settings settings, ThreadPool threadPool) {
        this.enabled = ENABLED.get(settings);

        TimeValue interval = RELOAD_INTERVAL_LOW.get(settings);
        lowMonitor = new ResourceMonitor(interval, Frequency.LOW);
        interval = RELOAD_INTERVAL_MEDIUM.get(settings);
        mediumMonitor = new ResourceMonitor(interval, Frequency.MEDIUM);
        interval = RELOAD_INTERVAL_HIGH.get(settings);
        highMonitor = new ResourceMonitor(interval, Frequency.HIGH);
        if (enabled) {
            final var executor = threadPool.generic();
            lowFuture = threadPool.scheduleWithFixedDelay(lowMonitor, lowMonitor.interval, executor);
            mediumFuture = threadPool.scheduleWithFixedDelay(mediumMonitor, mediumMonitor.interval, executor);
            highFuture = threadPool.scheduleWithFixedDelay(highMonitor, highMonitor.interval, executor);
        } else {
            lowFuture = null;
            mediumFuture = null;
            highFuture = null;
        }
    }

    @Override
    public void close() {
        if (enabled) {
            lowFuture.cancel();
            mediumFuture.cancel();
            highFuture.cancel();
        }
    }

    /**
     * Register new resource watcher that will be checked in default {@link Frequency#MEDIUM MEDIUM} frequency
     */
    public <W extends ResourceWatcher> WatcherHandle<W> add(W watcher) throws IOException {
        return add(watcher, Frequency.MEDIUM);
    }

    /**
     * Register new resource watcher that will be checked in the given frequency
     */
    public <W extends ResourceWatcher> WatcherHandle<W> add(W watcher, Frequency frequency) throws IOException {
        watcher.init();
        return switch (frequency) {
            case LOW -> lowMonitor.add(watcher);
            case MEDIUM -> mediumMonitor.add(watcher);
            case HIGH -> highMonitor.add(watcher);
        };
    }

    public void notifyNow(Frequency frequency) {
        switch (frequency) {
            case LOW -> lowMonitor.run();
            case MEDIUM -> mediumMonitor.run();
            case HIGH -> highMonitor.run();
            default -> throw new IllegalArgumentException("Unknown frequency [" + frequency + "]");
        }
    }

    static class ResourceMonitor implements Runnable {

        final TimeValue interval;
        final Frequency frequency;

        final Set<ResourceWatcher> watchers = new CopyOnWriteArraySet<>();

        private ResourceMonitor(TimeValue interval, Frequency frequency) {
            this.interval = interval;
            this.frequency = frequency;
        }

        private <W extends ResourceWatcher> WatcherHandle<W> add(W watcher) {
            watchers.add(watcher);
            return new WatcherHandle<>(this, watcher);
        }

        @Override
        public synchronized void run() {
            for (ResourceWatcher watcher : watchers) {
                try {
                    watcher.checkAndNotify();
                } catch (IOException e) {
                    logger.trace("failed to check resource watcher", e);
                }
            }
        }
    }
}
