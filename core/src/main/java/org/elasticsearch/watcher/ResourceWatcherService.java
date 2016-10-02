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
package org.elasticsearch.watcher;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Cancellable;
import org.elasticsearch.threadpool.ThreadPool.Names;

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
public class ResourceWatcherService extends AbstractLifecycleComponent {

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
    public static final Setting<TimeValue> RELOAD_INTERVAL_HIGH =
        Setting.timeSetting("resource.reload.interval.high", Frequency.HIGH.interval, Property.NodeScope);
    public static final Setting<TimeValue> RELOAD_INTERVAL_MEDIUM = Setting.timeSetting("resource.reload.interval.medium",
        Setting.timeSetting("resource.reload.interval", Frequency.MEDIUM.interval), Property.NodeScope);
    public static final Setting<TimeValue> RELOAD_INTERVAL_LOW =
        Setting.timeSetting("resource.reload.interval.low", Frequency.LOW.interval, Property.NodeScope);

    private final boolean enabled;
    private final ThreadPool threadPool;

    final ResourceMonitor lowMonitor;
    final ResourceMonitor mediumMonitor;
    final ResourceMonitor highMonitor;

    private volatile Cancellable lowFuture;
    private volatile Cancellable mediumFuture;
    private volatile Cancellable highFuture;

    @Inject
    public ResourceWatcherService(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.enabled = ENABLED.get(settings);
        this.threadPool = threadPool;

        TimeValue interval = RELOAD_INTERVAL_LOW.get(settings);
        lowMonitor = new ResourceMonitor(interval, Frequency.LOW);
        interval = RELOAD_INTERVAL_MEDIUM.get(settings);
        mediumMonitor = new ResourceMonitor(interval, Frequency.MEDIUM);
        interval = RELOAD_INTERVAL_HIGH.get(settings);
        highMonitor = new ResourceMonitor(interval, Frequency.HIGH);

        logRemovedSetting("watcher.enabled", "resource.reload.enabled");
        logRemovedSetting("watcher.interval", "resource.reload.interval");
        logRemovedSetting("watcher.interval.low", "resource.reload.interval.low");
        logRemovedSetting("watcher.interval.medium", "resource.reload.interval.medium");
        logRemovedSetting("watcher.interval.high", "resource.reload.interval.high");
    }

    @Override
    protected void doStart() {
        if (!enabled) {
            return;
        }
        lowFuture = threadPool.scheduleWithFixedDelay(lowMonitor, lowMonitor.interval, Names.SAME);
        mediumFuture = threadPool.scheduleWithFixedDelay(mediumMonitor, mediumMonitor.interval, Names.SAME);
        highFuture = threadPool.scheduleWithFixedDelay(highMonitor, highMonitor.interval, Names.SAME);
    }

    @Override
    protected void doStop() {
        if (!enabled) {
            return;
        }
        lowFuture.cancel();
        mediumFuture.cancel();
        highFuture.cancel();
    }

    @Override
    protected void doClose() {
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
        switch (frequency) {
            case LOW:
                return lowMonitor.add(watcher);
            case MEDIUM:
                return mediumMonitor.add(watcher);
            case HIGH:
                return highMonitor.add(watcher);
            default:
                throw new IllegalArgumentException("Unknown frequency [" + frequency + "]");
        }
    }

    public void notifyNow() {
        notifyNow(Frequency.MEDIUM);
    }

    public void notifyNow(Frequency frequency) {
        switch (frequency) {
            case LOW:
                lowMonitor.run();
                break;
            case MEDIUM:
                mediumMonitor.run();
                break;
            case HIGH:
                highMonitor.run();
                break;
            default:
                throw new IllegalArgumentException("Unknown frequency [" + frequency + "]");
        }
    }

    class ResourceMonitor implements Runnable {

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
            for(ResourceWatcher watcher : watchers) {
                try {
                    watcher.checkAndNotify();
                } catch (IOException e) {
                    logger.trace("failed to check resource watcher", e);
                }
            }
        }
    }
}
