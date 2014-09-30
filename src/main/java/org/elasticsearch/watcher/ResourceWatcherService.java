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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledFuture;

/**
 * Generic resource watcher service
 *
 * Other elasticsearch services can register their resource watchers with this service using {@link #add(ResourceWatcher)}
 * method. This service will call {@link org.elasticsearch.watcher.ResourceWatcher#checkAndNotify()} method of all
 * registered watcher periodically. The frequency of checks can be specified using {@code watcher.interval} setting, which
 * defaults to {@code 60s}. The service can be disabled by setting {@code watcher.enabled} setting to {@code false}.
 */
public class ResourceWatcherService extends AbstractLifecycleComponent<ResourceWatcherService> {

    public static enum Frequency {

        /**
         * Defaults to 5 seconds
         */
        HIGH(TimeValue.timeValueSeconds(5)),

        /**
         * Defaults to 30 seconds
         */
        MEDIUM(TimeValue.timeValueSeconds(25)),

        /**
         * Defaults to 60 seconds
         */
        LOW(TimeValue.timeValueSeconds(60)),

        /**
         * Allows the setting of custom frequencies
         */
        CUSTOM(TimeValue.timeValueSeconds(-1));

        final TimeValue interval;

        private Frequency(TimeValue interval) {
            this.interval = interval;
        }
    }

    final Map<TimeValue, ResourceMonitor> customMonitors = new HashMap<>();
    private final Map<TimeValue, ScheduledFuture> customFutures = new HashMap<>();

    private final boolean enabled;
    private final ThreadPool threadPool;

    final ResourceMonitor lowMonitor;
    final ResourceMonitor mediumMonitor;
    final ResourceMonitor highMonitor;

    private volatile ScheduledFuture lowFuture;
    private volatile ScheduledFuture mediumFuture;
    private volatile ScheduledFuture highFuture;

    @Inject
    public ResourceWatcherService(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.enabled = componentSettings.getAsBoolean("enabled", true);
        this.threadPool = threadPool;

        TimeValue interval = componentSettings.getAsTime("interval.low", Frequency.LOW.interval);
        lowMonitor = new ResourceMonitor(interval, Frequency.LOW);
        interval = componentSettings.getAsTime("interval.medium", componentSettings.getAsTime("interval", Frequency.MEDIUM.interval));
        mediumMonitor = new ResourceMonitor(interval, Frequency.MEDIUM);
        interval = componentSettings.getAsTime("interval.high", Frequency.HIGH.interval);
        highMonitor = new ResourceMonitor(interval, Frequency.HIGH);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (!enabled) {
            return;
        }
        lowFuture = threadPool.scheduleWithFixedDelay(lowMonitor, lowMonitor.interval);
        mediumFuture = threadPool.scheduleWithFixedDelay(mediumMonitor, mediumMonitor.interval);
        highFuture = threadPool.scheduleWithFixedDelay(highMonitor, highMonitor.interval);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        if (!enabled) {
            return;
        }
        lowFuture.cancel(true);
        mediumFuture.cancel(true);
        highFuture.cancel(true);
        synchronized (customFutures) {
            for (Map.Entry<TimeValue, ScheduledFuture> customEntry : customFutures.entrySet()) {
                customEntry.getValue().cancel(true);
            }
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    /**
     * Register new resource watcher that will be checked in default {@link Frequency#MEDIUM MEDIUM} frequency
     */
    public <W extends ResourceWatcher> WatcherHandle<W> add(W watcher) {
        return add(watcher, Frequency.MEDIUM);
    }

    public <W extends ResourceWatcher> WatcherHandle<W> add(W watcher, Frequency frequency) {
        return add(watcher,frequency,null);
    }
    /**
     * Register new resource watcher that will be checked in the given frequency
     */
    public <W extends ResourceWatcher> WatcherHandle<W> add(W watcher, Frequency frequency, @Nullable TimeValue customTimePeriod) {
        watcher.init();
        switch (frequency) {
            case LOW:
                return lowMonitor.add(watcher);
            case MEDIUM:
                return mediumMonitor.add(watcher);
            case HIGH:
                return highMonitor.add(watcher);
            case CUSTOM:
                if (customTimePeriod == null) {
                    throw new ElasticsearchIllegalArgumentException("For custom time frequency customTimePeriod cannot be null");
                }
                return addCustomWatcher(watcher, customTimePeriod );
            default:
                throw new ElasticsearchIllegalArgumentException("Unknown frequency [" + frequency + "]");
        }
    }

    public <W extends ResourceWatcher> boolean removeWatcher(W watcher, Frequency frequency) {
        switch (frequency) {
            case LOW:
                return lowMonitor.remove(watcher);
            case MEDIUM:
                return mediumMonitor.remove(watcher);
            case HIGH:
                return highMonitor.remove(watcher);
            case CUSTOM:
                synchronized (customMonitors) {
                    TimeValue foundAtTimeValue = null;
                    boolean isEmpty = false;
                    for (Map.Entry<TimeValue, ResourceMonitor> monitorEntry : customMonitors.entrySet()) {
                        if (monitorEntry.getValue().remove(watcher)) {
                            foundAtTimeValue = monitorEntry.getKey();
                            isEmpty = monitorEntry.getValue().isEmpty();
                            break;
                        }
                    }
                    if (foundAtTimeValue == null) {
                        return false; //We didn't find it in any of the custom time interval watchers
                    }
                    if (!isEmpty) {
                        return true;
                    }
                    //This was the only monitor at this custom interval we can cancel the future and remove this timeperiod
                    //from the maps
                    synchronized (customFutures) {
                        customFutures.get(foundAtTimeValue).cancel(true);
                        customFutures.remove(foundAtTimeValue);
                    }
                    customMonitors.remove(foundAtTimeValue);
                    return true;
                }
            default:
                throw new ElasticsearchIllegalArgumentException("Unknown frequency [" + frequency + "]");
        }
    }

    private <W extends ResourceWatcher> WatcherHandle<W> addCustomWatcher(W watcher, TimeValue customInterval) {
        synchronized (customMonitors) {
            if (customMonitors.containsKey(customInterval)) {
                return customMonitors.get(customInterval).add(watcher);
            } else {
                ResourceMonitor monitor = new ResourceMonitor(customInterval, Frequency.CUSTOM);
                customMonitors.put(customInterval, monitor);
                synchronized (customFutures) {
                    customFutures.put(customInterval, threadPool.scheduleWithFixedDelay(monitor, customInterval));
                }
                return monitor.add(watcher);
            }
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

        private boolean isEmpty() {
            return watchers.isEmpty();
        }

        private <W extends ResourceWatcher> WatcherHandle<W> add(W watcher) {
            watchers.add(watcher);
            return new WatcherHandle<>(this, watcher);
        }

        private <W extends ResourceWatcher> boolean remove(W watcher) {
            return watchers.remove(watcher);
        }

        @Override
        public void run() {
            for(ResourceWatcher watcher : watchers) {
                watcher.checkAndNotify();
            }
        }
    }
}
