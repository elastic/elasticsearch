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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 * Generic resource watcher service
 *
 * Other elasticsearch services can register their resource watchers with this service using {@link #add(ResourceWatcher)}
 * method. This service will call {@link org.elasticsearch.watcher.ResourceWatcher#checkAndNotify()} method of all
 * registered watcher periodically. The frequency of checks can be specified using {@code watcher.interval} setting, which
 * defaults to {@code 60s}. The service can be disabled by setting {@code watcher.enabled} setting to {@code false}.
 */
public class ResourceWatcherService extends AbstractLifecycleComponent<ResourceWatcherService> {

    private final List<ResourceWatcher> watchers = new CopyOnWriteArrayList<>();

    private volatile ScheduledFuture scheduledFuture;

    private final boolean enabled;

    private final TimeValue interval;

    private final ThreadPool threadPool;

    @Inject
    public ResourceWatcherService(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.enabled = componentSettings.getAsBoolean("enabled", true);
        this.interval = componentSettings.getAsTime("interval", timeValueSeconds(60));
        this.threadPool = threadPool;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (!enabled) {
            return;
        }
        scheduledFuture = threadPool.scheduleWithFixedDelay(new ResourceMonitor(), interval);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        if (!enabled) {
            return;
        }
        scheduledFuture.cancel(true);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    /**
     * Register new resource watcher
     */
    public void add(ResourceWatcher watcher) {
        watcher.init();
        watchers.add(watcher);
    }

    /**
     * Unregister a resource watcher
     */
    public void remove(ResourceWatcher watcher) {
        watchers.remove(watcher);
    }

    private class ResourceMonitor implements Runnable {

        @Override
        public void run() {
            for(ResourceWatcher watcher : watchers) {
                watcher.checkAndNotify();
            }
        }
    }
}
