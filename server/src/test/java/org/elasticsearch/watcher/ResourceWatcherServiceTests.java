/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.watcher;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ResourceWatcherServiceTests extends ESTestCase {
    public void testSettings() throws Exception {
        ThreadPool threadPool = new TestThreadPool("test");

        // checking the defaults
        Settings settings = Settings.builder().build();
        ResourceWatcherService service = new ResourceWatcherService(settings, threadPool);
        assertThat(service.highMonitor.interval, is(ResourceWatcherService.Frequency.HIGH.interval));
        assertThat(service.mediumMonitor.interval, is(ResourceWatcherService.Frequency.MEDIUM.interval));
        assertThat(service.lowMonitor.interval, is(ResourceWatcherService.Frequency.LOW.interval));

        // checking bwc
        settings = Settings.builder()
                .put("resource.reload.interval", "40s") // only applies to medium
                .build();
        service = new ResourceWatcherService(settings, threadPool);
        assertThat(service.highMonitor.interval.millis(), is(timeValueSeconds(5).millis()));
        assertThat(service.mediumMonitor.interval.millis(), is(timeValueSeconds(40).millis()));
        assertThat(service.lowMonitor.interval.millis(), is(timeValueSeconds(60).millis()));

        // checking custom
        settings = Settings.builder()
                .put("resource.reload.interval.high", "10s")
                .put("resource.reload.interval.medium", "20s")
                .put("resource.reload.interval.low", "30s")
                .build();
        service = new ResourceWatcherService(settings, threadPool);
        assertThat(service.highMonitor.interval.millis(), is(timeValueSeconds(10).millis()));
        assertThat(service.mediumMonitor.interval.millis(), is(timeValueSeconds(20).millis()));
        assertThat(service.lowMonitor.interval.millis(), is(timeValueSeconds(30).millis()));
        terminate(threadPool);
    }

    public void testHandle() throws Exception {
        ThreadPool threadPool = new TestThreadPool("test");
        Settings settings = Settings.builder().build();
        ResourceWatcherService service = new ResourceWatcherService(settings, threadPool);
        ResourceWatcher watcher = new ResourceWatcher() {
            @Override
            public void init() {
            }

            @Override
            public void checkAndNotify() {
            }
        };

        // checking default freq
        WatcherHandle handle = service.add(watcher);
        assertThat(handle, notNullValue());
        assertThat(handle.frequency(), equalTo(ResourceWatcherService.Frequency.MEDIUM));
        assertThat(service.lowMonitor.watchers.size(), is(0));
        assertThat(service.highMonitor.watchers.size(), is(0));
        assertThat(service.mediumMonitor.watchers.size(), is(1));
        handle.stop();
        assertThat(service.mediumMonitor.watchers.size(), is(0));
        handle.resume();
        assertThat(service.mediumMonitor.watchers.size(), is(1));
        handle.stop();

        // checking custom freq
        handle = service.add(watcher, ResourceWatcherService.Frequency.HIGH);
        assertThat(handle, notNullValue());
        assertThat(handle.frequency(), equalTo(ResourceWatcherService.Frequency.HIGH));
        assertThat(service.lowMonitor.watchers.size(), is(0));
        assertThat(service.mediumMonitor.watchers.size(), is(0));
        assertThat(service.highMonitor.watchers.size(), is(1));
        handle.stop();
        assertThat(service.highMonitor.watchers.size(), is(0));
        handle.resume();
        assertThat(service.highMonitor.watchers.size(), is(1));
        terminate(threadPool);
    }
}
