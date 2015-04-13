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

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class ResourceWatcherServiceTests extends ElasticsearchTestCase {

    @Test
    public void testSettings() throws Exception {
        ThreadPool threadPool = new ThreadPool("test");

        // checking the defaults
        Settings settings = ImmutableSettings.builder().build();
        ResourceWatcherService service = new ResourceWatcherService(settings, threadPool);
        assertThat(service.highMonitor.interval, is(ResourceWatcherService.Frequency.HIGH.interval));
        assertThat(service.mediumMonitor.interval, is(ResourceWatcherService.Frequency.MEDIUM.interval));
        assertThat(service.lowMonitor.interval, is(ResourceWatcherService.Frequency.LOW.interval));

        // checking bwc
        settings = ImmutableSettings.builder()
                .put("watcher.interval", "40s") // only applies to medium
                .build();
        service = new ResourceWatcherService(settings, threadPool);
        assertThat(service.highMonitor.interval.millis(), is(timeValueSeconds(5).millis()));
        assertThat(service.mediumMonitor.interval.millis(), is(timeValueSeconds(40).millis()));
        assertThat(service.lowMonitor.interval.millis(), is(timeValueSeconds(60).millis()));

        // checking custom
        settings = ImmutableSettings.builder()
                .put("watcher.interval.high", "10s")
                .put("watcher.interval.medium", "20s")
                .put("watcher.interval.low", "30s")
                .build();
        service = new ResourceWatcherService(settings, threadPool);
        assertThat(service.highMonitor.interval.millis(), is(timeValueSeconds(10).millis()));
        assertThat(service.mediumMonitor.interval.millis(), is(timeValueSeconds(20).millis()));
        assertThat(service.lowMonitor.interval.millis(), is(timeValueSeconds(30).millis()));
        terminate(threadPool);
    }


    @Test
    public void testHandle() throws Exception {
        ThreadPool threadPool = new ThreadPool("test");
        Settings settings = ImmutableSettings.builder().build();
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
