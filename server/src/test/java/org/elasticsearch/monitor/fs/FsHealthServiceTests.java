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

package org.elasticsearch.monitor.fs;


import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.mockito.Mockito.mock;


public class FsHealthServiceTests extends ESTestCase {

    public void testIsPathWritable() throws Exception {
        Map<Path, FsHealthService.TimeStampedStatus> pathHealthStats = new HashMap<>();
        try (NodeEnvironment env = newNodeEnvironment()) {
            pathHealthStats.put(env.nodeDataPaths()[0], new FsHealthService.TimeStampedStatus(FsHealthService.Status.HEALTHY));
            final Settings settings = Settings.EMPTY;
            final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, null,
                env, ()  ->  1){
                @Override
                Map<Path, TimeStampedStatus> getPathHealthStats() {
                    return pathHealthStats;
                }
            };
            assertTrue(fsHealthService.isWritable(env.nodeDataPaths()[0]));
            assertNull(fsHealthService.isWritable(mock(Path.class)));

            pathHealthStats.put(env.nodeDataPaths()[0], new FsHealthService.TimeStampedStatus(FsHealthService.Status.UNHEALTHY));
            fsHealthService = new FsHealthService(settings, clusterSettings, null,
                env, ()  ->  1){
                @Override
                Map<Path, TimeStampedStatus> getPathHealthStats() {
                    return pathHealthStats;
                }
            };
            assertFalse(fsHealthService.isWritable(env.nodeDataPaths()[0]));
            assertNull(fsHealthService.isWritable(mock(Path.class)));
        }
    }

    public void testIsPathWritableOnStaleHealth() throws Exception {
        Map<Path, FsHealthService.TimeStampedStatus> pathHealthStats = new HashMap<>();
        try (NodeEnvironment env = newNodeEnvironment()) {
            pathHealthStats.put(env.nodeDataPaths()[0], new FsHealthService.TimeStampedStatus(FsHealthService.Status.HEALTHY));
            final Settings settings = Settings.builder().put(FsHealthService.HEALTHY_TIMEOUT_SETTING.getKey(), 1000 + "ms").build();;
            final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            LongSupplier currentTimeSupplier = () -> System.currentTimeMillis() + randomLongBetween(1001, 10000);
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, null,
                env, currentTimeSupplier){
                @Override
                Map<Path, TimeStampedStatus> getPathHealthStats() {
                    return pathHealthStats;
                }
            };
            assertFalse(fsHealthService.isWritable(env.nodeDataPaths()[0]));
            assertNull(fsHealthService.isWritable(mock(Path.class)));

            pathHealthStats.put(env.nodeDataPaths()[0], new FsHealthService.TimeStampedStatus(FsHealthService.Status.UNHEALTHY));
            fsHealthService = new FsHealthService(settings, clusterSettings, null,
                env, currentTimeSupplier){
                @Override
                Map<Path, TimeStampedStatus> getPathHealthStats() {
                    return pathHealthStats;
                }
            };
            assertFalse(fsHealthService.isWritable(env.nodeDataPaths()[0]));
            assertNull(fsHealthService.isWritable(mock(Path.class)));
        }
    }

    public void testPathHealthMonitorSchedule() throws IOException {
        NodeEnvironment env = newNodeEnvironment();
        AtomicBoolean scheduled = new AtomicBoolean();
        Settings settings = Settings.builder().put(FsHealthService.REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(1000))
            .build();
        execute(env, settings, (command, interval, name) -> {
            scheduled.set(true);
            return new FsHealthServiceTests.MockCancellable();
        },  () -> assertTrue(scheduled.get()));
        env.close();
    }

    private static void execute(NodeEnvironment env, Settings settings, FsHealthServiceTests.TriFunction<Runnable, TimeValue, String,
        Scheduler.Cancellable> scheduler,  Runnable asserts)  {
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ThreadPool threadPool = null;
        Set<Path> paths = new TreeSet<>();
        try {
            threadPool = new TestThreadPool(FsHealthServiceTests.class.getCanonicalName()) {
                @Override
                public Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String name) {
                    paths.add(((FsHealthService.FsPathHealthMonitor) command).getPath());
                    assertEquals(name, Names.GENERIC);
                    assertEquals(interval, FsHealthService.REFRESH_INTERVAL_SETTING.get(settings));
                    return scheduler.apply(command, interval, name);
                }
            };
            FsHealthService service = new FsHealthService(settings, clusterSettings, threadPool, env, () -> 1);
            service.doStart();
            asserts.run();
            service.doStop();
            assertArrayEquals(env.nodeDataPaths(), paths.toArray());

        } finally {
            ThreadPool.terminate(threadPool, 300, TimeUnit.MILLISECONDS);
        }
    }

    interface TriFunction<S, T, U, R> {
        R apply(S s, T t, U u);
    }

    private static class MockCancellable implements Scheduler.Cancellable {

        @Override
        public boolean cancel() {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
    }
}
