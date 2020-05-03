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


import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;


public class FsHealthServiceTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
    }

    public void testReturnsUnhealthyAfterTimeout() throws Exception {
        final Settings settings = Settings.EMPTY;
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, deterministicTaskQueue.getThreadPool(), env);
            fsHealthService.new FsHealthMonitor().run();
            deterministicTaskQueue.scheduleAt(randomLongBetween(
                FsHealthService.HEALTHY_TIMEOUT_SETTING.get(settings).millis()
                    + FsHealthService.REFRESH_INTERVAL_SETTING.get(settings).millis() + 1, 10000), () -> {});
            deterministicTaskQueue.advanceTime();
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.UNHEALTHY);
        }
    }

    public void testSchedulesFsHealthMonitor() throws IOException {
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

    public void testSchedulesHealthCheckAtRefreshIntervals() throws Exception {
        long refreshInterval = randomLongBetween(1000, 12000);
        final Settings settings = Settings.builder().put(FsHealthService.REFRESH_INTERVAL_SETTING.getKey(), refreshInterval + "ms").build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, deterministicTaskQueue.getThreadPool(), env);
            fsHealthService.doStart();
            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertTrue(deterministicTaskQueue.hasDeferredTasks());
            for (int i = 1; i <= randomIntBetween(1, 10); i++) {
                assertEquals(deterministicTaskQueue.getLatestDeferredExecutionTime(), refreshInterval * i);
                deterministicTaskQueue.advanceTime();
                deterministicTaskQueue.runAllRunnableTasks();
            }
            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertTrue(deterministicTaskQueue.hasDeferredTasks());

            fsHealthService.doStop();
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();

            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertFalse(deterministicTaskQueue.hasDeferredTasks());
        }
    }

    public void testFailsHealthOnIOException() throws IOException{
        FileSystem current = PathUtils.getDefaultFileSystem();
        FileSystemIOExceptionProvider disruptFileSystemProvider = new FileSystemIOExceptionProvider(current);
        PathUtilsForTesting.installMock(disruptFileSystemProvider.getFileSystem(null));
        final Settings settings = Settings.EMPTY;
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, deterministicTaskQueue.getThreadPool(), env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.HEALTHY);

            //disrupt File system
            disruptFileSystemProvider.injectIOException.set(true);
            fsHealthService = new FsHealthService(settings, clusterSettings, deterministicTaskQueue.getThreadPool(), env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.UNHEALTHY);
        } finally {
            disruptFileSystemProvider.injectIOException.set(false);
        }
    }

    public void testFailsHealthOnIOHang() throws IOException{
        long refreshInterval = randomLongBetween(10, 20);
        long healthCheckTimeout = randomLongBetween(50, 60);
        int iteration = randomIntBetween(20, 50);
        AtomicLong disruptionStartTime = new AtomicLong();
        AtomicInteger counter = new AtomicInteger();
        AtomicBoolean isHealthy = new AtomicBoolean();
        CountDownLatch latch = new CountDownLatch(iteration);
        final Settings settings = Settings.builder().put(FsHealthService.HEALTHY_TIMEOUT_SETTING.getKey(), healthCheckTimeout + "ms")
            .put(FsHealthService.REFRESH_INTERVAL_SETTING.getKey(), refreshInterval + "ms")
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0).build();
        FileSystem current = PathUtils.getDefaultFileSystem();
        FileSystemIOHangProvider disruptFileSystemProvider = new FileSystemIOHangProvider(current,
            randomLongBetween(refreshInterval + 1000, 5000));
        PathUtilsForTesting.installMock(disruptFileSystemProvider.getFileSystem(null));
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ScheduledExecutorService healthCheckThreadpool = Executors.newScheduledThreadPool(1);
        final ThreadPool testThreadpool = new TestThreadPool(getClass().getName(), settings);
        try (NodeEnvironment env = newNodeEnvironment()) {
            final FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadpool, env);

            fsHealthService.doStart();
            healthCheckThreadpool.scheduleAtFixedRate(() -> {
                latch.countDown();
                isHealthy.set(fsHealthService.getHealth() == NodeHealthService.Status.HEALTHY);
                logger.debug("Reported health is : {}", isHealthy.get());
                //disrupt IO half way through
                if (counter.getAndIncrement() == iteration/2) {
                    disruptionStartTime.set(testThreadpool.relativeTimeInMillis());
                    disruptFileSystemProvider.injectIOHang.compareAndSet(false, true);
                }
                // healthy before disruption
                if (disruptFileSystemProvider.injectIOHang.get() == false) {
                    assertTrue(isHealthy.get());
                }
                //unhealthy after disruption and then the timeout interval
                if (disruptFileSystemProvider.injectIOHang.get() && testThreadpool.relativeTimeInMillis() - disruptionStartTime.get() >
                    (refreshInterval + healthCheckTimeout)) {
                    assertFalse(isHealthy.get());
                }
            }, 50, 10, TimeUnit.MILLISECONDS);
            latch.await(1000, TimeUnit.MILLISECONDS);
            assertEquals(latch.getCount(),0);
            fsHealthService.doStop();

        } catch (InterruptedException ex){
            Thread.currentThread().interrupt();
        } finally {
            ThreadPool.terminate(healthCheckThreadpool, 500, TimeUnit.MILLISECONDS);
            ThreadPool.terminate(testThreadpool, 500, TimeUnit.MILLISECONDS);
            disruptFileSystemProvider.injectIOHang.set(false);
        }
    }

    private static void execute(NodeEnvironment env, Settings settings, FsHealthServiceTests.TriFunction<Runnable, TimeValue, String,
        Scheduler.Cancellable> scheduler,  Runnable asserts)  {
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(FsHealthServiceTests.class.getCanonicalName()) {
                @Override
                public Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String name) {
                    assertEquals(name, Names.GENERIC);
                    assertEquals(interval, FsHealthService.REFRESH_INTERVAL_SETTING.get(settings));
                    return scheduler.apply(command, interval, name);
                }
            };
            FsHealthService service = new FsHealthService(settings, clusterSettings, threadPool,  env);
            service.doStart();
            asserts.run();
            service.doStop();

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

    private static class FileSystemIOExceptionProvider extends FilterFileSystemProvider {

        AtomicBoolean injectIOException = new AtomicBoolean();

        FileSystemIOExceptionProvider(FileSystem inner) {
            super("disrupt_fs_health://", inner);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            if (injectIOException.get() && path.toString().endsWith("es_temp_file")) {
                throw new IOException("fake IOException");
            }
            return super.newFileChannel(path, options, attrs);
        }
    }

    private static class FileSystemIOHangProvider extends FilterFileSystemProvider {

        AtomicBoolean injectIOHang = new AtomicBoolean();
        private long delay;

        FileSystemIOHangProvider(FileSystem inner, long delay) {
            super("disrupt_fs_health://", inner);
            this.delay = delay;
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            if(injectIOHang.get()){
                try {
                    Thread.sleep(delay);
                } catch(InterruptedException ex){
                    Thread.currentThread().interrupt();
                }
            }
            return super.newFileChannel(path, options, attrs);
        }
    }

}
