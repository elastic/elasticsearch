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
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.test.ESTestCase;
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

    public void testSchedulesHealthCheckAndSlowPathLoggingPeriodically() throws Exception {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(Settings.EMPTY, clusterSettings, deterministicTaskQueue.getThreadPool(), env);
            fsHealthService.doStart();
            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertTrue(deterministicTaskQueue.hasDeferredTasks());

            //schedules the first health check at refresh interval
            assertEquals(deterministicTaskQueue.getLatestDeferredExecutionTime(),
                FsHealthService.REFRESH_INTERVAL_SETTING.get(Settings.EMPTY).millis());
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();

            //schedules the next health check at refresh interval
            assertEquals(deterministicTaskQueue.getNextDeferredExecutionTime(),
                (FsHealthService.REFRESH_INTERVAL_SETTING.get(Settings.EMPTY).millis()) * 2);

            //We schedule a slow logging timeout handler per path
            assertEquals(deterministicTaskQueue.getDeferredTasks().size(), env.nodeDataPaths().length + 1);

            //Verify schedules slow path logging at the timeout
            assertEquals(deterministicTaskQueue.getLatestDeferredExecutionTime(),
                (FsHealthService.REFRESH_INTERVAL_SETTING.get(Settings.EMPTY).millis() +
                    FsHealthService.SLOW_PATH_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()));

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();

            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertTrue(deterministicTaskQueue.hasDeferredTasks());

            fsHealthService.doStop();
            // run deferred tasks
            while(deterministicTaskQueue.hasDeferredTasks()) {
                deterministicTaskQueue.advanceTime();
                deterministicTaskQueue.runAllRunnableTasks();
            }
            // no tasks post service stop
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
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.HEALTHY);

            //disrupt File system
            disruptFileSystemProvider.injectIOException.set(true);
            fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.UNHEALTHY);
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
            disruptFileSystemProvider.injectIOException.set(false);
        }
    }

    public void testFailsHealthOnSlowIO() throws IOException{
        long refreshInterval = randomLongBetween(50, 60);
        long healthyTimeout = randomLongBetween(100, 150);
        long unhealthyTimeout = randomLongBetween(50, 60);
        long hangDuration = healthyTimeout + randomLongBetween(10, 15);
        logger.warn("Healthy :{} Unhealthy : {}, hang {}", healthyTimeout, unhealthyTimeout, hangDuration);
        FileSystem current = PathUtils.getDefaultFileSystem();
        FileSystemIOHangProvider disruptFileSystemProvider = new FileSystemIOHangProvider(current, healthyTimeout + hangDuration);
        PathUtilsForTesting.installMock(disruptFileSystemProvider.getFileSystem(null));
        final Settings settings = Settings.builder().put(FsHealthService.HEALTHY_TIMEOUT_SETTING.getKey(), healthyTimeout + "ms")
            .put(FsHealthService.REFRESH_INTERVAL_SETTING.getKey(), refreshInterval + "ms")
            .put(FsHealthService.UNHEALTHY_TIMEOUT_SETTING.getKey(), unhealthyTimeout + "ms")
            .put(FsHealthService.SLOW_PATH_LOGGING_THRESHOLD_SETTING.getKey(), healthyTimeout - randomLongBetween(2, 5) + "ms")
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0).build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.HEALTHY);

            //add delay to fs response
            disruptFileSystemProvider.injectIOHang.set(true);
            fsHealthService.new FsHealthMonitor().run();

            //After system recovers from hung state node is UNHEALTHY
            Thread.sleep(healthyTimeout);
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.UNHEALTHY);

            //after hang recovers at this point checking back if the health is still UNHEALTHY
            Thread.sleep(hangDuration - healthyTimeout);
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.UNHEALTHY);

            //shorten the delay to greater than unhealthy but less than healthy, still UNHEALTHY
            disruptFileSystemProvider.setDelay(randomLongBetween(healthyTimeout - 20 , healthyTimeout - 10));
            fsHealthService.new FsHealthMonitor().run();
            Thread.sleep(healthyTimeout - randomLongBetween(5, 10));
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.UNHEALTHY);

            //shorten the delay to less than unhealthy, now HEALTHY
            disruptFileSystemProvider.setDelay(randomLongBetween(unhealthyTimeout - 20 , unhealthyTimeout - 10));
            fsHealthService.new FsHealthMonitor().run();
            Thread.sleep(healthyTimeout - randomLongBetween(5, 10));
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.HEALTHY);

        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            disruptFileSystemProvider.injectIOHang.set(false);
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
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

        void setDelay(long delay){
            this.delay = delay;
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            if (injectIOHang.get()) {
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
