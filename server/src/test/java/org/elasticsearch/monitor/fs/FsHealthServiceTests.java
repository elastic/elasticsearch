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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
            while (deterministicTaskQueue.hasDeferredTasks()) {
                deterministicTaskQueue.advanceTime();
                deterministicTaskQueue.runAllRunnableTasks();
            }
            // no tasks post service stop
            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertFalse(deterministicTaskQueue.hasDeferredTasks());
        }
    }

    public void testFailsHealthOnIOException() throws IOException {
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

            //disrupt file system
            disruptFileSystemProvider.injectIOException.set(true);
            fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.UNHEALTHY);
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
            disruptFileSystemProvider.injectIOException.set(false);
        }
    }

    public void testFailsHealthOnSinglePathIOException() throws IOException {
        FileSystem current = PathUtils.getDefaultFileSystem();
        FileSystemIOExceptionProvider disruptFileSystemProvider = new FileSystemIOExceptionProvider(current);
        PathUtilsForTesting.installMock(disruptFileSystemProvider.getFileSystem(null));
        final Settings settings = Settings.EMPTY;
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        try (NodeEnvironment env = newNodeEnvironment()) {
            Path[] paths = env.nodeDataPaths();
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.HEALTHY);

            //disrupt file system on single path
            disruptFileSystemProvider.injectIOException.set(true);
            disruptFileSystemProvider.restrictPathPrefix(randomFrom(paths).toString());
            fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(fsHealthService.getHealth(), NodeHealthService.Status.UNHEALTHY);
            assertEquals(disruptFileSystemProvider.getInjectedPathCount(), 1);

        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
            disruptFileSystemProvider.injectIOException.set(false);
        }
    }

    private static class FileSystemIOExceptionProvider extends FilterFileSystemProvider {

        AtomicBoolean injectIOException = new AtomicBoolean();
        AtomicInteger injectedPaths = new AtomicInteger();

        private String pathPrefix = "/";

        FileSystemIOExceptionProvider(FileSystem inner) {
            super("disrupt_fs_health://", inner);
        }

        public void restrictPathPrefix(String pathPrefix){
            this.pathPrefix = pathPrefix;
        }

        public int getInjectedPathCount(){
            return injectedPaths.get();
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            if (injectIOException.get()){
                if (path.toString().startsWith(pathPrefix) && path.toString().endsWith(".es_temp_file")) {
                    injectedPaths.incrementAndGet();
                    throw new IOException("fake IOException");
                }
            }
            return super.newFileChannel(path, options, attrs);
        }
    }
}
