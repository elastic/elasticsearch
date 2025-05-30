/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.engine.ThreadPoolMergeExecutorServiceTests.getThreadPoolMergeExecutorService;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.ABORT;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.BACKLOG;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.RUN;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThreadPoolMergeExecutorServiceDiskSpaceTests extends ESTestCase {

    private static TestMockFileStore aFileStore = new TestMockFileStore("mocka");
    private static TestMockFileStore bFileStore = new TestMockFileStore("mockb");
    private static String aPathPart;
    private static String bPathPart;

    private Settings settings;

    @BeforeClass
    public static void installMockUsableSpaceFS() {
        FileSystem current = PathUtils.getDefaultFileSystem();
        aPathPart = "a-" + randomUUID();
        bPathPart = "b-" + randomUUID();
        FileSystemProvider mock = new TestMockUsableSpaceFileSystemProvider(current);
        PathUtilsForTesting.installMock(mock.getFileSystem(null));
    }

    @AfterClass
    public static void removeMockUsableSpaceFS() {
        PathUtilsForTesting.teardown();
        aFileStore = null;
        bFileStore = null;
    }

    @Before
    public void setPathsInSettings() {
        Path path = PathUtils.get(createTempDir().toString());
        // use 2 data paths
        String[] paths = new String[] { path.resolve(aPathPart).toString(), path.resolve(bPathPart).toString() };
        this.settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), path)
            .putList(Environment.PATH_DATA_SETTING.getKey(), paths)
            .build();
    }

    static class TestMockUsableSpaceFileSystemProvider extends FilterFileSystemProvider {

        TestMockUsableSpaceFileSystemProvider(FileSystem inner) {
            super("mockusablespace://", inner);
        }

        @Override
        public FileStore getFileStore(Path path) throws IOException {
            if (path.toString().contains(path.getFileSystem().getSeparator() + aPathPart)) {
                return aFileStore;
            } else {
                assert path.toString().contains(path.getFileSystem().getSeparator() + bPathPart);
                return bFileStore;
            }
        }
    }

    static class TestMockFileStore extends FileStore {

        public long totalSpace;
        public long freeSpace;
        public long usableSpace;

        private final String desc;

        TestMockFileStore(String desc) {
            this.desc = desc;
        }

        @Override
        public String type() {
            return "mock";
        }

        @Override
        public String name() {
            return desc;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public long getTotalSpace() {
            return totalSpace;
        }

        @Override
        public long getUnallocatedSpace() {
            return freeSpace;
        }

        @Override
        public long getUsableSpace() {
            return usableSpace;
        }

        @Override
        public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
            return false;
        }

        @Override
        public boolean supportsFileAttributeView(String name) {
            return false;
        }

        @Override
        public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
            return null;
        }

        @Override
        public Object getAttribute(String attribute) {
            return null;
        }
    }

    public void testAvailableDiskSpaceMonitorSingleUpdateWithDefaultSettings() throws Exception {
        Settings settings = Settings.builder()
            .put(this.settings)
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "50ms")
            .build();
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);
        // path "a" has lots of free space, and "b" has little
        aFileStore.usableSpace = 100_000L;
        aFileStore.totalSpace = aFileStore.usableSpace * 2;
        bFileStore.usableSpace = 1_000L;
        bFileStore.totalSpace = bFileStore.usableSpace * 2;
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            try (NodeEnvironment nodeEnv = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                AtomicReference<ByteSizeValue> availableDiskSpaceForMerging = new AtomicReference<>();
                CountDownLatch diskSpaceMonitor = new CountDownLatch(1);
                try (
                    var diskSpacePeriodicMonitor = ThreadPoolMergeExecutorService.startDiskSpaceMonitoring(
                        testThreadPool,
                        nodeEnv.dataPaths(),
                        clusterSettings,
                        (availableDiskSpace) -> {
                            availableDiskSpaceForMerging.set(availableDiskSpace);
                            diskSpaceMonitor.countDown();
                        }
                    )
                ) {
                    safeAwait(diskSpaceMonitor);
                }
                // 100_000 (available) - 5% (default flood stage level) * 200_000 (total space)
                assertThat(availableDiskSpaceForMerging.get().getBytes(), is(90_000L));
            }
        }
    }

    public void testAvailableDiskSpaceMonitorSettingsUpdate() throws Exception {
        Settings settings = Settings.builder()
            .put(this.settings)
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "50ms")
            .build();
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);
        // path "b" has more usable (available) space, but path "a" has more total space
        aFileStore.usableSpace = 900_000L;
        aFileStore.totalSpace = 1_200_000L;
        bFileStore.usableSpace = 1_000_000L;
        bFileStore.totalSpace = 1_100_000L;
        LinkedHashSet<ByteSizeValue> availableDiskSpaceUpdates = new LinkedHashSet<>();
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            try (NodeEnvironment nodeEnv = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                try (
                    var diskSpacePeriodicMonitor = ThreadPoolMergeExecutorService.startDiskSpaceMonitoring(
                        testThreadPool,
                        nodeEnv.dataPaths(),
                        clusterSettings,
                        (availableDiskSpace) -> {
                            synchronized (availableDiskSpaceUpdates) {
                                availableDiskSpaceUpdates.add(availableDiskSpace);
                            }
                        }
                    )
                ) {
                    assertBusy(() -> {
                        synchronized (availableDiskSpaceUpdates) {
                            assertThat(availableDiskSpaceUpdates.size(), is(1));
                            // 1_000_000 (available) - 5% (default flood stage level) * 1_100_000 (total space)
                            assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(945_000L));
                        }
                    }, 5, TimeUnit.SECONDS);
                    // updated the ration for the watermark
                    clusterSettings.applySettings(
                        Settings.builder()
                            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(), "90%")
                            .build()
                    );
                    assertBusy(() -> {
                        synchronized (availableDiskSpaceUpdates) {
                            assertThat(availableDiskSpaceUpdates.size(), is(2));
                            // 1_000_000 (available) - 10% (indices.merge.disk.watermark.high) * 1_100_000 (total space)
                            assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(890_000L));
                        }
                    }, 5, TimeUnit.SECONDS);
                    // absolute value for the watermark limit
                    clusterSettings.applySettings(
                        Settings.builder()
                            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(), "3000b")
                            .build()
                    );
                    assertBusy(() -> {
                        synchronized (availableDiskSpaceUpdates) {
                            assertThat(availableDiskSpaceUpdates.size(), is(3));
                            // 1_000_000 (available) - 3_000 (indices.merge.disk.watermark.high)
                            assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(997_000L));
                        }
                    }, 5, TimeUnit.SECONDS);
                    // headroom value that takes priority over the watermark
                    clusterSettings.applySettings(
                        Settings.builder()
                            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(), "50%")
                            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING.getKey(), "11111b")
                            .build()
                    );
                    assertBusy(() -> {
                        synchronized (availableDiskSpaceUpdates) {
                            assertThat(availableDiskSpaceUpdates.size(), is(4));
                            // 1_000_000 (available) - 11_111 (indices.merge.disk.watermark.high)
                            assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(988_889L));
                        }
                    }, 5, TimeUnit.SECONDS);
                    // watermark limit that takes priority over the headroom
                    clusterSettings.applySettings(
                        Settings.builder()
                            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(), "98%")
                            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING.getKey(), "22222b")
                            .build()
                    );
                    assertBusy(() -> {
                        synchronized (availableDiskSpaceUpdates) {
                            assertThat(availableDiskSpaceUpdates.size(), is(5));
                            // 1_000_000 (available) - 2% (indices.merge.disk.watermark.high) * 1_100_000 (total space)
                            assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(978_000L));
                        }
                    }, 5, TimeUnit.SECONDS);
                    // headroom takes priority over the default watermark of 95%
                    clusterSettings.applySettings(
                        Settings.builder()
                            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING.getKey(), "22222b")
                            .build()
                    );
                    assertBusy(() -> {
                        synchronized (availableDiskSpaceUpdates) {
                            assertThat(availableDiskSpaceUpdates.size(), is(6));
                            // 1_000_000 (available) - 22_222
                            assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(977_778L));
                        }
                    }, 5, TimeUnit.SECONDS);
                    // watermark from routing allocation takes priority
                    clusterSettings.applySettings(
                        Settings.builder()
                            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "99%")
                            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "2b")
                            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING.getKey(), "22222b")
                            .build()
                    );
                    assertBusy(() -> {
                        synchronized (availableDiskSpaceUpdates) {
                            assertThat(availableDiskSpaceUpdates.size(), is(7));
                            // 1_000_000 (available) - 1% (cluster.routing.allocation.disk.watermark.flood_stage) * 1_100_000 (total space)
                            assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(989_000L));
                        }
                    }, 5, TimeUnit.SECONDS);
                }
            }
        }
    }

    public void testUnavailableBudgetBlocksEnqueuedMergeTasks() throws Exception {
        int mergeExecutorThreadCount = randomIntBetween(5, 10);
        // fewer merge tasks than pool threads so that there's always a free thread available
        int submittedMergesCount = randomIntBetween(1, mergeExecutorThreadCount - 1);
        Settings settings = Settings.builder()
                .put(this.settings)
                .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
                .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
                .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .build();
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);
        aFileStore.totalSpace = 150_000L;
        bFileStore.totalSpace = 140_000L;
        boolean aHasMoreSpace = randomBoolean();
        if (aHasMoreSpace) {
            // "a" has more available space
            aFileStore.usableSpace = 120_000L;
            bFileStore.usableSpace = 100_000L;
        } else {
            // "b" has more available space
            aFileStore.usableSpace = 90_000L;
            bFileStore.usableSpace = 110_000L;
        }
        long smallMergeTaskSize = 10L;
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            try (NodeEnvironment nodeEnv = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                try (
                    ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorService
                        .maybeCreateThreadPoolMergeExecutorService(testThreadPool, clusterSettings, nodeEnv)
                ) {
                    assert threadPoolMergeExecutorService != null;
                    // wait for the budget to be updated from the available disk space
                    AtomicLong availableBudget = new AtomicLong();
                    assertBusy(() -> {
                        if (aHasMoreSpace) {
                            // 120_000L (available) - 5% (default flood stage level) * 150_000L (total)
                            assertThat(threadPoolMergeExecutorService.getMergeTasksQueue().getAvailableBudget(), is(112_500L));
                            availableBudget.set(112_500L);
                        } else {
                            // 110_000L (available) - 5% (default flood stage level) * 140_000L (total)
                            assertThat(threadPoolMergeExecutorService.getMergeTasksQueue().getAvailableBudget(), is(103_000L));
                            availableBudget.set(103_000L);
                        }
                    });
                    // tasks that wait on the latch will hold up the budget
                    CountDownLatch blockMergeTasks = new CountDownLatch(1);
                    // submit merge tasks that don't finish, in order to deplete the available budget
                    while (submittedMergesCount > 0) {
                        ThreadPoolMergeScheduler.MergeTask mergeTask = mock(ThreadPoolMergeScheduler.MergeTask.class);
                        when(mergeTask.supportsIOThrottling()).thenReturn(randomBoolean());
                        doAnswer(mock -> {
                            Schedule schedule = randomFrom(Schedule.values());
                            if (schedule == BACKLOG) {
                                testThreadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                                    // re-enqueue backlogged merge task
                                    threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(mergeTask);
                                });
                            }
                            return schedule;
                        }).when(mergeTask).schedule();
                        // let some task complete, which will NOT hold up any budget
                        boolean letTaskComplete = randomBoolean();
                        if (letTaskComplete) {
                            // this task will NOT hold up any budget because it runs quickly (it is not blocked)
                            when(mergeTask.estimatedRemainingMergeSize()).thenReturn(randomLongBetween(1_000L, 10_000L));
                        } else {
                            // make sure the available budget is expended
                            if (submittedMergesCount == 1) {
                                when(mergeTask.estimatedRemainingMergeSize()).thenReturn(availableBudget.get());
                            } else {
                                long taskBudget = randomLongBetween(smallMergeTaskSize + 1L, availableBudget.get());
                                when(mergeTask.estimatedRemainingMergeSize()).thenReturn(taskBudget);
                                availableBudget.set(availableBudget.get() - taskBudget);
                            }
                            submittedMergesCount--;
                            // this task will hold up budget because it blocks when it runs (to simulate it running for a long time)
                            doAnswer(mock -> {
                                // wait to be signalled before completing (this holds up budget)
                                blockMergeTasks.await();
                                return null;
                            }).when(mergeTask).run();
                            doAnswer(mock -> {
                                // wait to be signalled before completing (this holds up budget)
                                blockMergeTasks.await();
                                return null;
                            }).when(mergeTask).abort();
                        }
                        threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                    }
                    // running (or aborting) merge tasks have depleted the available budget
                    assertBusy(() -> {
                        assertThat(threadPoolMergeExecutorService.getMergeTasksQueue().getAvailableBudget(), is(0L));
                    });
                    int moreMergeTasksCount = randomIntBetween(1, 10);
                    // any new merge tasks will only be enqueued but not actually run, until more budget becomes available
                    for (int i = 0; i < moreMergeTasksCount; i++) {
                        ThreadPoolMergeScheduler.MergeTask mergeTask = mock(ThreadPoolMergeScheduler.MergeTask.class);
                        when(mergeTask.supportsIOThrottling()).thenReturn(randomBoolean());
                        // even "small" merge tasks cannot run because the available budget is "0"
                        when(mergeTask.estimatedRemainingMergeSize()).thenReturn(randomLongBetween(1L, smallMergeTaskSize));
                        when(mergeTask.schedule()).thenReturn(RUN);
                        threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                    }
                    assertBusy(() -> {
                        assertThat(threadPoolMergeExecutorService.getMergeTasksQueue().getAvailableBudget(), is(0L));
                        // all the newly submitted merge tasks have been enqueued, none executed
                        assertThat(threadPoolMergeExecutorService.getMergeTasksQueueLength(), greaterThanOrEqualTo(moreMergeTasksCount));
                    });
                    // resume blocked merge tasks which should resume everything back
                    blockMergeTasks.countDown();
                    assertBusy(() -> {
                        assertThat(threadPoolMergeExecutorService.getRunningMergeTasks().size(), is(0));
                        assertThat(threadPoolMergeExecutorService.getMergeTasksQueueLength(), is(0));
                        if (aHasMoreSpace) {
                            assertThat(threadPoolMergeExecutorService.getMergeTasksQueue().getAvailableBudget(), is(112_500L));
                        } else {
                            assertThat(threadPoolMergeExecutorService.getMergeTasksQueue().getAvailableBudget(), is(103_000L));
                        }
                    });
                }
            }
        }
    }
}
