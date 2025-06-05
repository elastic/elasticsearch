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
import org.junit.BeforeClass;

import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.ABORT;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.BACKLOG;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.RUN;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ThreadPoolMergeExecutorServiceDiskSpaceTests extends ESTestCase {

    private static TestMockFileStore aFileStore = new TestMockFileStore("mocka");
    private static TestMockFileStore bFileStore = new TestMockFileStore("mockb");
    private static String aPathPart;
    private static String bPathPart;
    private static int mergeExecutorThreadCount;
    private static Settings settings;
    private static TestThreadPool testThreadPool;
    private static NodeEnvironment nodeEnvironment;

    @BeforeClass
    public static void installMockUsableSpaceFS() throws Exception {
        FileSystem current = PathUtils.getDefaultFileSystem();
        aPathPart = "a-" + randomUUID();
        bPathPart = "b-" + randomUUID();
        FileSystemProvider mock = new TestMockUsableSpaceFileSystemProvider(current);
        PathUtilsForTesting.installMock(mock.getFileSystem(null));
        Path path = PathUtils.get(createTempDir().toString());
        // use 2 data paths
        String[] paths = new String[] { path.resolve(aPathPart).toString(), path.resolve(bPathPart).toString() };
        // some tests hold one merge thread blocked, and need at least one other runnable
        mergeExecutorThreadCount = randomIntBetween(2, 8);
        Settings.Builder settingsBuilder = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), path)
            .putList(Environment.PATH_DATA_SETTING.getKey(), paths)
            // the default of "5s" slows down testing
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "50ms")
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount);
        if (randomBoolean()) {
            settingsBuilder.put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true);
        }
        settings = settingsBuilder.build();
        testThreadPool = new TestThreadPool("test", settings);
        nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
    }

    @AfterClass
    public static void removeMockUsableSpaceFS() {
        PathUtilsForTesting.teardown();
        aFileStore = null;
        bFileStore = null;
        testThreadPool.close();
        nodeEnvironment.close();
    }

    static class TestMockUsableSpaceFileSystemProvider extends FilterFileSystemProvider {

        TestMockUsableSpaceFileSystemProvider(FileSystem inner) {
            super("mockusablespace://", inner);
        }

        @Override
        public FileStore getFileStore(Path path) {
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

    public void testAvailableDiskSpaceMonitorWithDefaultSettings() throws Exception {
        // path "a" has lots of free space, and "b" has little
        aFileStore.usableSpace = 100_000L;
        aFileStore.totalSpace = aFileStore.usableSpace * 2;
        bFileStore.usableSpace = 1_000L;
        bFileStore.totalSpace = bFileStore.usableSpace * 2;
        AtomicReference<ByteSizeValue> availableDiskSpaceForMerging = new AtomicReference<>();
        CountDownLatch diskSpaceMonitor = new CountDownLatch(1);
        try (
            var diskSpacePeriodicMonitor = ThreadPoolMergeExecutorService.startDiskSpaceMonitoring(
                testThreadPool,
                nodeEnvironment.dataPaths(),
                ClusterSettings.createBuiltInClusterSettings(settings),
                (availableDiskSpace) -> {
                    availableDiskSpaceForMerging.set(availableDiskSpace);
                    diskSpaceMonitor.countDown();
                }
            )
        ) {
            // wait for the disk space monitor to do a first run
            safeAwait(diskSpaceMonitor);
        }
        // 100_000 (available) - 5% (default flood stage level) * 200_000 (total space)
        assertThat(availableDiskSpaceForMerging.get().getBytes(), is(90_000L));
    }

    public void testAvailableDiskSpaceMonitorSettingsUpdate() throws Exception {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);
        // path "b" has more usable (available) space, but path "a" has more total space
        aFileStore.usableSpace = 900_000L;
        aFileStore.totalSpace = 1_200_000L;
        bFileStore.usableSpace = 1_000_000L;
        bFileStore.totalSpace = 1_100_000L;
        LinkedHashSet<ByteSizeValue> availableDiskSpaceUpdates = new LinkedHashSet<>();
        try (
            var diskSpacePeriodicMonitor = ThreadPoolMergeExecutorService.startDiskSpaceMonitoring(
                testThreadPool,
                nodeEnvironment.dataPaths(),
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
                Settings.builder().put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(), "90%").build()
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
                Settings.builder().put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(), "3000b").build()
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

    public void testBackloggedMergeTasksDoNotHoldUpBudget() throws Exception {
        aFileStore.totalSpace = randomLongBetween(1_000L, 10_000L);
        bFileStore.totalSpace = randomLongBetween(1_000L, 10_000L);
        aFileStore.usableSpace = randomLongBetween(1_000L, aFileStore.totalSpace);
        bFileStore.usableSpace = randomLongBetween(1_000L, bFileStore.totalSpace);
        boolean aHasMoreSpace = aFileStore.usableSpace > bFileStore.usableSpace;
        try (
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorService
                .maybeCreateThreadPoolMergeExecutorService(
                    testThreadPool,
                    ClusterSettings.createBuiltInClusterSettings(settings),
                    nodeEnvironment
                )
        ) {
            assert threadPoolMergeExecutorService != null;
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), greaterThanOrEqualTo(1));
            // assumes the 5% default value for the remaining space watermark
            final long availableInitialBudget = aHasMoreSpace
                ? aFileStore.usableSpace - aFileStore.totalSpace / 20
                : bFileStore.usableSpace - bFileStore.totalSpace / 20;
            final AtomicLong expectedAvailableBudget = new AtomicLong(availableInitialBudget);
            assertBusy(
                () -> assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(expectedAvailableBudget.get()))
            );
            long backloggedMergeTaskDiskSpaceBudget = randomLongBetween(1L, expectedAvailableBudget.get());
            CountDownLatch testDoneLatch = new CountDownLatch(1);
            // take care that there's still at least one thread available to run merges
            int maxBlockingTasksToSubmit = mergeExecutorThreadCount - 1;
            // first maybe submit some running or aborting merge tasks that hold up some budget while running or aborting
            List<ThreadPoolMergeScheduler.MergeTask> runningMergeTasks = new ArrayList<>();
            List<ThreadPoolMergeScheduler.MergeTask> abortingMergeTasks = new ArrayList<>();
            while (expectedAvailableBudget.get() - backloggedMergeTaskDiskSpaceBudget > 0L
                && maxBlockingTasksToSubmit-- > 0
                && randomBoolean()) {
                ThreadPoolMergeScheduler.MergeTask mergeTask = mock(ThreadPoolMergeScheduler.MergeTask.class);
                long taskBudget = randomLongBetween(1L, expectedAvailableBudget.get() - backloggedMergeTaskDiskSpaceBudget);
                when(mergeTask.estimatedRemainingMergeSize()).thenReturn(taskBudget);
                when(mergeTask.schedule()).thenReturn(randomFrom(RUN, ABORT));
                // this task runs/aborts, and it's going to hold up some budget for it
                expectedAvailableBudget.set(expectedAvailableBudget.get() - taskBudget);
                // this task will hold up budget because it blocks when it runs (to simulate it running for a long time)
                doAnswer(mock -> {
                    // wait to be signalled before completing (this holds up budget)
                    testDoneLatch.await();
                    return null;
                }).when(mergeTask).run();
                doAnswer(mock -> {
                    // wait to be signalled before completing (this holds up budget)
                    testDoneLatch.await();
                    return null;
                }).when(mergeTask).abort();
                threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                if (mergeTask.schedule() == RUN) {
                    runningMergeTasks.add(mergeTask);
                } else {
                    abortingMergeTasks.add(mergeTask);
                }
            }
            assertBusy(
                () -> assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(expectedAvailableBudget.get()))
            );
            // submit some backlogging merge tasks which should NOT hold up any budget
            IdentityHashMap<ThreadPoolMergeScheduler.MergeTask, Integer> backloggingMergeTasksScheduleCountMap = new IdentityHashMap<>();
            int backloggingTaskCount = randomIntBetween(1, 10);
            while (backloggingTaskCount-- > 0) {
                ThreadPoolMergeScheduler.MergeTask mergeTask = mock(ThreadPoolMergeScheduler.MergeTask.class);
                long taskBudget = randomLongBetween(1L, backloggedMergeTaskDiskSpaceBudget);
                when(mergeTask.estimatedRemainingMergeSize()).thenReturn(taskBudget);
                doAnswer(mock -> {
                    // task always backlogs (as long as the test hasn't finished)
                    if (testDoneLatch.getCount() > 0) {
                        return BACKLOG;
                    } else {
                        return RUN;
                    }
                }).when(mergeTask).schedule();
                threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                backloggingMergeTasksScheduleCountMap.put(mergeTask, 1);
            }
            int checkRounds = randomIntBetween(1, 10);
            // assert all backlogging merge tasks have been scheduled while possibly re-enqueued,
            // BUT none run and none aborted, AND the available budget is left unchanged
            while (true) {
                assertBusy(() -> {
                    for (ThreadPoolMergeScheduler.MergeTask mergeTask : backloggingMergeTasksScheduleCountMap.keySet()) {
                        verify(mergeTask, times(backloggingMergeTasksScheduleCountMap.get(mergeTask))).schedule();
                    }
                    for (ThreadPoolMergeScheduler.MergeTask mergeTask : backloggingMergeTasksScheduleCountMap.keySet()) {
                        verify(mergeTask, times(0)).run();
                        verify(mergeTask, times(0)).abort();
                    }
                    // budget hasn't changed!
                    assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(expectedAvailableBudget.get()));
                });
                if (checkRounds-- > 0) {
                    break;
                }
                // maybe re-enqueue backlogged merge task
                for (ThreadPoolMergeScheduler.MergeTask backlogged : backloggingMergeTasksScheduleCountMap.keySet()) {
                    if (randomBoolean()) {
                        threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(backlogged);
                        backloggingMergeTasksScheduleCountMap.put(backlogged, backloggingMergeTasksScheduleCountMap.get(backlogged) + 1);
                    }
                }
                // double check that submitting a runnable merge task under budget works correctly
                ThreadPoolMergeScheduler.MergeTask mergeTask = mock(ThreadPoolMergeScheduler.MergeTask.class);
                long taskBudget = randomLongBetween(1L, backloggedMergeTaskDiskSpaceBudget);
                when(mergeTask.estimatedRemainingMergeSize()).thenReturn(taskBudget);
                when(mergeTask.schedule()).thenReturn(RUN);
                threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                assertBusy(() -> {
                    verify(mergeTask).schedule();
                    verify(mergeTask).run();
                });
            }
            // let the test finish
            testDoneLatch.countDown();
            for (ThreadPoolMergeScheduler.MergeTask backlogged : backloggingMergeTasksScheduleCountMap.keySet()) {
                threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(backlogged);
            }
            assertBusy(() -> {
                for (ThreadPoolMergeScheduler.MergeTask mergeTask : runningMergeTasks) {
                    verify(mergeTask).run();
                }
                for (ThreadPoolMergeScheduler.MergeTask mergeTask : abortingMergeTasks) {
                    verify(mergeTask).abort();
                }
                for (ThreadPoolMergeScheduler.MergeTask backlogged : backloggingMergeTasksScheduleCountMap.keySet()) {
                    verify(backlogged).run();
                }
                // available budget is restored
                assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(availableInitialBudget));
                assertThat(threadPoolMergeExecutorService.allDone(), is(true));
            });
        }
    }

    public void testUnavailableBudgetBlocksNewMergeTasksFromStartingExecution() throws Exception {
        int submittedMergesCount = randomIntBetween(1, mergeExecutorThreadCount - 1);
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
        try (
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorService
                .maybeCreateThreadPoolMergeExecutorService(
                    testThreadPool,
                    ClusterSettings.createBuiltInClusterSettings(settings),
                    nodeEnvironment
                )
        ) {
            assert threadPoolMergeExecutorService != null;
            // wait for the budget to be updated from the available disk space
            AtomicLong expectedAvailableBudget = new AtomicLong();
            assertBusy(() -> {
                if (aHasMoreSpace) {
                    // 120_000L (available) - 5% (default flood stage level) * 150_000L (total)
                    assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(112_500L));
                    expectedAvailableBudget.set(112_500L);
                } else {
                    // 110_000L (available) - 5% (default flood stage level) * 140_000L (total)
                    assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(103_000L));
                    expectedAvailableBudget.set(103_000L);
                }
            });
            List<ThreadPoolMergeScheduler.MergeTask> runningOrAbortingMergeTasksList = new ArrayList<>();
            List<CountDownLatch> latchesBlockingMergeTasksList = new ArrayList<>();
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
                if (randomBoolean()) {
                    // this task will NOT hold up any budget because it runs quickly (it is not blocked)
                    when(mergeTask.estimatedRemainingMergeSize()).thenReturn(randomLongBetween(1_000L, 10_000L));
                } else {
                    CountDownLatch blockMergeTaskLatch = new CountDownLatch(1);
                    long taskBudget = randomLongBetween(1L, expectedAvailableBudget.get());
                    when(mergeTask.estimatedRemainingMergeSize()).thenReturn(taskBudget);
                    expectedAvailableBudget.set(expectedAvailableBudget.get() - taskBudget);
                    submittedMergesCount--;
                    // this task will hold up budget because it blocks when it runs (to simulate it running for a long time)
                    doAnswer(mock -> {
                        // wait to be signalled before completing (this holds up budget)
                        blockMergeTaskLatch.await();
                        return null;
                    }).when(mergeTask).run();
                    doAnswer(mock -> {
                        // wait to be signalled before completing (this holds up budget)
                        blockMergeTaskLatch.await();
                        return null;
                    }).when(mergeTask).abort();
                    runningOrAbortingMergeTasksList.add(mergeTask);
                    latchesBlockingMergeTasksList.add(blockMergeTaskLatch);
                }
                threadPoolMergeExecutorService.submitMergeTask(mergeTask);
            }
            // currently running (or aborting) merge tasks have consumed some of the available budget
            while (runningOrAbortingMergeTasksList.isEmpty() == false) {
                assertBusy(
                    () -> assertThat(
                        threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(),
                        is(expectedAvailableBudget.get())
                    )
                );
                ThreadPoolMergeScheduler.MergeTask mergeTask1 = mock(ThreadPoolMergeScheduler.MergeTask.class);
                when(mergeTask1.supportsIOThrottling()).thenReturn(randomBoolean());
                when(mergeTask1.schedule()).thenReturn(RUN);
                ThreadPoolMergeScheduler.MergeTask mergeTask2 = mock(ThreadPoolMergeScheduler.MergeTask.class);
                when(mergeTask2.supportsIOThrottling()).thenReturn(randomBoolean());
                when(mergeTask2.schedule()).thenReturn(RUN);
                boolean task1Runs = randomBoolean();
                long currentAvailableBudget = expectedAvailableBudget.get();
                long overBudget = randomLongBetween(currentAvailableBudget + 1L, currentAvailableBudget + 100L);
                long underBudget = randomLongBetween(0L, currentAvailableBudget);
                if (task1Runs) {
                    // merge task 1 can run because it is under budget
                    when(mergeTask1.estimatedRemainingMergeSize()).thenReturn(underBudget);
                    // merge task 2 cannot run because it is over budget
                    when(mergeTask2.estimatedRemainingMergeSize()).thenReturn(overBudget);
                } else {
                    // merge task 1 cannot run because it is over budget
                    when(mergeTask1.estimatedRemainingMergeSize()).thenReturn(overBudget);
                    // merge task 2 can run because it is under budget
                    when(mergeTask2.estimatedRemainingMergeSize()).thenReturn(underBudget);
                }
                threadPoolMergeExecutorService.submitMergeTask(mergeTask1);
                threadPoolMergeExecutorService.submitMergeTask(mergeTask2);
                assertBusy(() -> {
                    if (task1Runs) {
                        verify(mergeTask1).schedule();
                        verify(mergeTask1).run();
                        verify(mergeTask2, times(0)).schedule();
                        verify(mergeTask2, times(0)).run();
                    } else {
                        verify(mergeTask2).schedule();
                        verify(mergeTask2).run();
                        verify(mergeTask1, times(0)).schedule();
                        verify(mergeTask1, times(0)).run();
                    }
                });
                // let one task finish from the bunch that is holding up budget
                int index = randomIntBetween(0, runningOrAbortingMergeTasksList.size() - 1);
                latchesBlockingMergeTasksList.remove(index).countDown();
                ThreadPoolMergeScheduler.MergeTask completedMergeTask = runningOrAbortingMergeTasksList.remove(index);
                // update the expected budget given that one task now finished
                expectedAvailableBudget.set(expectedAvailableBudget.get() + completedMergeTask.estimatedRemainingMergeSize());
            }
            // let the test finish cleanly
            assertBusy(() -> {
                assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(aHasMoreSpace ? 112_500L : 103_000L));
                assertThat(threadPoolMergeExecutorService.allDone(), is(true));
            });
        }
    }
}
