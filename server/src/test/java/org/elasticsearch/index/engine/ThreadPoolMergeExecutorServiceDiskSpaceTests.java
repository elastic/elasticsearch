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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
    private static TestCapturingThreadPool testThreadPool;
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
        mergeExecutorThreadCount = randomIntBetween(2, 9);
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
        testThreadPool = new TestCapturingThreadPool("test", settings);
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

    @After
    public void cleanupThreadPool() {
        testThreadPool.scheduledTasks.clear();
    }

    static class TestCapturingThreadPool extends TestThreadPool {
        final List<Tuple<TimeValue, Cancellable>> scheduledTasks = new ArrayList<>();

        TestCapturingThreadPool(String name, Settings settings) {
            super(name, settings);
        }

        @Override
        public Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, Executor executor) {
            Cancellable cancellable = super.scheduleWithFixedDelay(command, interval, executor);
            scheduledTasks.add(new Tuple<>(interval, cancellable));
            return cancellable;
        }
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

        public volatile long totalSpace;
        public volatile long freeSpace;
        public volatile long usableSpace;
        public volatile boolean throwIoException;

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
        public long getTotalSpace() throws IOException {
            if (throwIoException) {
                throw new IOException("Test IO Exception");
            }
            return totalSpace;
        }

        @Override
        public long getUnallocatedSpace() throws IOException {
            if (throwIoException) {
                throw new IOException("Test IO Exception");
            }
            return freeSpace;
        }

        @Override
        public long getUsableSpace() throws IOException {
            if (throwIoException) {
                throw new IOException("Test IO Exception");
            }
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
        LinkedHashSet<ByteSizeValue> availableDiskSpaceUpdates = new LinkedHashSet<>();
        try (
            var diskSpacePeriodicMonitor = ThreadPoolMergeExecutorService.startDiskSpaceMonitoring(
                testThreadPool,
                nodeEnvironment.dataPaths(),
                ClusterSettings.createBuiltInClusterSettings(settings),
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
                    // 100_000 (available) - 5% (default flood stage level) * 200_000 (total space)
                    assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(90_000L));
                }
            });
            // "b" now has more available space
            bFileStore.usableSpace = 110_000L;
            bFileStore.totalSpace = 130_000L;
            assertBusy(() -> {
                synchronized (availableDiskSpaceUpdates) {
                    assertThat(availableDiskSpaceUpdates.size(), is(2));
                    // 110_000 (available) - 5% (default flood stage level) * 130_000 (total space)
                    assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(103_500L));
                }
            });
            // available space for "a" and "b" is below the limit => it's clamp down to "0"
            aFileStore.usableSpace = 100L;
            bFileStore.usableSpace = 1_000L;
            assertBusy(() -> {
                synchronized (availableDiskSpaceUpdates) {
                    assertThat(availableDiskSpaceUpdates.size(), is(3));
                    // 1_000 (available) - 5% (default flood stage level) * 130_000 (total space) < 0
                    assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(0L));
                }
            });
        }
    }

    public void testDiskSpaceMonitorStartsAsDisabled() throws Exception {
        aFileStore.usableSpace = randomLongBetween(1L, 100L);
        aFileStore.totalSpace = randomLongBetween(1L, 100L);
        aFileStore.throwIoException = randomBoolean();
        bFileStore.usableSpace = randomLongBetween(1L, 100L);
        bFileStore.totalSpace = randomLongBetween(1L, 100L);
        bFileStore.throwIoException = randomBoolean();
        Settings.Builder settingsBuilder = Settings.builder().put(settings);
        if (randomBoolean()) {
            settingsBuilder.put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0");
        } else {
            settingsBuilder.put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0s");
        }
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);
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
            assertThat(diskSpacePeriodicMonitor.isScheduled(), is(false));
            assertThat(availableDiskSpaceUpdates.size(), is(1));
            assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(Long.MAX_VALUE));
            // updating monitoring interval should enable the monitor
            String intervalSettingValue = randomFrom("1s", "123ms", "5nanos", "2h");
            clusterSettings.applySettings(
                Settings.builder()
                    .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), intervalSettingValue)
                    .build()
            );
            assertThat(diskSpacePeriodicMonitor.isScheduled(), is(true));
            assertThat(testThreadPool.scheduledTasks.size(), is(1));
            assertThat(
                testThreadPool.scheduledTasks.getLast().v1(),
                is(
                    TimeValue.parseTimeValue(
                        intervalSettingValue,
                        ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey()
                    )
                )
            );
        }
        aFileStore.throwIoException = false;
        bFileStore.throwIoException = false;
    }

    public void testAvailableDiskSpaceMonitorWhenFileSystemStatErrors() throws Exception {
        aFileStore.usableSpace = randomLongBetween(1L, 100L);
        aFileStore.totalSpace = randomLongBetween(1L, 100L);
        bFileStore.usableSpace = randomLongBetween(1L, 100L);
        bFileStore.totalSpace = randomLongBetween(1L, 100L);
        boolean aErrorsFirst = randomBoolean();
        if (aErrorsFirst) {
            // the "a" file system will error when collecting stats
            aFileStore.throwIoException = true;
            bFileStore.throwIoException = false;
        } else {
            aFileStore.throwIoException = false;
            bFileStore.throwIoException = true;
        }
        LinkedHashSet<ByteSizeValue> availableDiskSpaceUpdates = new LinkedHashSet<>();
        try (
            var diskSpacePeriodicMonitor = ThreadPoolMergeExecutorService.startDiskSpaceMonitoring(
                testThreadPool,
                nodeEnvironment.dataPaths(),
                ClusterSettings.createBuiltInClusterSettings(settings),
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
                    if (aErrorsFirst) {
                        // uses the stats from "b"
                        assertThat(
                            availableDiskSpaceUpdates.getLast().getBytes(),
                            // the default 5% (same as flood stage level)
                            is(Math.max(bFileStore.usableSpace - bFileStore.totalSpace / 20, 0L))
                        );
                    } else {
                        // uses the stats from "a"
                        assertThat(
                            availableDiskSpaceUpdates.getLast().getBytes(),
                            // the default 5% (same as flood stage level)
                            is(Math.max(aFileStore.usableSpace - aFileStore.totalSpace / 20, 0L))
                        );
                    }
                }
            });
            if (aErrorsFirst) {
                // the "b" file system will also now error when collecting stats
                bFileStore.throwIoException = true;
            } else {
                // the "a" file system will also now error when collecting stats
                aFileStore.throwIoException = true;
            }
            assertBusy(() -> {
                synchronized (availableDiskSpaceUpdates) {
                    assertThat(availableDiskSpaceUpdates.size(), is(2));
                    // consider the available disk space as unlimited when no fs stats can be collected
                    assertThat(availableDiskSpaceUpdates.getLast().getBytes(), is(Long.MAX_VALUE));
                }
            });
            if (aErrorsFirst) {
                // "a" fs stats collection recovered
                aFileStore.throwIoException = false;
            } else {
                // "b" fs stats collection recovered
                bFileStore.throwIoException = false;
            }
            assertBusy(() -> {
                synchronized (availableDiskSpaceUpdates) {
                    assertThat(availableDiskSpaceUpdates.size(), is(3));
                    if (aErrorsFirst) {
                        // uses the stats from "a"
                        assertThat(
                            availableDiskSpaceUpdates.getLast().getBytes(),
                            // the default 5% (same as flood stage level)
                            is(Math.max(aFileStore.usableSpace - aFileStore.totalSpace / 20, 0L))
                        );
                    } else {
                        // uses the stats from "b"
                        assertThat(
                            availableDiskSpaceUpdates.getLast().getBytes(),
                            // the default 5% (same as flood stage level)
                            is(Math.max(bFileStore.usableSpace - bFileStore.totalSpace / 20, 0L))
                        );
                    }
                }
            });
        }
        aFileStore.throwIoException = false;
        bFileStore.throwIoException = false;
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

    public void testAbortingOrRunningMergeTaskHoldsUpBudget() throws Exception {
        aFileStore.totalSpace = randomLongBetween(1_000L, 10_000L);
        bFileStore.totalSpace = randomLongBetween(1_000L, 10_000L);
        aFileStore.usableSpace = randomLongBetween(900L, aFileStore.totalSpace);
        bFileStore.usableSpace = randomLongBetween(900L, bFileStore.totalSpace);
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
            // wait for the merge scheduler to learn about the available disk space
            assertBusy(
                () -> assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(expectedAvailableBudget.get()))
            );
            ThreadPoolMergeScheduler.MergeTask stallingMergeTask = mock(ThreadPoolMergeScheduler.MergeTask.class);
            long taskBudget = randomLongBetween(1L, expectedAvailableBudget.get());
            when(stallingMergeTask.estimatedRemainingMergeSize()).thenReturn(taskBudget);
            when(stallingMergeTask.schedule()).thenReturn(randomFrom(RUN, ABORT));
            CountDownLatch testDoneLatch = new CountDownLatch(1);
            doAnswer(mock -> {
                // wait to be signalled before completing (this holds up budget)
                testDoneLatch.await();
                return null;
            }).when(stallingMergeTask).run();
            doAnswer(mock -> {
                // wait to be signalled before completing (this holds up budget)
                testDoneLatch.await();
                return null;
            }).when(stallingMergeTask).abort();
            threadPoolMergeExecutorService.submitMergeTask(stallingMergeTask);
            // assert the merge task is holding up disk space budget
            expectedAvailableBudget.set(expectedAvailableBudget.get() - taskBudget);
            assertBusy(
                () -> assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(expectedAvailableBudget.get()))
            );
            // double check that submitting a runnable merge task under budget works correctly
            ThreadPoolMergeScheduler.MergeTask mergeTask = mock(ThreadPoolMergeScheduler.MergeTask.class);
            when(mergeTask.estimatedRemainingMergeSize()).thenReturn(randomLongBetween(0L, expectedAvailableBudget.get()));
            when(mergeTask.schedule()).thenReturn(RUN);
            threadPoolMergeExecutorService.submitMergeTask(mergeTask);
            assertBusy(() -> {
                verify(mergeTask).schedule();
                verify(mergeTask).run();
                verify(mergeTask, times(0)).abort();
            });
            // let the test finish
            testDoneLatch.countDown();
            assertBusy(() -> {
                // available budget is back to the initial value
                assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(availableInitialBudget));
                if (stallingMergeTask.schedule() == RUN) {
                    verify(stallingMergeTask).run();
                    verify(stallingMergeTask, times(0)).abort();
                } else {
                    verify(stallingMergeTask).abort();
                    verify(stallingMergeTask, times(0)).run();
                }
                assertThat(threadPoolMergeExecutorService.allDone(), is(true));
            });
        }
    }

    public void testBackloggedMergeTasksDoNotHoldUpBudget() throws Exception {
        aFileStore.totalSpace = randomLongBetween(1_000L, 10_000L);
        bFileStore.totalSpace = randomLongBetween(1_000L, 10_000L);
        aFileStore.usableSpace = randomLongBetween(900L, aFileStore.totalSpace);
        bFileStore.usableSpace = randomLongBetween(900L, bFileStore.totalSpace);
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
                if (checkRounds-- <= 0) {
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
                    verify(mergeTask, times(0)).abort();
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
            int submittedMergesCount = randomIntBetween(1, mergeExecutorThreadCount - 1);
            // submit merge tasks that don't finish, in order to deplete the available budget
            while (submittedMergesCount > 0 && expectedAvailableBudget.get() > 0L) {
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

    public void testMergeTasksAreUnblockedWhenMoreDiskSpaceBecomesAvailable() throws Exception {
        aFileStore.totalSpace = randomLongBetween(300L, 1_000L);
        bFileStore.totalSpace = randomLongBetween(300L, 1_000L);
        long grantedUsableSpaceBuffer = randomLongBetween(10L, 50L);
        aFileStore.usableSpace = randomLongBetween(200L, aFileStore.totalSpace - grantedUsableSpaceBuffer);
        bFileStore.usableSpace = randomLongBetween(200L, bFileStore.totalSpace - grantedUsableSpaceBuffer);
        boolean aHasMoreSpace = aFileStore.usableSpace > bFileStore.usableSpace;
        Settings.Builder settingsBuilder = Settings.builder().put(settings);
        // change the watermark level, just for coverage and it's easier with the calculations
        if (randomBoolean()) {
            settingsBuilder.put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(), "90%");
        } else {
            settingsBuilder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "90%");
        }
        try (
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorService
                .maybeCreateThreadPoolMergeExecutorService(
                    testThreadPool,
                    ClusterSettings.createBuiltInClusterSettings(settingsBuilder.build()),
                    nodeEnvironment
                )
        ) {
            assert threadPoolMergeExecutorService != null;
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), greaterThanOrEqualTo(1));
            // uses the 10% watermark limit
            final long availableInitialBudget = aHasMoreSpace
                ? aFileStore.usableSpace - aFileStore.totalSpace / 10
                : bFileStore.usableSpace - bFileStore.totalSpace / 10;
            final AtomicLong expectedAvailableBudget = new AtomicLong(availableInitialBudget);
            assertBusy(
                () -> assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(expectedAvailableBudget.get()))
            );
            // maybe let some merge tasks hold up some budget
            // take care that there's still at least one thread available to run merges
            int maxBlockingTasksToSubmit = mergeExecutorThreadCount - 1;
            // first maybe submit some running or aborting merge tasks that hold up some budget while running or aborting
            List<ThreadPoolMergeScheduler.MergeTask> runningMergeTasks = new ArrayList<>();
            List<ThreadPoolMergeScheduler.MergeTask> abortingMergeTasks = new ArrayList<>();
            CountDownLatch testDoneLatch = new CountDownLatch(1);
            while (expectedAvailableBudget.get() > 0L && maxBlockingTasksToSubmit-- > 0 && randomBoolean()) {
                ThreadPoolMergeScheduler.MergeTask mergeTask = mock(ThreadPoolMergeScheduler.MergeTask.class);
                long taskBudget = randomLongBetween(1L, expectedAvailableBudget.get());
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
            assertBusy(() -> {
                assertThat(threadPoolMergeExecutorService.getMergeTasksQueueLength(), is(0));
                assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(expectedAvailableBudget.get()));
            });
            // send some runnable merge tasks that although runnable are currently over budget
            int overBudgetTaskCount = randomIntBetween(1, 5);
            List<ThreadPoolMergeScheduler.MergeTask> overBudgetTasksToRunList = new ArrayList<>();
            List<ThreadPoolMergeScheduler.MergeTask> overBudgetTasksToAbortList = new ArrayList<>();
            while (overBudgetTaskCount-- > 0) {
                ThreadPoolMergeScheduler.MergeTask mergeTask = mock(ThreadPoolMergeScheduler.MergeTask.class);
                // currently over-budget
                long taskBudget = randomLongBetween(
                    expectedAvailableBudget.get() + 1L,
                    expectedAvailableBudget.get() + grantedUsableSpaceBuffer
                );
                when(mergeTask.estimatedRemainingMergeSize()).thenReturn(taskBudget);
                Schedule schedule = randomFrom(RUN, ABORT);
                when(mergeTask.schedule()).thenReturn(schedule);
                threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                if (schedule == RUN) {
                    overBudgetTasksToRunList.add(mergeTask);
                } else {
                    overBudgetTasksToAbortList.add(mergeTask);
                }
            }
            // over-budget tasks did not run, are enqueued, and budget is unchanged
            assertBusy(() -> {
                for (ThreadPoolMergeScheduler.MergeTask mergeTask : overBudgetTasksToAbortList) {
                    verify(mergeTask, times(0)).schedule();
                    verify(mergeTask, times(0)).run();
                    verify(mergeTask, times(0)).abort();
                }
                for (ThreadPoolMergeScheduler.MergeTask mergeTask : overBudgetTasksToRunList) {
                    verify(mergeTask, times(0)).schedule();
                    verify(mergeTask, times(0)).run();
                    verify(mergeTask, times(0)).abort();
                }
                assertThat(
                    threadPoolMergeExecutorService.getMergeTasksQueueLength(),
                    is(overBudgetTasksToAbortList.size() + overBudgetTasksToRunList.size())
                );
                assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(expectedAvailableBudget.get()));
            });
            // more disk space becomes available
            if (aHasMoreSpace) {
                aFileStore.usableSpace += grantedUsableSpaceBuffer;
            } else {
                bFileStore.usableSpace += grantedUsableSpaceBuffer;
            }
            expectedAvailableBudget.set(expectedAvailableBudget.get() + grantedUsableSpaceBuffer);
            // all over-budget tasks can now run because more disk space became available
            assertBusy(() -> {
                for (ThreadPoolMergeScheduler.MergeTask mergeTask : overBudgetTasksToRunList) {
                    verify(mergeTask).schedule();
                    verify(mergeTask).run();
                    verify(mergeTask, times(0)).abort();
                }
                for (ThreadPoolMergeScheduler.MergeTask mergeTask : overBudgetTasksToAbortList) {
                    verify(mergeTask).schedule();
                    verify(mergeTask, times(0)).run();
                    verify(mergeTask).abort();
                }
                assertThat(threadPoolMergeExecutorService.getMergeTasksQueueLength(), is(0));
                assertThat(threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(), is(expectedAvailableBudget.get()));
            });
            // let test finish cleanly
            testDoneLatch.countDown();
            assertBusy(() -> {
                for (ThreadPoolMergeScheduler.MergeTask mergeTask : runningMergeTasks) {
                    verify(mergeTask).run();
                }
                for (ThreadPoolMergeScheduler.MergeTask mergeTask : abortingMergeTasks) {
                    verify(mergeTask).abort();
                }
                assertThat(
                    threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(),
                    is(availableInitialBudget + grantedUsableSpaceBuffer)
                );
                assertThat(threadPoolMergeExecutorService.allDone(), is(true));
                assertThat(
                    threadPoolMergeExecutorService.getDiskSpaceAvailableForNewMergeTasks(),
                    is(availableInitialBudget + grantedUsableSpaceBuffer)
                );
            });
        }
    }
}
