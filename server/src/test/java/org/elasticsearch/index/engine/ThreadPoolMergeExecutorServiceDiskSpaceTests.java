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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;

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
    public void x() {
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
                }
            }
        }
    }
}
