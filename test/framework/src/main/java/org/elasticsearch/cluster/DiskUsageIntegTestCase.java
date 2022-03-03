/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.apache.lucene.tests.mockfile.FilterFileStore;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * An integration test case that allows mocking the disk usage per node. Notice that only Lucene files count towards disk usage and translog
 * and state files are disregarded.
 */
public class DiskUsageIntegTestCase extends ESIntegTestCase {
    protected static TestFileSystemProvider fileSystemProvider;
    private FileSystem defaultFileSystem;

    @Before
    public void installFilesystemProvider() {
        assertNull(defaultFileSystem);
        defaultFileSystem = PathUtils.getDefaultFileSystem();
        assertNull(fileSystemProvider);
        fileSystemProvider = new TestFileSystemProvider(defaultFileSystem, createTempDir());
        PathUtilsForTesting.installMock(fileSystemProvider.getFileSystem(null));
    }

    @After
    public void removeFilesystemProvider() {
        fileSystemProvider = null;
        assertNotNull(defaultFileSystem);
        PathUtilsForTesting.installMock(defaultFileSystem); // set the default filesystem back
        defaultFileSystem = null;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Path dataPath = fileSystemProvider.getRootDir().resolve("node-" + nodeOrdinal);
        try {
            Files.createDirectories(dataPath);
        } catch (IOException e) {
            throw new AssertionError("unexpected", e);
        }
        fileSystemProvider.addTrackedPath(dataPath);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Environment.PATH_DATA_SETTING.getKey(), dataPath)
            .put(FsService.ALWAYS_REFRESH_SETTING.getKey(), true)
            .build();
    }

    public TestFileStore getTestFileStore(String nodeName) {
        return fileSystemProvider.getTestFileStore(internalCluster().getInstance(Environment.class, nodeName).dataFiles()[0]);
    }

    protected static class TestFileStore extends FilterFileStore {

        private final Path path;

        private volatile long totalSpace = -1;

        TestFileStore(FileStore delegate, String scheme, Path path) {
            super(delegate, scheme);
            this.path = path;
        }

        @Override
        public String name() {
            return "fake"; // Lucene's is-spinning-disk check expects the device name here
        }

        @Override
        public long getTotalSpace() throws IOException {
            final long totalSpaceCopy = this.totalSpace;
            if (totalSpaceCopy == -1) {
                return super.getTotalSpace();
            } else {
                return totalSpaceCopy;
            }
        }

        public void setTotalSpace(long totalSpace) {
            assertThat(totalSpace, anyOf(is(-1L), greaterThan(0L)));
            this.totalSpace = totalSpace;
        }

        @Override
        public long getUsableSpace() throws IOException {
            final long totalSpaceCopy = this.totalSpace;
            if (totalSpaceCopy == -1) {
                return super.getUsableSpace();
            } else {
                return Math.max(0L, totalSpaceCopy - getTotalFileSize(path));
            }
        }

        @Override
        public long getUnallocatedSpace() throws IOException {
            final long totalSpaceCopy = this.totalSpace;
            if (totalSpaceCopy == -1) {
                return super.getUnallocatedSpace();
            } else {
                return Math.max(0L, totalSpaceCopy - getTotalFileSize(path));
            }
        }

        private static long getTotalFileSize(Path path) throws IOException {
            if (Files.isRegularFile(path)) {
                if (path.getFileName().toString().equals("nodes")
                    && Files.readString(path, StandardCharsets.UTF_8).contains("prevent a downgrade")) {
                    return 0;
                }
                try {
                    return Files.size(path);
                } catch (NoSuchFileException | FileNotFoundException e) {
                    // probably removed
                    return 0L;
                }
            } else if (path.getFileName().toString().equals("_state") || path.getFileName().toString().equals("translog")) {
                // ignore metadata and translog, since the disk threshold decider only cares about the store size
                return 0L;
            } else {
                try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path)) {
                    long total = 0L;
                    for (Path subpath : directoryStream) {
                        total += getTotalFileSize(subpath);
                    }
                    return total;
                } catch (NotDirectoryException | NoSuchFileException | FileNotFoundException e) {
                    // probably removed
                    return 0L;
                }
            }
        }
    }

    private static class TestFileSystemProvider extends FilterFileSystemProvider {
        private final Map<Path, TestFileStore> trackedPaths = newConcurrentMap();
        private final Path rootDir;

        TestFileSystemProvider(FileSystem delegateInstance, Path rootDir) {
            super("diskthreshold://", delegateInstance);
            this.rootDir = new FilterPath(rootDir, fileSystem);
        }

        Path getRootDir() {
            return rootDir;
        }

        void addTrackedPath(Path path) {
            assertTrue(path + " starts with " + rootDir, path.startsWith(rootDir));
            final FileStore fileStore;
            try {
                fileStore = super.getFileStore(path);
            } catch (IOException e) {
                throw new AssertionError("unexpected", e);
            }
            assertNull(trackedPaths.put(path, new TestFileStore(fileStore, getScheme(), path)));
        }

        @Override
        public FileStore getFileStore(Path path) {
            return getTestFileStore(path);
        }

        TestFileStore getTestFileStore(Path path) {
            final TestFileStore fileStore = trackedPaths.get(path);
            if (fileStore != null) {
                return fileStore;
            }

            // On Linux, and only Linux, Lucene obtains a filestore for the index in order to determine whether it's on a spinning disk or
            // not so it can configure the merge scheduler accordingly
            assertTrue(path + " not tracked and not on Linux", Constants.LINUX);
            final Set<Path> containingPaths = trackedPaths.keySet().stream().filter(path::startsWith).collect(Collectors.toSet());
            assertThat(path + " not contained in a unique tracked path", containingPaths, hasSize(1));
            return trackedPaths.get(containingPaths.iterator().next());
        }

        void clearTrackedPaths() throws IOException {
            for (Path path : trackedPaths.keySet()) {
                IOUtils.rm(path);
            }
            trackedPaths.clear();
        }
    }
}
