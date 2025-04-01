/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.file;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractFileWatchingServiceTests extends ESTestCase {

    static class TestFileWatchingService extends AbstractFileWatchingService {

        private final Consumer<Path> called;

        TestFileWatchingService(Path watchedFile, Consumer<Path> called) {
            super(watchedFile.getParent());
            this.called = called;
        }

        @Override
        protected void processFileChanges(Path file) throws InterruptedException, ExecutionException, IOException {
            // sometimes files from ExtraFS appear in the directory
            if (called != null && file.getFileName().toString().startsWith("extra") == false) {
                called.accept(file);
            }
        }

        @Override
        protected void processInitialFilesMissing() {
            if (called != null) {
                called.accept(null);
            }
        }

        // the following methods are a workaround to ensure exclusive access for files
        // required by child watchers; this is required because we only check the caller's module
        // not the entire stack
        @Override
        protected boolean filesExists(Path path) {
            return Files.exists(path);
        }

        @Override
        protected boolean filesIsDirectory(Path path) {
            return Files.isDirectory(path);
        }

        @Override
        protected <A extends BasicFileAttributes> A filesReadAttributes(Path path, Class<A> clazz) throws IOException {
            return Files.readAttributes(path, clazz);
        }

        @Override
        protected Stream<Path> filesList(Path dir) throws IOException {
            return Files.list(dir);
        }

        @Override
        protected Path filesSetLastModifiedTime(Path path, FileTime time) throws IOException {
            return Files.setLastModifiedTime(path, time);
        }

        @Override
        protected InputStream filesNewInputStream(Path path) throws IOException {
            return Files.newInputStream(path);
        }
    }

    private Path watchedFile;
    private AbstractFileWatchingService fileWatchingService;
    private BlockingQueue<Optional<Path>> updates;
    private ThreadPool threadpool;
    private ClusterService clusterService;
    private Environment env;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadpool = new TestThreadPool("file_settings_service_tests");

        clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.builder().put(NODE_NAME_SETTING.getKey(), "test").build());

        final DiscoveryNode localNode = DiscoveryNodeUtils.create("node");
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        env = newEnvironment(Settings.EMPTY);

        Files.createDirectories(env.configDir());

        watchedFile = getWatchedFilePath(env);
        updates = new ArrayBlockingQueue<>(5);
        fileWatchingService = new TestFileWatchingService(watchedFile, f -> updates.add(Optional.ofNullable(f)));
    }

    @After
    public void tearDown() throws Exception {
        fileWatchingService.close();
        super.tearDown();
        threadpool.shutdownNow();
    }

    public void testStartStop() {
        fileWatchingService.start();
        assertTrue(fileWatchingService.watching());
        fileWatchingService.stop();
        assertFalse(fileWatchingService.watching());
    }

    public void testWatchedFile() throws Exception {
        Path tmpFile = createTempFile();
        Path tmpFile1 = createTempFile();
        Path otherFile = tmpFile.getParent().resolve("other.json");
        // we return false on non-existent paths, we don't remember state
        assertFalse(fileWatchingService.fileChanged(otherFile));

        // we remember the previous state
        assertTrue(fileWatchingService.fileChanged(tmpFile));
        assertFalse(fileWatchingService.fileChanged(tmpFile));

        // we modify the timestamp of the file, it should trigger a change
        Instant now = LocalDateTime.now(ZoneId.systemDefault()).toInstant(ZoneOffset.ofHours(0));
        Files.setLastModifiedTime(tmpFile, FileTime.from(now));

        assertTrue(fileWatchingService.fileChanged(tmpFile));
        assertFalse(fileWatchingService.fileChanged(tmpFile));

        // we change to another real file, it should be changed
        assertTrue(fileWatchingService.fileChanged(tmpFile1));
        assertFalse(fileWatchingService.fileChanged(tmpFile1));
    }

    public void testCallsProcessing() throws Exception {
        fileWatchingService.start();
        assertTrue(fileWatchingService.watching());
        // poll ping from processInitialFilesMissing
        assertThat(updates.poll(30, TimeUnit.SECONDS), isEmpty());

        Files.createDirectories(fileWatchingService.watchedFileDir());

        writeTestFile(watchedFile, "{}");

        // we need to wait a bit, on MacOS it may take up to 10 seconds for the Java watcher service to notice the file,
        // on Linux is instantaneous. Windows is instantaneous too.
        assertThat(updates.poll(30, TimeUnit.SECONDS), isPresentWith(watchedFile));

        // test another file
        Path otherFile = watchedFile.resolveSibling("otherFile.json");
        writeTestFile(otherFile, "{}");
        assertThat(updates.poll(30, TimeUnit.SECONDS), isPresentWith(otherFile));
    }

    public void testRegisterWatchKeyRetry() throws IOException, InterruptedException {
        var service = spy(fileWatchingService);
        doAnswer(i -> 0L).when(service).retryDelayMillis(anyInt());

        Files.createDirectories(service.watchedFileDir());

        var mockedPath = spy(service.watchedFileDir());
        var prevWatchKey = mock(WatchKey.class);
        var newWatchKey = mock(WatchKey.class);

        doThrow(new IOException("can't register")).doThrow(new IOException("can't register - attempt 2"))
            .doAnswer(i -> newWatchKey)
            .when(mockedPath)
            .register(
                any(),
                eq(StandardWatchEventKinds.ENTRY_MODIFY),
                eq(StandardWatchEventKinds.ENTRY_CREATE),
                eq(StandardWatchEventKinds.ENTRY_DELETE)
            );

        var result = service.enableDirectoryWatcher(prevWatchKey, mockedPath);
        assertThat(result, sameInstance(newWatchKey));
        assertTrue(result != prevWatchKey);

        verify(service, times(2)).retryDelayMillis(anyInt());
    }

    // helpers
    private static void writeTestFile(Path path, String contents) throws IOException {
        Path tempFilePath = createTempFile();

        Files.writeString(tempFilePath, contents);
        Files.move(tempFilePath, path, StandardCopyOption.ATOMIC_MOVE);
    }

    private static Path getWatchedFilePath(Environment env) {
        return env.configDir().toAbsolutePath().resolve("test").resolve("test.json");
    }

}
