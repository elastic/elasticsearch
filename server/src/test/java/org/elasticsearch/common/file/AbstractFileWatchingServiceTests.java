/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.file;

import org.elasticsearch.cluster.ClusterChangedEvent;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
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

    class TestFileWatchingService extends AbstractFileWatchingService {

        private final CountDownLatch countDownLatch;

        TestFileWatchingService(ClusterService clusterService, Path watchedFile) {
            super(clusterService, watchedFile);
            this.countDownLatch = null;
        }

        TestFileWatchingService(ClusterService clusterService, Path watchedFile, CountDownLatch countDownLatch) {
            super(clusterService, watchedFile);
            this.countDownLatch = countDownLatch;
        }

        @Override
        protected void processFileChanges() throws InterruptedException, ExecutionException, IOException {
            if (countDownLatch != null) {
                countDownLatch.countDown();
            }
        }
    }

    private AbstractFileWatchingService fileWatchingService;
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

        Files.createDirectories(env.configFile());

        fileWatchingService = new TestFileWatchingService(clusterService, getWatchedFilePath(env));
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        threadpool.shutdownNow();
    }

    public void testStartStop() {
        fileWatchingService.start();
        fileWatchingService.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));
        assertTrue(fileWatchingService.watching());
        fileWatchingService.stop();
        assertFalse(fileWatchingService.watching());
        fileWatchingService.close();
    }

    public void testWatchedFile() throws Exception {
        Path tmpFile = createTempFile();
        Path tmpFile1 = createTempFile();
        Path otherFile = tmpFile.getParent().resolve("other.json");
        // we return false on non-existent paths, we don't remember state
        assertFalse(fileWatchingService.watchedFileChanged(otherFile));

        // we remember the previous state
        assertTrue(fileWatchingService.watchedFileChanged(tmpFile));
        assertFalse(fileWatchingService.watchedFileChanged(tmpFile));

        // we modify the timestamp of the file, it should trigger a change
        Instant now = LocalDateTime.now(ZoneId.systemDefault()).toInstant(ZoneOffset.ofHours(0));
        Files.setLastModifiedTime(tmpFile, FileTime.from(now));

        assertTrue(fileWatchingService.watchedFileChanged(tmpFile));
        assertFalse(fileWatchingService.watchedFileChanged(tmpFile));

        // we change to another real file, it should be changed
        assertTrue(fileWatchingService.watchedFileChanged(tmpFile1));
        assertFalse(fileWatchingService.watchedFileChanged(tmpFile1));
    }

    public void testCallsProcessing() throws Exception {
        CountDownLatch processFileLatch = new CountDownLatch(1);

        AbstractFileWatchingService service = new TestFileWatchingService(clusterService, getWatchedFilePath(env), processFileLatch);

        service.start();
        service.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), ClusterState.EMPTY_STATE));
        assertTrue(service.watching());

        Files.createDirectories(service.watchedFileDir());

        writeTestFile(service.watchedFile(), "{}");

        // we need to wait a bit, on MacOS it may take up to 10 seconds for the Java watcher service to notice the file,
        // on Linux is instantaneous. Windows is instantaneous too.
        processFileLatch.await(30, TimeUnit.SECONDS);

        service.stop();
        assertFalse(service.watching());
        service.close();
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
    private void writeTestFile(Path path, String contents) throws IOException {
        Path tempFilePath = createTempFile();

        Files.write(tempFilePath, contents.getBytes(StandardCharsets.UTF_8));
        Files.move(tempFilePath, path, StandardCopyOption.ATOMIC_MOVE);
    }

    private static Path getWatchedFilePath(Environment env) {
        return env.configFile().toAbsolutePath().resolve("test").resolve("test.json");
    }

}
