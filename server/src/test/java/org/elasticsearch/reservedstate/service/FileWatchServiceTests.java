/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.test.ESTestCase;
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
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
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

public class FileWatchServiceTests extends ESTestCase {

    private Path directory;
    private FileWatchService fileWatchService;
    private static final String FILENAME = "settings.json";

    @Before
    public void setUp() throws Exception {
        super.setUp();

        directory = createTempDir();
        fileWatchService = new FileWatchService(directory, FILENAME);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();

        fileWatchService.stopWatcher();
    }

    public void testWatchedFile() throws Exception {
        Path tmpFile = createTempFile();
        Path tmpFile1 = createTempFile();
        Path otherFile = tmpFile.getParent().resolve("other.json");
        // we return false on non-existent paths, we don't remember state
        assertFalse(fileWatchService.watchedFileChanged(otherFile));

        // we remember the previous state
        assertTrue(fileWatchService.watchedFileChanged(tmpFile));
        assertFalse(fileWatchService.watchedFileChanged(tmpFile));

        // we modify the timestamp of the file, it should trigger a change
        Instant now = LocalDateTime.now(ZoneId.systemDefault()).toInstant(ZoneOffset.ofHours(0));
        Files.setLastModifiedTime(tmpFile, FileTime.from(now));

        assertTrue(fileWatchService.watchedFileChanged(tmpFile));
        assertFalse(fileWatchService.watchedFileChanged(tmpFile));

        // we change to another real file, it should be changed
        assertTrue(fileWatchService.watchedFileChanged(tmpFile1));
        assertFalse(fileWatchService.watchedFileChanged(tmpFile1));
    }

    public void testRegisterWatchKeyRetry() throws IOException, InterruptedException {
        var service = spy(fileWatchService);
        doAnswer(i -> 0L).when(service).retryDelayMillis(anyInt());

        Files.createDirectories(service.operatorSettingsDir());

        var mockedPath = spy(service.operatorSettingsDir());
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

        var result = service.enableSettingsWatcher(prevWatchKey, mockedPath);
        assertThat(result, sameInstance(newWatchKey));
        assertTrue(result != prevWatchKey);

        verify(service, times(2)).retryDelayMillis(anyInt());
    }

    public void testOperatorSettingsDir() {
        assertThat(fileWatchService.operatorSettingsDir(), equalTo(directory));
    }

    public void testOperatorSettingsFile() {
        assertThat(fileWatchService.operatorSettingsFile(), equalTo(directory.resolve(FILENAME)));
    }

    public void testIsActive() {
        fileWatchService.doStart();
        assertTrue(fileWatchService.isActive());
        fileWatchService.doStop();
        assertFalse(fileWatchService.isActive());
    }

    public void testStartWatcherWithInitialFileSettings() throws Exception {

        writeTestFile(fileWatchService.operatorSettingsFile(), "{}");

        // startup latch = process a settings file latch
        CountDownLatch processFileSettingsLatch = new CountDownLatch(2);

        // listener latch = no setting file present latch
        CountDownLatch noInitialFileSettingsLatch = new CountDownLatch(1);

        fileWatchService.startWatcher(processFileSettingsLatch::countDown, noInitialFileSettingsLatch::countDown);

        // we modify the timestamp of the file, it should trigger a change
        Instant now = LocalDateTime.now(ZoneId.systemDefault()).toInstant(ZoneOffset.ofHours(0));
        Files.setLastModifiedTime(fileWatchService.operatorSettingsFile(), FileTime.from(now));

        assertTrue(processFileSettingsLatch.await(30, TimeUnit.SECONDS));
        assertThat(noInitialFileSettingsLatch.getCount(), equalTo(1L));
    }

    public void testStartWatcherWithoutInitialFileSettings() throws Exception {

        // startup latch = process a settings file latch
        CountDownLatch processFileSettingsLatch = new CountDownLatch(1);

        // listener latch = no setting file present latch
        CountDownLatch noInitialFileSettingsLatch = new CountDownLatch(1);

        fileWatchService.startWatcher(processFileSettingsLatch::countDown, noInitialFileSettingsLatch::countDown);

        assertTrue(noInitialFileSettingsLatch.await(30, TimeUnit.SECONDS));

        writeTestFile(fileWatchService.operatorSettingsFile(), "{}");

        assertTrue(processFileSettingsLatch.await(30, TimeUnit.SECONDS));
    }

    public void testStopWatcher() throws Exception {
        writeTestFile(fileWatchService.operatorSettingsFile(), "{}");
        fileWatchService.startWatcher(() -> {}, () -> {});
        assertTrue(fileWatchService.watching());

        fileWatchService.stopWatcher();
        assertFalse(fileWatchService.watching());
    }

    private void writeTestFile(Path path, String contents) throws IOException {
        Path tempFilePath = createTempFile();

        Files.write(tempFilePath, contents.getBytes(StandardCharsets.UTF_8));
        Files.move(tempFilePath, path, StandardCopyOption.ATOMIC_MOVE);
    }
}
