/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FileWatchServiceTests extends ESTestCase {

    private FileWatchService fileWatchService;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        Path directory = createTempDir();
        String filename = "settings.json";
        fileWatchService = new FileWatchService(directory, filename);
    }

    // TODO[wrb]: Move to file watch service tests
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

        Files.createDirectories(service.operatorSettingsDir);

        var mockedPath = spy(service.operatorSettingsDir);
        var prevWatchKey = mock(WatchKey.class);
        var newWatchKey = mock(WatchKey.class);

        doThrow(new IOException("can't register")).doThrow(new IOException("can't register - attempt 2"))
            .doAnswer(i -> newWatchKey)
            .when(mockedPath)
            .register(any(), any());

        var result = service.enableSettingsWatcher(prevWatchKey, mockedPath);
        assertNotNull(result);
        assertTrue(result != prevWatchKey);

        verify(service, times(2)).retryDelayMillis(anyInt());
    }

    public void testOperatorSettingsDir() {
        // TODO[wrb]: add test
    }

    public void testOperatorSettingsFile() {
        // TODO[wrb]: add test
    }

    public void testWatchedFileChanged() {
        // TODO[wrb]: add test
    }

    public void testWatchService() {
        // TODO[wrb]: add test
    }

    public void testDoStart() {
        // TODO[wrb]: add test
    }

    public void testDoStop() {
        // TODO[wrb]: add test
    }

    public void testDoClose() {
        // TODO[wrb]: add test
    }

    public void testSetActive() {
        // TODO[wrb]: add test
    }

    public void testIsActive() {
        // TODO[wrb]: add test
    }

    public void testWatching() {
        // TODO[wrb]: add test
    }

    public void testStartWatcher() {
        // TODO[wrb]: add test
    }

    public void testWatcherThread() {
        // TODO[wrb]: add test
    }

    public void testStopWatcher() {
        // TODO[wrb]: add test
    }

    public void testRetryDelayMillis() {
        // TODO[wrb]: add test
    }

    public void testEnableSettingsWatcher() {
        // TODO[wrb]: add test
    }
}
