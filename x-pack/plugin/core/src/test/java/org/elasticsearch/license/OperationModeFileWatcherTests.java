/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class OperationModeFileWatcherTests extends ESTestCase {
    private ResourceWatcherService watcherService;
    private TestThreadPool threadPool;
    private Path licenseModePath;
    private OperationModeFileWatcher operationModeFileWatcher;
    private AtomicReference<CountDownLatch> onChangeCounter;

    @Before
    public void setup() throws Exception {
        threadPool = new TestThreadPool("license mode file watcher tests");
        Settings settings = Settings.builder()
                .put("resource.reload.interval.high", "10ms")
                .build();
        watcherService = new ResourceWatcherService(settings, threadPool);
        licenseModePath = createTempFile();
        onChangeCounter = new AtomicReference<>(new CountDownLatch(1));
        operationModeFileWatcher = new OperationModeFileWatcher(watcherService, licenseModePath, logger,
                () -> onChangeCounter.get().countDown());
    }

    @After
    public void shutdown() throws InterruptedException {
        watcherService.close();
        terminate(threadPool);
    }

    public void testInit() throws Exception {
        onChangeCounter.set(new CountDownLatch(2));
        writeMode("gold");
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.PLATINUM));
        operationModeFileWatcher.init();
        assertTrue(onChangeCounter.get().await(5, TimeUnit.SECONDS));
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.GOLD));
    }

    public void testUpdateModeFromFile() throws Exception {
        Files.delete(licenseModePath);
        operationModeFileWatcher.init();
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.PLATINUM));
        writeMode("gold");
        assertTrue(onChangeCounter.get().await(5, TimeUnit.SECONDS));
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.GOLD));
        onChangeCounter.set(new CountDownLatch(1));
        writeMode("basic");
        assertTrue(onChangeCounter.get().await(5, TimeUnit.SECONDS));
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.BASIC));
    }

    public void testDeleteModeFromFile() throws Exception {
        Files.delete(licenseModePath);
        operationModeFileWatcher.init();
        writeMode("gold");
        assertTrue(onChangeCounter.get().await(5, TimeUnit.SECONDS));
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.GOLD));
        onChangeCounter.set(new CountDownLatch(1));

        Files.delete(licenseModePath);
        assertTrue(onChangeCounter.get().await(5, TimeUnit.SECONDS));
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.PLATINUM));
    }

    public void testInvalidModeFromFile() throws Exception {
        writeMode("invalid");
        operationModeFileWatcher.init();
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.PLATINUM));
        operationModeFileWatcher.onFileChanged(licenseModePath);
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.PLATINUM));
    }

    public void testLicenseModeFileIsDirectory() throws Exception {
        licenseModePath = createTempDir();
        operationModeFileWatcher.init();
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.PLATINUM));
        operationModeFileWatcher.onFileChanged(licenseModePath);
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.PLATINUM));
    }

    public void testLicenseModeFileCreatedAfterInit() throws Exception {
        Files.delete(licenseModePath);
        operationModeFileWatcher.init();
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.PLATINUM));
        onChangeCounter.set(new CountDownLatch(1));
        Path tempFile = createTempFile();
        writeMode("gold", tempFile);
        licenseModePath = tempFile;
        assertTrue(onChangeCounter.get().await(5, TimeUnit.SECONDS));
        assertThat(operationModeFileWatcher.getCurrentOperationMode(), equalTo(License.OperationMode.GOLD));
    }

    private void writeMode(String mode) throws IOException {
        writeMode(mode, licenseModePath);
    }

    static void writeMode(String mode, Path file) throws IOException {
        Files.write(file, mode.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
    }
}
