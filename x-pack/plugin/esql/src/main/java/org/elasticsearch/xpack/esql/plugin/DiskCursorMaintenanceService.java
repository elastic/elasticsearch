/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Periodically scans the disk cursor directory and deletes expired cursor data.
 */
public class DiskCursorMaintenanceService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(DiskCursorMaintenanceService.class);
    private static final TimeValue CLEANUP_INTERVAL = TimeValue.timeValueHours(1);

    private final DiskCursorStorageService storageService;
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable cancellable;

    public DiskCursorMaintenanceService(DiskCursorStorageService storageService, ThreadPool threadPool) {
        this.storageService = storageService;
        this.threadPool = threadPool;
    }

    @Override
    protected void doStart() {
        scheduleNextCleanup();
    }

    @Override
    protected void doStop() {
        if (cancellable != null && cancellable.isCancelled() == false) {
            cancellable.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {}

    private void scheduleNextCleanup() {
        try {
            cancellable = threadPool.schedule(this::runCleanup, CLEANUP_INTERVAL, threadPool.generic());
        } catch (EsRejectedExecutionException e) {
            if (e.isExecutorShutdown()) {
                logger.debug("failed to schedule disk cursor cleanup; shutting down", e);
            } else {
                throw e;
            }
        }
    }

    private void runCleanup() {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }
        Path basePath = storageService.basePath();
        if (Files.exists(basePath) == false) {
            scheduleNextCleanup();
            return;
        }
        long now = System.currentTimeMillis();
        try (DirectoryStream<Path> dirs = Files.newDirectoryStream(basePath)) {
            for (Path cursorDir : dirs) {
                if (Files.isDirectory(cursorDir) == false) {
                    continue;
                }
                Path metaFile = cursorDir.resolve("meta.bin");
                if (Files.exists(metaFile) == false) {
                    continue;
                }
                try {
                    CursorStorageService.CursorMetadata metadata = storageService.readMetaFile(cursorDir);
                    if (metadata.expirationTimeMillis() > 0 && now >= metadata.expirationTimeMillis()) {
                        logger.debug("deleting expired disk cursor [{}]", cursorDir.getFileName());
                        IOUtils.rm(cursorDir);
                    }
                } catch (Exception e) {
                    logger.warn("failed to read metadata for cursor dir [{}], skipping", cursorDir.getFileName(), e);
                }
            }
        } catch (IOException e) {
            logger.warn("failed to scan disk cursor directory [{}]", basePath, e);
        }
        scheduleNextCleanup();
    }
}
