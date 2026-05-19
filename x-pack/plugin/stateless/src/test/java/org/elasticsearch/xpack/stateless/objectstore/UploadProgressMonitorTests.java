/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.commits.VirtualBatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.junit.After;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UploadProgressMonitorTests extends ESTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = mock(ThreadPool.class);
    }

    @After
    public void tearDown() throws Exception {
        threadPool = null;
        super.tearDown();
    }

    public void testZeroLogIntervalDisablesTrackingAndScheduling() {
        UploadProgressMonitor monitor = newMonitor(TimeValue.ZERO, false);
        verify(threadPool, never()).scheduleWithFixedDelay(any(), any(), any());

        ByteArrayInputStream in = new ByteArrayInputStream(new byte[] { 1, 2, 3 });
        assertThat(monitor.monitor(in), sameInstance(in));
        assertTrue(monitor.cancel());
        assertTrue(monitor.isCancelled());
    }

    public void testSinglePartTracker() throws IOException {
        UploadProgressMonitor.UploadProgressTracker tracker = UploadProgressMonitor.UploadProgressTracker.newInstance(true, false);
        byte[] data = new byte[100];
        try (InputStream in = tracker.track(new ByteArrayInputStream(data))) {
            byte[] buffer = new byte[40];
            assertThat(in.read(buffer), equalTo(40));
            assertThat(tracker.bytesRead(), equalTo(40L));
            assertThat(tracker.bytesUploaded(), equalTo(0L));

            assertThat(in.read(buffer, 0, 30), equalTo(30));
            assertThat(tracker.bytesRead(), equalTo(70L));
            assertThat(tracker.bytesUploaded(), equalTo(40L));

            in.read();
            assertThat(tracker.bytesRead(), equalTo(71L));
            assertThat(tracker.bytesUploaded(), equalTo(70L));

            assertThat(in.skip(10), equalTo(10L));
            assertThat(tracker.bytesRead(), equalTo(81L));
            assertThat(tracker.bytesUploaded(), equalTo(71L));
        }
        assertThat(tracker.bytesRead(), equalTo(81L));
        assertThat(tracker.bytesUploaded(), equalTo(81L));
    }

    public void testMultiPartTracker() throws IOException {
        UploadProgressMonitor.UploadProgressTracker tracker = UploadProgressMonitor.UploadProgressTracker.newInstance(true, true);
        byte[] buffer = new byte[20];
        try (
            InputStream first = tracker.track(new ByteArrayInputStream(new byte[50]));
            InputStream second = tracker.track(new ByteArrayInputStream(new byte[30]))
        ) {
            assertThat(first.read(buffer, 0, 20), equalTo(20));
            assertThat(second.read(buffer, 0, 20), equalTo(20));
            assertThat(tracker.bytesRead(), equalTo(40L));
            assertThat(tracker.bytesUploaded(), equalTo(0L));

            assertThat(first.read(buffer, 0, 20), equalTo(20));
            assertThat(second.read(buffer, 0, 20), equalTo(10));
            assertThat(tracker.bytesRead(), equalTo(70L));
            assertThat(tracker.bytesUploaded(), equalTo(40L));

            assertThat(first.read(buffer, 0, 20), equalTo(10));
            assertThat(tracker.bytesRead(), equalTo(80L));
            assertThat(tracker.bytesUploaded(), equalTo(60L));
        }
        assertThat(tracker.bytesUploaded(), equalTo(80L));
    }

    @SuppressWarnings("unchecked")
    public void testNewInstanceSchedulesProgressLoggingWithElapsedTime() {
        Logger logger = mock(Logger.class);
        Scheduler.Cancellable loggingTask = mock(Scheduler.Cancellable.class);
        ExecutorService generic = mock(ExecutorService.class);
        when(threadPool.generic()).thenReturn(generic);
        when(threadPool.relativeTimeInNanos()).thenReturn(1_000_000L, 6_000_000_000L);
        ArgumentCaptor<Runnable> progressLogging = ArgumentCaptor.forClass(Runnable.class);
        when(threadPool.scheduleWithFixedDelay(progressLogging.capture(), any(TimeValue.class), eq(generic))).thenReturn(loggingTask);
        when(loggingTask.cancel()).thenReturn(true);

        UploadProgressMonitor monitor = newMonitor(logger, TimeValue.timeValueSeconds(30), false);
        assertFalse(monitor.isCancelled());

        progressLogging.getValue().run();
        ArgumentCaptor<Supplier<String>> logMessage = ArgumentCaptor.forClass(Supplier.class);
        verify(logger).info(logMessage.capture());
        String message = logMessage.getValue().get();
        assertThat(message, containsString("batched compound commit upload progress"));
        assertThat(message, containsString("blob [commits/commit.dat]"));
        assertThat(message, containsString("bytesRead=0/1000"));
        assertThat(message, containsString("bytesUploaded=0/1000"));
        assertThat(message, containsString("elapsed=5999ms"));

        assertTrue(monitor.cancel());
        verify(loggingTask).cancel();
    }

    private UploadProgressMonitor newMonitor(TimeValue logInterval, boolean multipart) {
        return newMonitor(mock(Logger.class), logInterval, multipart);
    }

    private UploadProgressMonitor newMonitor(Logger logger, TimeValue logInterval, boolean multipart) {
        VirtualBatchedCompoundCommit vbcc = mock(VirtualBatchedCompoundCommit.class);
        ShardId shardId = new ShardId("index", "_na_", 0);
        PrimaryTermAndGeneration primaryTermAndGeneration = new PrimaryTermAndGeneration(134, 1250);
        when(vbcc.getShardId()).thenReturn(shardId);
        when(vbcc.getPrimaryTermAndGeneration()).thenReturn(primaryTermAndGeneration);
        when(vbcc.getBlobName()).thenReturn("commit.dat");
        when(vbcc.getTotalSizeInBytes()).thenReturn(1_000L);

        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.path()).thenReturn(BlobPath.EMPTY.add("commits"));

        return UploadProgressMonitor.newInstance(logger, threadPool, blobContainer, vbcc, logInterval, multipart);
    }
}
