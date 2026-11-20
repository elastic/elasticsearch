/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * Reads from a {@link StreamStorage} and waits for rows, the counterpart of {@code AsyncTaskManagementService}.
 * <p>
 * The wait follows the rules of {@code wait_for_completion_timeout} of the async API: a timeout that is not set, zero or negative does
 * not wait, a positive timeout is a total deadline, and reaching it is not an error. One thing differs on purpose: a stream may stay
 * running for a long time while it keeps adding pages, so the wait does not wait for completion. It ends as soon as a read returns rows or
 * the stream is no longer running.
 * <p>
 * There is no task to listen to, so waiting polls the storage on the scheduler and never blocks a thread.
 */
final class StreamTaskManagementService {

    static final long INITIAL_POLL_INTERVAL_MILLIS = 50;
    static final long MAX_POLL_INTERVAL_MILLIS = 250;
    /** Longest wait that keeps {@code System.nanoTime() + wait} comparable by subtraction. */
    static final long MAX_WAIT_NANOS = Long.MAX_VALUE / 2;

    private static final Logger logger = LogManager.getLogger(StreamTaskManagementService.class);

    private StreamTaskManagementService() {}

    /**
     * Reads the metadata and then the pages of {@code [offset, offset + rows)}, once. The metadata is read first, so a terminal status
     * guarantees that an empty read is the end of the result.
     * <p>
     * Fails with {@link ResourceNotFoundException} if the stream is unknown or expired.
     */
    static void getCurrentResult(StreamStorage store, String id, long offset, long rows, ActionListener<StreamPages> listener) {
        read(store, id, null, offset, rows, listener);
    }

    /**
     * One read: the metadata, unless the caller has just read it and passes it as {@code known}, and then the pages.
     */
    private static void read(
        StreamStorage store,
        String id,
        @Nullable StreamMetadata known,
        long offset,
        long rows,
        ActionListener<StreamPages> listener
    ) {
        ActionListener<StreamPages> uniform = uniformNotFound(id, listener);
        if (known != null) {
            ActionListener.run(uniform, l -> readPages(store, id, known, offset, rows, l));
        } else {
            store.getMetadata(id, uniform.delegateFailureAndWrap((l, metadata) -> readPages(store, id, metadata, offset, rows, l)));
        }
    }

    private static void readPages(
        StreamStorage store,
        String id,
        @Nullable StreamMetadata metadata,
        long offset,
        long rows,
        ActionListener<StreamPages> listener
    ) {
        if (metadata == null || metadata.expiration().toEpochMilli() < System.currentTimeMillis()) {
            throw new ResourceNotFoundException(id);
        }
        store.getPages(id, offset, rows, listener.delegateFailureAndWrap((l, pages) -> {
            StreamPages read = new StreamPages(metadata, pages);
            try {
                l.onResponse(read);
            } catch (Exception e) {
                // Whoever receives the pages owns them, but a failure on the way must not leak them. Closing releases only the pages
                // that nobody took.
                read.close();
                throw e;
            }
        }));
    }

    /**
     * Makes every "not found" look the same. The storage may report an unknown stream with its own message or exception, for example
     * when the stream is deleted between the metadata read and the page read. A caller must not be able to tell that case from an
     * unknown, expired or not owned stream, which all fail with the bare id. The original failure is logged, because a storage that is
     * broken (a missing index, say) also reports "not found".
     */
    static <T> ActionListener<T> uniformNotFound(String id, ActionListener<T> listener) {
        return listener.delegateResponse((l, e) -> {
            if (ExceptionsHelper.status(e) == RestStatus.NOT_FOUND) {
                logger.debug(() -> Strings.format("stream [%s] was not found", id), e);
                l.onFailure(new ResourceNotFoundException(id));
            } else {
                l.onFailure(e);
            }
        });
    }

    /**
     * Like {@link #getCurrentResult}, but when {@code timeout} is positive and a read finds no rows in a running stream, reads again
     * until rows are found, the stream stops running or the timeout elapses. The timeout is a total deadline that never resets.
     */
    static void addCompletionListener(
        ThreadPool threadPool,
        CancellableTask task,
        StreamStorage store,
        String id,
        long offset,
        long rows,
        TimeValue timeout,
        ActionListener<StreamPages> listener
    ) {
        addCompletionListener(threadPool, task, store, id, null, offset, rows, timeout, listener);
    }

    /**
     * Like the method above, for a caller that has just read {@code metadata} itself: the first read uses it and goes straight to the
     * pages, so the metadata is not read twice in a row. Every later read of a wait reads the metadata again.
     */
    static void addCompletionListener(
        ThreadPool threadPool,
        CancellableTask task,
        StreamStorage store,
        String id,
        @Nullable StreamMetadata metadata,
        long offset,
        long rows,
        TimeValue timeout,
        ActionListener<StreamPages> listener
    ) {
        if (timeout.nanos() <= 0) {
            read(store, id, metadata, offset, rows, listener);
            return;
        }
        // Capped so that a huge timeout cannot overflow the deadline, which would end the wait at once.
        long deadlineNanos = System.nanoTime() + Math.min(timeout.nanos(), MAX_WAIT_NANOS);
        attempt(threadPool, task, store, id, metadata, offset, rows, deadlineNanos, INITIAL_POLL_INTERVAL_MILLIS, listener);
    }

    private static void attempt(
        ThreadPool threadPool,
        CancellableTask task,
        StreamStorage store,
        String id,
        @Nullable StreamMetadata metadata,
        long offset,
        long rows,
        long deadlineNanos,
        long pollIntervalMillis,
        ActionListener<StreamPages> listener
    ) {
        if (task != null && task.notifyIfCancelled(listener)) {
            return;
        }
        ActionListener<StreamPages> onRead = listener.delegateFailureAndWrap((l, chunk) -> {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (chunk.pages().isEmpty() == false || chunk.isTerminal() || remainingNanos <= 0) {
                l.onResponse(chunk);
                return;
            }
            // Nothing to return yet and the stream is still running: poll again, never later than the deadline.
            long delayMillis = Math.min(pollIntervalMillis, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
            long nextInterval = Math.min(pollIntervalMillis * 2, MAX_POLL_INTERVAL_MILLIS);
            threadPool.schedule(
                ActionRunnable.wrap(l, ignored -> attempt(threadPool, task, store, id, null, offset, rows, deadlineNanos, nextInterval, l)),
                TimeValue.timeValueMillis(delayMillis),
                threadPool.generic()
            );
        });
        read(store, id, metadata, offset, rows, onRead);
    }
}
