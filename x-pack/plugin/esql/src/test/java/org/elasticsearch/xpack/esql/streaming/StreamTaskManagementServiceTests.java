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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The counterpart of {@code AsyncTaskManagementServiceTests}, for {@code wait_timeout}.
 * <p>
 * Async waits for completion. The stream GET never does: it returns what {@code getPages} returns, and {@code wait_timeout} only bounds
 * how long it waits when there are no rows yet.
 * <p>
 * The tests do not depend on timing. A test that changes the stream during a wait first waits for a read that found no rows (see
 * {@link PollingStore}), so the wait is known to be polling when the change happens. A test that expects no wait counts the reads. And
 * a wait that should end early uses {@link #LONG_WAIT}, which is longer than the time {@code safeGet} waits for a result, so a wait that
 * wrongly runs to its deadline fails the test.
 */
public class StreamTaskManagementServiceTests extends AbstractStreamTestCase {

    private static final long CHUNK = 100;

    /** Longer than {@link #SAFE_AWAIT_TIMEOUT}: a wait with this timeout has to end for another reason, or the test fails. */
    private static final TimeValue LONG_WAIT = TimeValue.timeValueSeconds(60);

    /**
     * Observes the reads of a wait and passes them to the real store. It counts them, and it hands out one permit for every read that
     * found no rows, so that a test can change the stream only after the wait has started to poll.
     */
    private static final class PollingStore extends DelegatingStreamStorage {
        private final AtomicInteger reads = new AtomicInteger();
        private final Semaphore emptyReads = new Semaphore(0);

        PollingStore(StreamStorage delegate) {
            super(delegate);
        }

        @Override
        public void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener) {
            reads.incrementAndGet();
            super.getPages(streamId, offset, rows, listener.map(pages -> {
                if (pages.isEmpty()) {
                    emptyReads.release();
                }
                return pages;
            }));
        }

        /** Blocks until {@code count} more reads have found no rows. */
        void awaitEmptyReads(int count) {
            safeAcquire(count, emptyReads);
        }

        int reads() {
            return reads.get();
        }
    }

    private PlainActionFuture<StreamPages> wait(CancellableTask task, StreamStorage storage, String id, long offset, TimeValue timeout) {
        PlainActionFuture<StreamPages> future = new PlainActionFuture<>();
        StreamTaskManagementService.addCompletionListener(threadPool, task, storage, id, offset, CHUNK, timeout, future);
        return future;
    }

    private PlainActionFuture<StreamPages> wait(StreamStorage storage, String id, long offset, TimeValue timeout) {
        return wait(newTask(), storage, id, offset, timeout);
    }

    private Exception waitFailure(PlainActionFuture<StreamPages> future) {
        return expectThrows(Exception.class, () -> future.actionGet(SAFE_AWAIT_TIMEOUT));
    }

    // ---- no wait: the first read is the answer

    public void testNoWaitWhenTheTimeoutIsZeroOrNegative() {
        createStream("s");
        for (TimeValue timeout : new TimeValue[] { TimeValue.MINUS_ONE, TimeValue.ZERO }) {
            PollingStore polling = new PollingStore(store);
            try (StreamPages chunk = safeGet(wait(polling, "s", 0, timeout))) {
                assertThat(chunk.pages(), equalTo(List.of()));
                assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.RUNNING));
            }
            assertThat("no second read for " + timeout, polling.reads(), equalTo(1));
        }
    }

    public void testRowsAlreadyStoredAreReturnedAtOnce() {
        createStream("s").append(3);
        PollingStore polling = new PollingStore(store);
        try (StreamPages chunk = safeGet(wait(polling, "s", 0, LONG_WAIT))) {
            // asked for CHUNK rows, only 3 exist: they come back, the wait does not try to fill the request
            assertThat(chunk.rows(), equalTo(3L));
            assertThat(chunk.isTerminal(), equalTo(false));
        }
        assertThat(polling.reads(), equalTo(1));
    }

    public void testAlreadyTerminalDoesNotWait() {
        createStream("s").append(2).complete();
        PollingStore polling = new PollingStore(store);
        // offset 2 is the end of the result: no rows, and no more will come
        try (StreamPages chunk = safeGet(wait(polling, "s", 2, LONG_WAIT))) {
            assertThat(chunk.pages(), equalTo(List.of()));
            assertThat(chunk.isTerminal(), equalTo(true));
        }
        assertThat(polling.reads(), equalTo(1));
    }

    // ---- what ends a wait

    public void testRowsArrivingEndTheWaitWhileTheStreamIsStillRunning() {
        Writer writer = createStream("s").append(2);
        PollingStore polling = new PollingStore(store);
        // Read from offset 2, after the rows that are already stored, so the wait has to poll until the second page arrives.
        PlainActionFuture<StreamPages> future = wait(polling, "s", 2, LONG_WAIT);
        polling.awaitEmptyReads(1);
        writer.append(2);
        try (StreamPages chunk = safeGet(future)) {
            // the wait ends with the new page, it does not wait for the stream to complete
            assertThat(chunk.rows(), equalTo(2L));
            assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.RUNNING));
        }
    }

    public void testHugeTimeoutStillWaitsForRows() {
        Writer writer = createStream("s");
        PollingStore polling = new PollingStore(store);
        // a deadline that overflows would be in the past, and would end the wait at the first read that finds no rows
        PlainActionFuture<StreamPages> future = wait(polling, "s", 0, new TimeValue(Long.MAX_VALUE, TimeUnit.NANOSECONDS));
        polling.awaitEmptyReads(1);
        writer.append(5);
        try (StreamPages chunk = safeGet(future)) {
            assertThat(chunk.rows(), equalTo(5L));
        }
    }

    public void testReturnAfterTimeout() {
        Writer writer = createStream("s");
        long start = System.nanoTime();
        try (StreamPages chunk = safeGet(wait(store, "s", 0, TimeValue.timeValueMillis(300)))) {
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            // a timeout is not an error: the running result comes back, with no rows
            assertThat(chunk.pages(), equalTo(List.of()));
            assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.RUNNING));
            // the deadline is set after "start" and nothing but the deadline can end this wait
            assertThat("waited for the timeout", elapsedMillis, greaterThanOrEqualTo(300L));
        }
        // rows that arrive afterwards are there for the next GET
        writer.append(4);
        try (StreamPages chunk = safeGet(wait(store, "s", 0, TimeValue.ZERO))) {
            assertThat(chunk.rows(), equalTo(4L));
        }
    }

    public void testCompletionEndsTheWait() {
        Writer writer = createStream("s");
        PollingStore polling = new PollingStore(store);
        PlainActionFuture<StreamPages> future = wait(polling, "s", 0, LONG_WAIT);
        polling.awaitEmptyReads(1);
        writer.complete();
        try (StreamPages chunk = safeGet(future)) {
            assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.COMPLETE));
            assertThat(chunk.pages(), equalTo(List.of()));
        }
    }

    public void testFailureEndsTheWait() {
        Writer writer = createStream("s");
        PollingStore polling = new PollingStore(store);
        PlainActionFuture<StreamPages> future = wait(polling, "s", 0, LONG_WAIT);
        polling.awaitEmptyReads(1);
        writer.fail(new IllegalStateException("boom"));
        try (StreamPages chunk = safeGet(future)) {
            // the failure of the query is stored data, not a failure of the read
            assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.FAILED));
            assertThat(chunk.metadata().completionInfo().failure().reason(), equalTo("boom"));
            assertThat(chunk.pages(), equalTo(List.of()));
        }
    }

    public void testCancelledStatusEndsTheWait() {
        createStream("s");
        AtomicInteger metadataReads = new AtomicInteger();
        // The storage has no call that cancels a stream, so the status is changed on the way out: running for two reads, then canceled.
        StreamStorage cancelling = new DelegatingStreamStorage(store) {
            @Override
            public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
                boolean cancel = metadataReads.incrementAndGet() > 2;
                super.getMetadata(
                    streamId,
                    listener.map(
                        m -> cancel
                            ? new StreamMetadata(
                                m.streamId(),
                                m.taskId(),
                                m.expiration(),
                                m.columns(),
                                m.headers(),
                                m.warnings(),
                                StreamMetadata.Status.CANCELED,
                                null
                            )
                            : m
                    )
                );
            }
        };
        try (StreamPages chunk = safeGet(wait(cancelling, "s", 0, LONG_WAIT))) {
            assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.CANCELED));
            assertThat(chunk.pages(), equalTo(List.of()));
        }
        assertThat("the wait ends at the first read that sees the status", metadataReads.get(), equalTo(3));
    }

    public void testCancellingTheTaskStopsTheWait() {
        createStream("s");
        CancellableTask task = newTask();
        PollingStore polling = new PollingStore(store);
        PlainActionFuture<StreamPages> future = wait(task, polling, "s", 0, LONG_WAIT);
        polling.awaitEmptyReads(1);
        TaskCancelHelper.cancel(task, "test");
        Exception e = waitFailure(future);
        assertThat(ExceptionsHelper.unwrap(e, TaskCancelledException.class), instanceOf(TaskCancelledException.class));
    }

    // ---- a stream that goes away during a wait is not found, like an unknown id

    public void testStreamDeletedDuringTheWaitIsNotFound() {
        createStream("s");
        PollingStore polling = new PollingStore(store);
        PlainActionFuture<StreamPages> future = wait(polling, "s", 0, LONG_WAIT);
        polling.awaitEmptyReads(1);
        awaitVoid(l -> store.delete("s", l));
        Exception e = waitFailure(future);
        assertThat(e, instanceOf(ResourceNotFoundException.class));
        assertThat(e.getMessage(), equalTo("s"));
    }

    public void testTtlExpiringDuringTheWaitIsNotFound() {
        createStream("s");
        PollingStore polling = new PollingStore(store);
        PlainActionFuture<StreamPages> future = wait(polling, "s", 0, LONG_WAIT);
        polling.awaitEmptyReads(1);
        // move the expiration into the past, instead of sleeping until a short one passes
        awaitVoid(l -> store.extendTtl("s", System.currentTimeMillis() - 1, l));
        Exception e = waitFailure(future);
        assertThat(e, instanceOf(ResourceNotFoundException.class));
        assertThat(e.getMessage(), equalTo("s"));
    }

    public void testNotFoundFromTheStorageDuringTheWaitLooksLikeAnUnknownId() {
        createStream("s");
        AtomicInteger pageReads = new AtomicInteger();
        // Two reads find no rows, and then the storage reports "not found" in its own words, as a real one does when the stream is
        // deleted between the metadata read and the page read.
        StreamStorage failing = new DelegatingStreamStorage(store) {
            @Override
            public void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener) {
                if (pageReads.incrementAndGet() > 2) {
                    listener.onFailure(new ResourceNotFoundException("no such index [.esql-stream-results]"));
                } else {
                    super.getPages(streamId, offset, rows, listener);
                }
            }
        };
        Exception e = waitFailure(wait(failing, "s", 0, LONG_WAIT));
        assertThat(e, instanceOf(ResourceNotFoundException.class));
        assertThat("the words of the storage are not passed on", e.getMessage(), equalTo("s"));
    }

    // ---- how the wait polls

    public void testEveryAttemptReadsTheMetadataBeforeThePages() {
        Writer writer = createStream("s");
        RecordingStore recording = new RecordingStore();
        PollingStore polling = new PollingStore(recording);
        PlainActionFuture<StreamPages> future = wait(polling, "s", 0, LONG_WAIT);
        polling.awaitEmptyReads(2);
        writer.append(1);
        try (StreamPages chunk = safeGet(future)) {
            assertThat(chunk.rows(), equalTo(1L));
        }
        List<String> calls = List.copyOf(recording.calls);
        assertThat("two reads without rows and one with", calls.size(), greaterThanOrEqualTo(6));
        for (int i = 0; i < calls.size(); i += 2) {
            assertThat(calls.get(i), equalTo("metadata"));
            assertThat(calls.get(i + 1), equalTo("pages"));
        }
    }

    public void testTheWaitStillFiresWhenTheSearchPoolIsBusy() {
        // the polling must not run on the SEARCH pool, so a pool that is fully busy cannot delay the timeout
        int threads = threadPool.info(ThreadPool.Names.SEARCH).getMax();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch occupied = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                occupied.countDown();
                safeAwait(release);
            });
        }
        try {
            safeAwait(occupied);
            createStream("s");
            try (StreamPages chunk = safeGet(wait(store, "s", 0, TimeValue.timeValueMillis(200)))) {
                assertThat(chunk.pages(), equalTo(List.of()));
            }
        } finally {
            release.countDown();
        }
    }

    public void testManyWaitersOnOneStreamAllGetTheRows() {
        Writer writer = createStream("s");
        PollingStore polling = new PollingStore(store);
        int waiters = 50;
        List<PlainActionFuture<StreamPages>> futures = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            futures.add(wait(polling, "s", 0, LONG_WAIT));
        }
        polling.awaitEmptyReads(waiters);
        writer.append(1);
        // Every waiter gets its own copy of the page, read on the threads of the pool. The check after the test fails if one of them
        // is not released.
        for (PlainActionFuture<StreamPages> future : futures) {
            try (StreamPages chunk = safeGet(future)) {
                assertThat(chunk.rows(), equalTo(1L));
            }
        }
    }

    // ---- who owns the pages of a read

    public void testPagesAreReleasedWhenTheListenerThrows() {
        createStream("s").append(5);
        AtomicReference<Exception> failure = new AtomicReference<>();
        StreamTaskManagementService.getCurrentResult(store, "s", 0, CHUNK, new ActionListener<>() {
            @Override
            public void onResponse(StreamPages pages) {
                // the receiver fails before it takes the pages
                throw new IllegalStateException("boom");
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        });
        assertThat(failure.get().getMessage(), equalTo("boom"));
        // the breaker check after the test fails if a page was not released
    }

    /**
     * A {@link StreamPages} owns its pages only until they are taken. After that the taker owns them, and closing the
     * {@link StreamPages} must not release them again: releasing a page twice gives back a reference that belongs to someone else.
     */
    public void testClosingAfterThePagesWereTakenReleasesNothing() {
        // The stream stores one page of 5 rows.
        createStream("s").append(5);
        // Read it. "read" now owns one page, a shallow copy that holds its own reference on the stored blocks.
        StreamPages read = safeGet(wait(store, "s", 0, TimeValue.ZERO));

        // Hand the page over, as the response body does. From here "taken" owns the page and "read" owns nothing.
        List<Page> taken = read.takePages();
        assertThat(taken.size(), equalTo(1));
        assertThat(read.pages(), equalTo(List.of()));

        // Close "read", twice, as a failure path that cannot know the pages were taken would do. It has no page left, so this
        // releases nothing. If it still released the page, the release below would be a second one, and it would give back the
        // reference that the store holds on the same blocks.
        read.close();
        read.close();

        // The new owner releases the page, once. The checks after the test then release the store and verify that the breaker is
        // back to zero, so a page that was never released, or released twice, makes the test fail there.
        taken.forEach(Page::releaseBlocks);
    }
}
