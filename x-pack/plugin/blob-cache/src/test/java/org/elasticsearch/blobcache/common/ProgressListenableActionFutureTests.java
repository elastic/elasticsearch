/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ProgressListenableActionFutureTests extends ESTestCase {

    public void testOnResponseCallsListeners() {
        final ProgressListenableActionFuture future = randomFuture();

        final AtomicArray<Long> listenersResponses = new AtomicArray<>(between(0, 50));
        for (int i = 0; i < listenersResponses.length(); i++) {
            final int listenerIndex = i;
            future.addListener(
                ActionListener.wrap(
                    progress -> listenersResponses.setOnce(listenerIndex, progress),
                    e -> listenersResponses.setOnce(listenerIndex, null)
                ),
                randomLongBetween(future.start + 1L, future.end) // +1 to avoid immediate execution
            );
        }
        assertTrue(listenersResponses.asList().stream().allMatch(Objects::isNull));
        future.onProgress(future.end);
        future.onResponse(future.end);
        assertTrue(listenersResponses.asList().stream().allMatch(value -> value <= future.end));

        final IllegalStateException ise = expectThrows(IllegalStateException.class, () -> future.onResponse(future.end));
        assertThat(ise.getMessage(), containsString("Future is already completed"));
    }

    public void testOnFailureCallsListeners() {
        final ProgressListenableActionFuture future = randomFuture();

        final AtomicArray<Exception> listenersResponses = new AtomicArray<>(between(0, 50));
        for (int i = 0; i < listenersResponses.length(); i++) {
            final int listenerIndex = i;
            future.addListener(
                ActionListener.wrap(
                    o -> listenersResponses.setOnce(listenerIndex, null),
                    e -> listenersResponses.setOnce(listenerIndex, e)
                ),
                randomLongBetween(future.start + 1L, future.end) // +1 to avoid immediate execution
            );
        }
        assertTrue(listenersResponses.asList().stream().allMatch(Objects::isNull));

        final Exception exception = new ElasticsearchException("simulated");
        future.onFailure(exception);

        for (int i = 0; i < listenersResponses.length(); i++) {
            assertThat(listenersResponses.get(i), sameInstance(exception));
        }

        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> future.onFailure(exception));
        assertThat(ise.getMessage(), containsString("Future is already completed"));
    }

    public void testProgressUpdatesCallsListeners() throws Exception {
        final ProgressListenableActionFuture future = randomFuture();

        final Thread[] threads = new Thread[between(1, 5)];
        final CountDownLatch startLatch = new CountDownLatch(1);

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    startLatch.await();
                    while (future.isDone() == false) {
                        final long expectedProgress = randomLongBetween(future.start, future.end);
                        final PlainActionFuture<Long> listener = new PlainActionFuture<>();
                        future.addListener(listener.delegateFailureAndWrap((l, r) -> l.onResponse(r)), expectedProgress);
                        assertThat(listener.get(), greaterThanOrEqualTo(expectedProgress));
                    }
                } catch (Throwable t) {
                    logger.error("Failed to wait for progress to be reached", t);
                    if (future.isDone() == false) {
                        future.onFailure(
                            new Exception("Failed to update progress [" + t.getClass().getName() + ':' + t.getMessage() + "]")
                        );
                    }
                    throw new AssertionError(t);
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        final Thread progressUpdaterThread = new Thread(() -> {
            try {
                startLatch.await();
                long progress = future.start;
                while (progress < future.end) {
                    progress = randomLongBetween(progress + 1L, future.end);
                    future.onProgress(progress);
                }
                future.onResponse(future.end);
            } catch (Throwable t) {
                logger.error("Failed to update progress", t);
                if (future.isDone() == false) {
                    future.onFailure(new Exception("Failed to update progress [" + t.getClass().getName() + ':' + t.getMessage() + "]"));
                }
                throw new AssertionError(t);
            }
        });
        progressUpdaterThread.start();

        startLatch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }
        progressUpdaterThread.join();
        assertTrue(future.isDone());
    }

    public void testPartialProgressionThenFailure() throws Exception {
        final ProgressListenableActionFuture future = randomFuture();
        final long limit = randomLongBetween(future.start + 1L, future.end);

        final Set<PlainActionFuture<Long>> completedListeners = new HashSet<>();
        for (long i = 0L; i < between(1, 10); i++) {
            final PlainActionFuture<Long> listener = new PlainActionFuture<>();
            future.addListener(listener.delegateFailureAndWrap((l, r) -> l.onResponse(r)), randomLongBetween(future.start, limit));
            completedListeners.add(listener);
        }

        final Set<PlainActionFuture<Long>> failedListeners = new HashSet<>();
        if (limit < future.end) {
            for (long i = 0L; i < between(1, 10); i++) {
                final PlainActionFuture<Long> listener = new PlainActionFuture<>();
                future.addListener(listener.delegateFailureAndWrap((l, r) -> l.onResponse(r)), randomLongBetween(limit + 1L, future.end));
                failedListeners.add(listener);
            }
        }

        long progress = future.start;
        while (progress < limit) {
            progress = randomLongBetween(progress + 1L, limit);
            future.onProgress(progress);
        }

        final ElasticsearchException exception = new ElasticsearchException("Failure at " + limit);
        future.onFailure(exception);
        assertTrue(future.isDone());

        for (PlainActionFuture<Long> completedListener : completedListeners) {
            assertThat(completedListener.isDone(), is(true));
            assertThat(completedListener.actionGet(), lessThanOrEqualTo(limit));
        }

        for (PlainActionFuture<Long> failedListener : failedListeners) {
            assertThat(failedListener.isDone(), is(true));
            assertThat(expectThrows(ElasticsearchException.class, failedListener::actionGet), sameInstance(exception));
        }
    }

    public void testListenerCalledImmediatelyAfterResponse() throws Exception {
        final ProgressListenableActionFuture future = randomFuture();
        future.onProgress(future.end);
        future.onResponse(future.end);
        assertTrue(future.isDone());

        final SetOnce<Long> listenerResponse = new SetOnce<>();
        final SetOnce<Exception> listenerFailure = new SetOnce<>();

        future.addListener(ActionListener.wrap(listenerResponse::set, listenerFailure::set), randomLongBetween(future.start, future.end));

        assertThat(listenerResponse.get(), equalTo(future.get()));
        assertThat(listenerFailure.get(), nullValue());
    }

    public void testListenerCalledImmediatelyAfterFailure() {
        final ProgressListenableActionFuture future = randomFuture();

        final Exception failure = new ElasticsearchException("simulated");
        future.onFailure(failure);
        assertTrue(future.isDone());

        final SetOnce<Exception> listenerFailure = new SetOnce<>();
        final SetOnce<Long> listenerResponse = new SetOnce<>();

        future.addListener(ActionListener.wrap(listenerResponse::set, listenerFailure::set), randomLongBetween(future.start, future.end));

        assertThat(listenerFailure.get(), sameInstance(failure));
        assertThat(listenerResponse.get(), nullValue());
    }

    public void testListenerCalledImmediatelyWhenProgressReached() {
        final ProgressListenableActionFuture future = randomFuture();
        final long progress = randomLongBetween(future.start, future.end);

        final PlainActionFuture<Long> listenerResponse = new PlainActionFuture<>();
        if (randomBoolean()) {
            future.onProgress(progress);
            future.addListener(listenerResponse, randomLongBetween(future.start, progress));
        } else {
            future.addListener(listenerResponse, randomLongBetween(future.start, progress));
            future.onProgress(progress);
        }

        assertThat(listenerResponse.isDone(), is(true));
        assertThat(listenerResponse.actionGet(), equalTo(progress));

        future.onProgress(future.end);
        future.onResponse(future.end);
        assertThat(future.isDone(), is(true));
    }

    public void testLongConsumerCalledOnProgressUpdate() {
        // min length of 2 to have at least one progress update before reaching the end
        long length = randomLongBetween(2L, ByteSizeUnit.TB.toBytes(1L));
        long start = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE - length);
        long end = start + length;

        var consumed = new HashSet<Long>();
        var future = new ProgressListenableActionFuture(
            start,
            end,
            p -> assertThat("LongConsumer should not consumed the same value twice", consumed.add(p), equalTo(true))
        );

        long position = start;
        int iters = randomIntBetween(10, 25);
        for (int i = 0; i < iters && position < end - 1L; i++) {
            var progress = randomLongBetween(position + 1L, end - 1L);

            var listener = new PlainActionFuture<Long>();
            future.addListener(
                ActionListener.runBefore(
                    listener,
                    () -> assertThat(
                        "LongConsumer should have been called before listener completion",
                        consumed.contains(progress),
                        equalTo(true)
                    )
                ),
                randomLongBetween(position + 1L, progress)
            );
            future.onProgress(progress);

            assertThat(consumed.contains(progress), equalTo(true));
            assertThat(listener.isDone(), equalTo(true));
            position = progress;
        }
        future.onProgress(end);
        assertThat("LongConsumer is not called when progress is updated to the end", consumed.contains(end), equalTo(false));
    }

    public void testSplit() {
        final ProgressListenableActionFuture future = randomFuture();
        assertTrue("randomFuture must produce a range of at least 3", future.end - future.start >= 3);

        final long splitPoint = randomLongBetween(future.start + 1L, future.end - 1L);

        // Threshold strictly inside lower's range (or at splitPoint if lower has length 1)
        final long thresholdInLower = splitPoint > future.start + 1L ? randomLongBetween(future.start + 1L, splitPoint - 1L) : splitPoint;
        final PlainActionFuture<Long> listenerInLower = new PlainActionFuture<>();

        // Threshold at the split boundary
        final PlainActionFuture<Long> listenerAtSplit = new PlainActionFuture<>();

        // Threshold strictly inside upper's range (or at end if upper has length 1)
        final long thresholdInUpper = future.end > splitPoint + 1L ? randomLongBetween(splitPoint + 1L, future.end - 1L) : future.end;
        final PlainActionFuture<Long> listenerInUpper = new PlainActionFuture<>();

        // Threshold at the very end
        final PlainActionFuture<Long> listenerAtEnd = new PlainActionFuture<>();

        boolean addListenersBeforeSplit = randomBoolean();
        if (addListenersBeforeSplit) {
            future.addListener(listenerInLower, thresholdInLower);
            future.addListener(listenerAtSplit, splitPoint);
            future.addListener(listenerInUpper, thresholdInUpper);
            future.addListener(listenerAtEnd, future.end);
        }

        final ProgressListenableActionFuture[] parts = future.split(splitPoint);
        final ProgressListenableActionFuture lower = parts[0];
        final ProgressListenableActionFuture upper = parts[1];

        if (!addListenersBeforeSplit) {
            future.addListener(listenerInLower, thresholdInLower);
            future.addListener(listenerAtSplit, splitPoint);
            future.addListener(listenerInUpper, thresholdInUpper);
            future.addListener(listenerAtEnd, future.end);
        }

        assertThat(lower.start, equalTo(future.start));
        assertThat(lower.end, equalTo(splitPoint));
        assertThat(upper.start, equalTo(splitPoint));
        assertThat(upper.end, equalTo(future.end));
        assertFalse(lower.isDone());
        assertFalse(upper.isDone());
        assertFalse(future.isDone());

        if (randomBoolean()) {
            // Case A: lower fills first, then upper — upper's per-byte forwarding fires listenerInUpper
            fillHalf(lower);
            assertThat(
                "listener in lower fires at or above its threshold",
                listenerInLower.actionGet(),
                greaterThanOrEqualTo(thresholdInLower)
            );
            assertThat("listener at split fires when lower completes", listenerAtSplit.actionGet(), equalTo(splitPoint));
            assertFalse("listener in upper must not fire before lower completes", listenerInUpper.isDone());
            assertFalse(listenerAtEnd.isDone());
            assertFalse(future.isDone());

            fillHalf(upper);
            assertThat(
                "listener in upper fires at or above its threshold via forwarding",
                listenerInUpper.actionGet(),
                greaterThanOrEqualTo(thresholdInUpper)
            );
            assertThat("listener at end fires when both halves complete", listenerAtEnd.actionGet(), equalTo(future.end));
            assertTrue(future.isDone());
        } else {
            // Case B: upper fills and completes first — upper's consumer is gated on lower.isDone() so
            // nothing forwards to future until lower is done; listeners fire via onResponse(end) at the end
            fillHalf(upper);
            assertFalse("no future listener should fire while lower is still pending", listenerInLower.isDone());
            assertFalse(listenerAtSplit.isDone());
            assertFalse(listenerInUpper.isDone());
            assertFalse(listenerAtEnd.isDone());
            assertFalse(future.isDone());

            fillHalf(lower);
            assertThat(
                "listener in lower fires at or above its threshold",
                listenerInLower.actionGet(),
                greaterThanOrEqualTo(thresholdInLower)
            );
            // listenerAtSplit fires at upper.progress (>= splitPoint), caught up via onProgressAtLeast
            assertThat(
                "listener at split fires when lower completes, at or above splitPoint",
                listenerAtSplit.actionGet(),
                greaterThanOrEqualTo(splitPoint)
            );
            // Both halves now done: listener in upper fires via onProgressAtLeast catch-up or via onResponse(end)
            assertThat(
                "listener in upper fires at or above its threshold",
                listenerInUpper.actionGet(),
                greaterThanOrEqualTo(thresholdInUpper)
            );
            assertThat("listener at end fires via completion", listenerAtEnd.actionGet(), equalTo(future.end));
            assertTrue(future.isDone());
        }
    }

    private static void fillHalf(ProgressListenableActionFuture half) {
        long step = (half.end - half.start) / 10 + 1;
        for (long p = half.start + 1L; p < half.end; p += step) {
            half.onProgress(p);
        }
        half.onResponse(half.end);
    }

    public void testSplitCatchesUpToUpperProgressWhenLowerCompletes() {
        // Range: [0, end); split at splitPoint so both halves have at least 2 bytes.
        final long splitPoint = randomLongBetween(2L, 9L);
        final long end = splitPoint + randomLongBetween(2L, 9L);
        final ProgressListenableActionFuture future = new ProgressListenableActionFuture(0L, end, null);
        final ProgressListenableActionFuture[] parts = future.split(splitPoint);
        final ProgressListenableActionFuture lower = parts[0];
        final ProgressListenableActionFuture upper = parts[1];

        // Upper advances past splitPoint but does not complete — lower is still pending so progress is not forwarded yet
        final long upperMid = randomLongBetween(splitPoint + 1L, end - 1L);
        upper.onProgress(upperMid);

        // A listener on the outer future at upperMid should not fire yet
        final PlainActionFuture<Long> listener = new PlainActionFuture<>();
        future.addListener(listener, upperMid);
        assertFalse("listener must not fire before lower completes", listener.isDone());

        // Complete lower — onProgressAtLeast must catch up to upper.progress (= upperMid), firing the listener
        lower.onResponse(splitPoint);
        assertTrue("listener must fire when lower completes after upper already reached its threshold", listener.isDone());
        assertFalse("outer future must not be done until upper also completes", future.isDone());

        upper.onResponse(end);
        assertTrue(future.isDone());
    }

    public void testOnProgressAtLeastFiresListeners() {
        final ProgressListenableActionFuture future = randomFuture();
        assertTrue("randomFuture must produce a range of at least 2", future.end - future.start >= 2);

        final long threshold = randomLongBetween(future.start + 1L, future.end - 1L);
        final PlainActionFuture<Long> listener = new PlainActionFuture<>();
        future.addListener(listener, threshold);
        assertFalse(listener.isDone());

        future.onProgressAtLeast(randomLongBetween(threshold, future.end - 1L));
        assertTrue("onProgressAtLeast should fire listener once threshold is reached", listener.isDone());
    }

    public void testOnProgressAtLeastIsNoOpWhenAlreadyAdvanced() {
        final ProgressListenableActionFuture future = randomFuture();
        assertTrue("randomFuture must produce a range of at least 3", future.end - future.start >= 3);

        // Advance to some mid-point
        final long mid = randomLongBetween(future.start + 1L, future.end - 2L);
        future.onProgress(mid);

        // Add a listener above the current progress — it should NOT fire via onProgressAtLeast below
        final long aboveThreshold = randomLongBetween(mid + 1L, future.end - 1L);
        final PlainActionFuture<Long> listener = new PlainActionFuture<>();
        future.addListener(listener, aboveThreshold);
        assertFalse(listener.isDone());

        // onProgressAtLeast with a value <= current progress is a no-op
        future.onProgressAtLeast(randomLongBetween(future.start + 1L, mid));
        assertFalse("onProgressAtLeast must be a no-op when progress already advanced past the value", listener.isDone());

        future.onProgress(future.end);
        future.onResponse(future.end);

        assertTrue(listener.isDone());
    }

    private static ProgressListenableActionFuture randomFuture() {
        final long delta = randomLongBetween(3L, ByteSizeUnit.TB.toBytes(1L));
        final long start = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE - delta);
        return new ProgressListenableActionFuture(start, start + delta, null);
    }
}
