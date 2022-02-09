/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.common;

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
                        future.addListener(ActionListener.wrap(listener::onResponse, listener::onFailure), expectedProgress);
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
            future.addListener(ActionListener.wrap(listener::onResponse, listener::onFailure), randomLongBetween(future.start, limit));
            completedListeners.add(listener);
        }

        final Set<PlainActionFuture<Long>> failedListeners = new HashSet<>();
        if (limit < future.end) {
            for (long i = 0L; i < between(1, 10); i++) {
                final PlainActionFuture<Long> listener = new PlainActionFuture<>();
                future.addListener(
                    ActionListener.wrap(listener::onResponse, listener::onFailure),
                    randomLongBetween(limit + 1L, future.end)
                );
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

        final PlainActionFuture<Long> listenerResponse = PlainActionFuture.newFuture();
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

    private static ProgressListenableActionFuture randomFuture() {
        final long delta = randomLongBetween(1L, ByteSizeUnit.TB.toBytes(1L));
        final long start = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE - delta);
        return new ProgressListenableActionFuture(start, start + delta);
    }
}
