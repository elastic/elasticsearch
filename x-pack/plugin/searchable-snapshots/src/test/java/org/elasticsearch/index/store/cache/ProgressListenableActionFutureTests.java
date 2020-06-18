/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.store.cache;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.test.ESTestCase;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ProgressListenableActionFutureTests extends ESTestCase {

    public void testOnResponseCallsListeners() {
        final ProgressListenableActionFuture future = randomFuture();

        final AtomicArray<Long> listenersResponses = new AtomicArray<>(between(0, 50));
        for (int i = 0; i < listenersResponses.length(); i++) {
            final int listenerIndex = i;
            final ActionListener<Long> listener = ActionListener.wrap(
                progress -> listenersResponses.setOnce(listenerIndex, progress),
                e -> listenersResponses.setOnce(listenerIndex, null)
            );

            if (randomBoolean()) {
                future.addListener(listener, randomLongBetween(future.start, future.end));
            } else {
                future.addListener(listener);
            }
        }
        assertTrue(listenersResponses.asList().stream().allMatch(Objects::isNull));
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
            final ActionListener<Long> listener = ActionListener.wrap(
                o -> listenersResponses.setOnce(listenerIndex, null),
                e -> listenersResponses.setOnce(listenerIndex, e)
            );

            if (randomBoolean()) {
                future.addListener(listener, randomLongBetween(future.start, future.end));
            } else {
                future.addListener(listener);
            }
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
                } catch (InterruptedException | ExecutionException e) {
                    throw new AssertionError(e);
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
                    future.onProgress(progress);
                    progress += randomLongBetween(1L, Math.max(1L, future.end - progress));
                }
                future.onResponse(future.end);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
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

    public void testListenerCalledImmediatelyAfterResponse() throws Exception {
        final ProgressListenableActionFuture future = randomFuture();
        future.onResponse(randomLongBetween(future.start, future.end));
        assertTrue(future.isDone());

        {
            final SetOnce<Long> listenerResponse = new SetOnce<>();
            final SetOnce<Exception> listenerFailure = new SetOnce<>();

            future.addListener(ActionListener.wrap(listenerResponse::set, listenerFailure::set));

            assertThat(listenerResponse.get(), equalTo(future.get()));
            assertThat(listenerFailure.get(), nullValue());
        }
        {
            final SetOnce<Long> listenerResponse = new SetOnce<>();
            final SetOnce<Exception> listenerFailure = new SetOnce<>();

            future.addListener(
                ActionListener.wrap(listenerResponse::set, listenerFailure::set),
                randomLongBetween(future.start, future.end)
            );

            assertThat(listenerResponse.get(), equalTo(future.get()));
            assertThat(listenerFailure.get(), nullValue());
        }
    }

    public void testListenerCalledImmediatelyAfterFailure() {
        final ProgressListenableActionFuture future = randomFuture();

        final Exception failure = new ElasticsearchException("simulated");
        future.onFailure(failure);
        assertTrue(future.isDone());

        {
            final SetOnce<Exception> listenerFailure = new SetOnce<>();
            final SetOnce<Long> listenerResponse = new SetOnce<>();

            future.addListener(ActionListener.wrap(listenerResponse::set, listenerFailure::set));

            assertThat(listenerFailure.get(), sameInstance(failure));
            assertThat(listenerResponse.get(), nullValue());
        }
        {
            final SetOnce<Exception> listenerFailure = new SetOnce<>();
            final SetOnce<Long> listenerResponse = new SetOnce<>();

            future.addListener(
                ActionListener.wrap(listenerResponse::set, listenerFailure::set),
                randomLongBetween(future.start, future.end)
            );

            assertThat(listenerFailure.get(), sameInstance(failure));
            assertThat(listenerResponse.get(), nullValue());
        }
    }

    private static ProgressListenableActionFuture randomFuture() {
        final long delta = randomLongBetween(1L, ByteSizeUnit.TB.toBytes(randomIntBetween(1, 10)));
        final long start = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE - delta);
        return new ProgressListenableActionFuture(start, start + delta);
    }
}
