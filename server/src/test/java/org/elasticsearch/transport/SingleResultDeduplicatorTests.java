/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.SingleResultDeduplicator;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class SingleResultDeduplicatorTests extends ESTestCase {

    public void testDeduplicatesWithoutShowingStaleData() {
        final SetOnce<ActionListener<Object>> firstListenerRef = new SetOnce<>();
        final SetOnce<ActionListener<Object>> secondListenerRef = new SetOnce<>();
        final var deduplicator = new SingleResultDeduplicator<>(new ThreadContext(Settings.EMPTY), l -> {
            if (firstListenerRef.trySet(l) == false) {
                secondListenerRef.set(l);
            }
        });
        final Object result1 = new Object();
        final Object result2 = new Object();

        final int totalListeners = randomIntBetween(2, 10);
        final boolean[] called = new boolean[totalListeners];
        deduplicator.execute(ActionTestUtils.assertNoFailureListener(response -> {
            assertFalse(called[0]);
            called[0] = true;
            assertEquals(result1, response);
        }));

        for (int i = 1; i < totalListeners; i++) {
            final int index = i;
            deduplicator.execute(ActionTestUtils.assertNoFailureListener(response -> {
                assertFalse(called[index]);
                called[index] = true;
                assertEquals(result2, response);
            }));
        }
        for (int i = 0; i < totalListeners; i++) {
            assertFalse(called[i]);
        }
        firstListenerRef.get().onResponse(result1);
        assertTrue(called[0]);
        for (int i = 1; i < totalListeners; i++) {
            assertFalse(called[i]);
        }
        secondListenerRef.get().onResponse(result2);
        for (int i = 0; i < totalListeners; i++) {
            assertTrue(called[i]);
        }
    }

    public void testThreadContextPreservation() {
        final var resources = new Releasable[1];
        try {

            final var workerResponseHeaderValueCounter = new AtomicInteger();
            final List<Integer> allSeenResponseHeaderValues = Collections.synchronizedList(new ArrayList<>());
            final List<Integer> allSeenThreadHeaderValues = Collections.synchronizedList(new ArrayList<>());

            final var threads = between(1, 5);
            final var future = new PlainActionFuture<Void>();
            try (var listeners = new RefCountingListener(future)) {
                final var workerRequestHeaderName = "worker-request-header";
                final var workerResponseHeaderName = "worker-response-header";
                final var threadHeaderName = "test-header";
                final var threadContext = new ThreadContext(Settings.EMPTY);
                final var deduplicator = new SingleResultDeduplicator<Void>(threadContext, l -> {
                    threadContext.putHeader(workerRequestHeaderName, randomAlphaOfLength(5));
                    threadContext.addResponseHeader(
                        workerResponseHeaderName,
                        String.valueOf(workerResponseHeaderValueCounter.getAndIncrement())
                    );
                    allSeenThreadHeaderValues.add(Integer.valueOf(threadContext.getHeader(threadHeaderName)));
                    l.onResponse(null);
                });
                final var executor = EsExecutors.newFixed(
                    "test",
                    threads,
                    0,
                    EsExecutors.daemonThreadFactory("test"),
                    threadContext,
                    EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
                );
                resources[0] = () -> ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
                final var barrier = new CyclicBarrier(threads);
                for (int i = 0; i < threads; i++) {
                    try (var ignored = threadContext.stashContext()) {
                        final var threadHeaderValue = String.valueOf(i);
                        threadContext.putHeader(threadHeaderName, threadHeaderValue);
                        executor.execute(ActionRunnable.wrap(listeners.<Void>acquire(v -> {
                            // original request header before the work execution should be preserved
                            assertEquals(threadHeaderValue, threadContext.getHeader(threadHeaderName));
                            // request header used by the work execution should *not* be preserved
                            assertThat(threadContext.getHeaders(), not(hasKey(workerRequestHeaderName)));
                            // response header should be preserved which is from a single execution of the work
                            final List<String> responseHeader = threadContext.getResponseHeaders().get(workerResponseHeaderName);
                            assertThat(responseHeader, hasSize(1));
                            allSeenResponseHeaderValues.add(Integer.valueOf(responseHeader.get(0)));
                        }), listener -> {
                            safeAwait(barrier);
                            deduplicator.execute(listener);
                        }));
                    }
                }
            }
            future.actionGet(10, TimeUnit.SECONDS);
            assertThat(allSeenResponseHeaderValues, hasSize(threads));
            // The total number of observed response header values consistent with how many times it is generated
            assertThat(
                Set.copyOf(allSeenResponseHeaderValues),
                equalTo(IntStream.range(0, workerResponseHeaderValueCounter.get()).boxed().collect(Collectors.toUnmodifiableSet()))
            );
            // The following proves each work execution will see a different thread's context in that execution batch
            assertThat(Set.copyOf(allSeenThreadHeaderValues), hasSize(workerResponseHeaderValueCounter.get()));
        } finally {
            Releasables.closeExpectNoException(resources);
        }
    }
}
