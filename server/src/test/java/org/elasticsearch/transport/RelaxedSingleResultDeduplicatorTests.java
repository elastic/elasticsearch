/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RelaxedSingleResultDeduplicator;
import org.elasticsearch.action.SingleResultDeduplicator;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class RelaxedSingleResultDeduplicatorTests extends SingleResultDeduplicatorTests {

    @Override
    protected <T> SingleResultDeduplicator<T> makeSingleResultDeduplicator(
        ThreadContext threadContext,
        Consumer<ActionListener<T>> executeAction
    ) {
        return new RelaxedSingleResultDeduplicator<T>(threadContext, executeAction);
    }

    public void testDeduplicate() {
        final int numThreads = between(2, 10);
        final var actionExecutionCount = new AtomicInteger(0);
        final var responses = new Object[numThreads];
        final var resultObjectRef = new AtomicReference<Object>();
        final var countDownLatch = new CountDownLatch(numThreads - 1);

        final Consumer<ActionListener<Object>> computation = l -> {
            final var count = actionExecutionCount.incrementAndGet();
            // The first thread will block until all the other callers have added a waiting listener.
            safeAwait(countDownLatch);
            final var resultObject = new Object();
            resultObjectRef.set(resultObject);
            l.onResponse(resultObject);
        };
        final var deduplicator = makeSingleResultDeduplicator(new ThreadContext(Settings.EMPTY), computation);

        ESTestCase.startInParallel(numThreads, threadNumber -> safeAwait(l -> {
            deduplicator.execute(ActionTestUtils.assertNoFailureListener(response -> {
                assertNull(responses[threadNumber]);
                responses[threadNumber] = response;
                l.onResponse(null);
            }));
            // If another thread has already gone down into the computation action, this thread will have a waiting listener added to
            // the list and the call to deduplicator.execute() will return immediately.
            countDownLatch.countDown();
        }));

        assertEquals("expected the action computation to run once", 1, actionExecutionCount.get());
        for (int i = 0; i < numThreads; i++) {
            assertSame("unexpected result response for thread " + i, resultObjectRef.get(), responses[i]);
        }
    }

    public void testDeduplicateWithActionFailure() {
        final int numThreads = between(2, 10);
        final var actionExecutionCount = new AtomicInteger(0);
        final var failures = new Exception[numThreads];
        final var exceptionRef = new AtomicReference<Exception>();
        final var countDownLatch = new CountDownLatch(numThreads - 1);

        final Consumer<ActionListener<Object>> computation = l -> {
            final var count = actionExecutionCount.incrementAndGet();
            // The first thread will block until all the other callers have added a waiting listener.
            safeAwait(countDownLatch);
            exceptionRef.set(new RuntimeException("failure"));
            l.onFailure(exceptionRef.get());
        };
        final var deduplicator = makeSingleResultDeduplicator(new ThreadContext(Settings.EMPTY), computation);

        ESTestCase.startInParallel(numThreads, threadNumber -> safeAwait(l -> {
            deduplicator.execute(ActionTestUtils.assertNoSuccessListener(e -> {
                assertNull(failures[threadNumber]);
                failures[threadNumber] = e;
                l.onResponse(null);
            }));
            // If another thread has already gone down into the computation action, this thread will have a waiting listener added to
            // the list and the call to deduplicator.execute() will return immediately.
            countDownLatch.countDown();
        }));

        // Verify that the both the listener for the thread that ran the action and all waiting listeners all get the same failure.
        assertEquals("expected the action computation to run once", 1, actionExecutionCount.get());
        for (int i = 0; i < numThreads; i++) {
            assertSame("unexpected failure exception for thread " + i, exceptionRef.get(), failures[i]);
        }
    }
}
