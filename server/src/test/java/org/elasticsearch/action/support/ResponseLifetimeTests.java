/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResponseLifetimeTests extends ESTestCase {
    public void testSimple() throws Exception {
        final var responseFuture = new PlainActionFuture<AbstractRefCounted>();
        final var responseClosed = new AtomicBoolean();
        final var response = AbstractRefCounted.of(() -> assertTrue(responseClosed.compareAndSet(false, true)));

        try (var lifetime = new ResponseLifetime()) {
            lifetime.retainResponse(responseFuture).onResponse(response);
            response.decRef();

            assertSame(response, responseFuture.get());
            assertFalse(responseClosed.get());
        }
        assertTrue(responseClosed.get());
    }

    public void testMultipleResponses() throws Exception {
        final var responseFuture = new PlainActionFuture<AbstractRefCounted>();
        final var responseClosed = new AtomicBoolean();
        final var response = AbstractRefCounted.of(() -> assertTrue(responseClosed.compareAndSet(false, true)));
        final var ignoredResponseClosed = new AtomicBoolean();
        final var ignoredResponse = AbstractRefCounted.of(() -> assertTrue(ignoredResponseClosed.compareAndSet(false, true)));
        final var exceptionFirst = randomBoolean();

        try (var lifetime = new ResponseLifetime()) {
            final var retainingListener = lifetime.retainResponse(responseFuture);
            if (exceptionFirst) {
                retainingListener.onFailure(new ElasticsearchException("simulated"));
            }
            retainingListener.onResponse(response);
            retainingListener.onResponse(response);
            retainingListener.onResponse(ignoredResponse);
            if (randomBoolean()) {
                retainingListener.onFailure(new IllegalStateException("ignored"));
            }
            response.decRef();
            ignoredResponse.decRef();

            if (exceptionFirst) {
                assertEquals(
                    "simulated",
                    expectThrows(ExecutionException.class, ElasticsearchException.class, responseFuture::get).getMessage()
                );
            } else {
                assertSame(response, responseFuture.get());
            }
            assertFalse(responseClosed.get());
            assertFalse(ignoredResponseClosed.get());
        }
        assertTrue(responseClosed.get());
        assertTrue(ignoredResponseClosed.get());
    }

    public void testRespondAfterClose() throws Exception {
        final var responseFuture = new PlainActionFuture<AbstractRefCounted>();
        final var responseClosed = new AtomicBoolean();
        final var response = AbstractRefCounted.of(() -> assertTrue(responseClosed.compareAndSet(false, true)));
        final ActionListener<AbstractRefCounted> retainingListener;

        try (var lifetime = new ResponseLifetime()) {
            retainingListener = lifetime.retainResponse(responseFuture);
        }

        retainingListener.onResponse(response);
        response.decRef();

        assertSame(response, responseFuture.get());
        assertTrue(responseClosed.get());
    }

    public void testConcurrentRespondAndClose() throws Exception {
        final var responseFuture = new PlainActionFuture<AbstractRefCounted>();
        final var responseClosed = new AtomicBoolean();
        final var response = AbstractRefCounted.of(() -> assertTrue(responseClosed.compareAndSet(false, true)));

        @SuppressWarnings("resource")
        final var lifetime = new ResponseLifetime();
        final var retainingListener = lifetime.retainResponse(responseFuture);

        final var completions = between(1, 5);
        final var threads = new Thread[completions + 1];
        final var barrier = new CyclicBarrier(threads.length);
        threads[0] = new Thread(() -> {
            safeAwait(barrier);
            lifetime.close();
        });
        for (int i = 1; i < threads.length; i++) {
            threads[i] = randomBoolean()

                ? new Thread(() -> {
                    safeAwait(barrier);
                    retainingListener.onResponse(response);
                })
                : new Thread(() -> {
                    safeAwait(barrier);
                    retainingListener.onFailure(new ElasticsearchException("simulated"));
                });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        response.decRef();
        assertTrue(responseClosed.get());

        try {
            assertSame(response, responseFuture.get());
        } catch (ExecutionException e) {
            assertEquals("simulated", e.getCause().getMessage());
        }
    }
}
