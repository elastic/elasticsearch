/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ActionRunnableTests extends ESTestCase {
    public void testWrapReleasingNotRejected() throws Exception {
        final var executor = EsExecutors.newScaling(
            "test",
            1,
            1,
            60,
            TimeUnit.SECONDS,
            true,
            Thread::new,
            new ThreadContext(Settings.EMPTY)
        );
        try {
            final var resultListener = new PlainActionFuture<Void>();
            final var releaseListener = new PlainActionFuture<Void>();
            final var safeReleaseListener = ActionListener.assertOnce(releaseListener);
            final var barrier = new CyclicBarrier(2);

            executor.execute(() -> safeAwait(barrier)); // block the thread before running the ActionRunnable

            executor.execute(ActionRunnable.wrapReleasing(resultListener.delegateResponse((l, e) -> {
                assertThat(e, Matchers.instanceOf(ElasticsearchException.class));
                assertEquals("simulated", e.getMessage());
                assertTrue(releaseListener.isDone());
                l.onResponse(null);
            }), () -> safeReleaseListener.onResponse(null), l -> executor.execute(ActionRunnable.run(l, () -> {
                if (randomBoolean()) {
                    throw new ElasticsearchException("simulated");
                }
            }))));

            executor.execute(() -> safeAwait(barrier)); // block the thread after running the ActionRunnable but before completing listener

            assertFalse(releaseListener.isDone());
            assertFalse(resultListener.isDone());

            safeAwait(barrier); // execute the ActionRunnable

            assertNull(releaseListener.get(10, TimeUnit.SECONDS));
            assertFalse(resultListener.isDone());

            safeAwait(barrier); // complete the listener

            assertNull(resultListener.get(10, TimeUnit.SECONDS));
        } finally {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }
    }

    public void testWrapReleasingRejected() throws Exception {
        final var executor = EsExecutors.newFixed(
            "test",
            1,
            0,
            Thread::new,
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        try {
            final var listener = new PlainActionFuture<Void>();
            final var isReleased = new AtomicBoolean();

            final var barrier = new CyclicBarrier(2);
            executor.execute(() -> safeAwait(barrier));

            executor.execute(ActionRunnable.wrapReleasing(new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    fail("should not execute");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, Matchers.instanceOf(EsRejectedExecutionException.class));
                    assertFalse(isReleased.get());
                    listener.onResponse(null);
                }
            }, () -> assertTrue(isReleased.compareAndSet(false, true)), l -> fail("should not execute")));

            assertTrue(listener.isDone());
            assertTrue(isReleased.get());
            assertNull(listener.get(10, TimeUnit.SECONDS));
            safeAwait(barrier);
        } finally {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }
    }

    public void testWrapReleasingOnFailure() {
        final var isComplete = new AtomicBoolean();
        final var isReleased = new AtomicBoolean();

        ActionRunnable.wrapReleasing(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                fail("should not execute");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, Matchers.instanceOf(ElasticsearchException.class));
                assertEquals("simulated", e.getMessage());
                assertFalse(isReleased.get());
                assertTrue(isComplete.compareAndSet(false, true));
            }
        }, () -> assertTrue(isReleased.compareAndSet(false, true)), l -> fail("should not execute"))
            .onFailure(new ElasticsearchException("simulated"));

        assertTrue(isComplete.get());
        assertTrue(isReleased.get());
    }

    public void testWrapReleasingConsumerThrows() {
        final var isComplete = new AtomicBoolean();
        final var isReleased = new AtomicBoolean();

        ActionRunnable.wrapReleasing(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                fail("should not execute");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, Matchers.instanceOf(ElasticsearchException.class));
                assertEquals("simulated", e.getMessage());
                assertFalse(isReleased.get());
                assertTrue(isComplete.compareAndSet(false, true));
            }
        }, () -> assertTrue(isReleased.compareAndSet(false, true)), l -> { throw new ElasticsearchException("simulated"); }).run();

        assertTrue(isComplete.get());
        assertTrue(isReleased.get());
    }

}
