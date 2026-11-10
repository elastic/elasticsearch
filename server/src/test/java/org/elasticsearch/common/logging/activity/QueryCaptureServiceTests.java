/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchCaptureContext;
import org.elasticsearch.action.search.SearchCaptureContextBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class QueryCaptureServiceTests extends ESTestCase {
    public void testQueuedResponseAndRejectedResponseOwnership() throws Exception {
        try (var pool = new TestThreadPool(getTestName(), Settings.builder().put("thread_pool.query_capture.queue_size", 1).build())) {
            var service = new QueryCaptureService(pool);
            var started = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var processed = new AtomicBoolean();
            SearchResponse queued = SearchResponseUtils.successfulResponse(SearchHits.EMPTY_WITH_TOTAL_HITS);
            SearchResponse rejected = SearchResponseUtils.successfulResponse(SearchHits.EMPTY_WITH_TOTAL_HITS);
            try {
                service.execute(() -> {
                    started.countDown();
                    safeAwait(release);
                }, null);
                assertTrue(started.await(10, TimeUnit.SECONDS));
                service.execute(() -> {
                    assertTrue(queued.hasReferences());
                    assertNotNull(queued.getHits());
                    processed.set(true);
                }, queued);
                assertFalse(queued.decRef());
                service.execute(() -> fail("rejected work must never run"), rejected);
                assertTrue(rejected.decRef());
                for (var stats : pool.stats()) {
                    if (stats.name().equals(ThreadPool.Names.QUERY_CAPTURE)) {
                        assertEquals(1L, stats.rejected());
                    }
                }
            } finally {
                release.countDown();
            }
            assertBusy(() -> {
                assertTrue(processed.get());
                assertFalse(queued.hasReferences());
            });
        }
    }

    public void testListenerCompletesBeforeWorkerAndResponseRemainsReadable() throws Exception {
        try (var pool = new TestThreadPool(getTestName())) {
            var service = new QueryCaptureService(pool);
            var started = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var captured = new AtomicBoolean();
            var delivered = new AtomicBoolean();
            var caller = Thread.currentThread();
            var elapsed = new AtomicLong(TimeUnit.SECONDS.toNanos(1));
            SearchResponse response = SearchResponseUtils.successfulResponse(SearchHits.EMPTY_WITH_TOTAL_HITS);
            var sink = new TestQuerySink<SearchCaptureContext>(() -> true, context -> {
                assertNotSame(caller, Thread.currentThread());
                assertTrue(response.hasReferences());
                assertEquals(0, context.getResultCount());
                assertEquals(TimeUnit.SECONDS.toNanos(1), context.getTookInNanos());
                assertEquals("request", pool.getThreadContext().getHeader("capture-test"));
                captured.set(true);
            }, () -> "test");
            var interceptor = new QueryInterceptor<>(List.of(sink));
            var builder = new SearchCaptureContextBuilder(
                new Task(1, "test", "search", "", TaskId.EMPTY_TASK_ID, Map.of()),
                new NamedWriteableRegistry(List.of()),
                new SearchRequest()
            ) {
                @Override
                protected long elapsed() {
                    return elapsed.get();
                }
            };
            try {
                service.execute(() -> {
                    started.countDown();
                    safeAwait(release);
                }, null);
                assertTrue(started.await(10, TimeUnit.SECONDS));
                try (var ignored = pool.getThreadContext().stashContext()) {
                    pool.getThreadContext().putHeader("capture-test", "request");
                    interceptor.wrapAndRun(ActionListener.wrap(r -> {
                        assertSame(response, r);
                        delivered.set(true);
                    }, e -> fail(e.toString())), builder, listener -> listener.onResponse(response), service);
                }
                assertTrue(delivered.get());
                assertFalse(captured.get());
                assertFalse(response.decRef());
                elapsed.set(TimeUnit.SECONDS.toNanos(10));
            } finally {
                release.countDown();
            }
            assertBusy(() -> {
                assertTrue(captured.get());
                assertFalse(response.hasReferences());
            });
        }
    }

    public void testShutdownRejectionReleasesResponse() {
        try (var pool = new TestThreadPool(getTestName())) {
            var service = new QueryCaptureService(pool);
            pool.shutdown();
            var response = SearchResponseUtils.successfulResponse(SearchHits.EMPTY_WITH_TOTAL_HITS);
            service.execute(() -> fail("shutdown executor must reject"), response);
            assertTrue(response.decRef());
        }
    }

    public void testProcessingFailureReleasesResponse() throws Exception {
        try (var pool = new TestThreadPool(getTestName()); var log = MockLog.capture(QueryCaptureService.class)) {
            log.addExpectation(
                new MockLog.SeenEventExpectation(
                    "capture failure",
                    QueryCaptureService.class.getCanonicalName(),
                    Level.WARN,
                    "query capture processing failed"
                )
            );
            var response = SearchResponseUtils.successfulResponse(SearchHits.EMPTY_WITH_TOTAL_HITS);
            new QueryCaptureService(pool).execute(() -> { throw new IllegalStateException("test failure"); }, response);
            response.decRef();
            assertBusy(() -> assertFalse(response.hasReferences()));
            log.assertAllExpectationsMatched();
        }
    }

    public void testFailedSearchIsCaptured() throws Exception {
        try (var pool = new TestThreadPool(getTestName())) {
            var captured = new AtomicBoolean();
            var failure = new IllegalStateException("search failure");
            var sink = new TestQuerySink<SearchCaptureContext>(() -> true, context -> {
                assertFalse(context.isSuccess());
                assertEquals(failure.getMessage(), context.getErrorMessage());
                captured.set(true);
            }, () -> "test");
            var builder = new SearchCaptureContextBuilder(
                new Task(1, "test", "search", "", TaskId.EMPTY_TASK_ID, Map.of()),
                new NamedWriteableRegistry(List.of()),
                new SearchRequest()
            );
            new QueryInterceptor<>(List.of(sink)).wrapAndRun(
                ActionListener.wrap(r -> fail("expected failure"), e -> assertSame(failure, e)),
                builder,
                listener -> listener.onFailure(failure),
                new QueryCaptureService(pool)
            );
            assertBusy(() -> assertTrue(captured.get()));
        }
    }

    public void testDisabledCaptureDoesNotWrapListener() {
        try (var pool = new TestThreadPool(getTestName())) {
            var sink = new TestQuerySink<SearchCaptureContext>(() -> false, context -> fail("disabled"), () -> "test");
            var builder = new SearchCaptureContextBuilder(
                new Task(1, "test", "search", "", TaskId.EMPTY_TASK_ID, Map.of()),
                new NamedWriteableRegistry(List.of()),
                new SearchRequest()
            );
            ActionListener<SearchResponse> original = ActionListener.wrap(r -> {}, e -> fail(e.toString()));
            new QueryInterceptor<>(List.of(sink)).wrapAndRun(
                original,
                builder,
                listener -> assertSame(original, listener),
                new QueryCaptureService(pool)
            );
        }
    }
}
