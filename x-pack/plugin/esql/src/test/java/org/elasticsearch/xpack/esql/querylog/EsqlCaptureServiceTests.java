/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.logging.activity.QueryCaptureService;
import org.elasticsearch.common.logging.activity.QuerySink;
import org.elasticsearch.common.logging.activity.QuerySinkRegistry;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.session.Result;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/** Verifies that ES|QL capture data remains usable after control returns to the response consumer. */
public class EsqlCaptureServiceTests extends ESTestCase {
    public void testResponseRetainedUntilWorkerCompletes() throws Exception {
        assumeTrue("requires asynchronous capture", QueryCaptureService.ASYNC_FEATURE_FLAG.isEnabled());
        try (var pool = new TestThreadPool(getTestName())) {
            var service = new QueryCaptureService(pool, MeterRegistry.NOOP, new QuerySinkRegistry(List.of()));
            var started = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var captured = new AtomicBoolean();
            var info = executionInfo();
            var page = new Page(TestBlockFactory.getNonBreakingInstance().newConstantIntBlockWith(42, 3));
            var response = new EsqlQueryResponse(
                List.of(),
                List.of(page),
                0,
                0,
                null,
                false,
                null,
                false,
                false,
                ZoneOffset.UTC,
                0,
                0,
                info
            );
            var interceptor = service.interceptor(new QuerySink<EsqlCaptureContext>(() -> "esql") {
                @Override
                public boolean isEnabled() {
                    return true;
                }

                @Override
                public void accept(EsqlCaptureContext context) {
                    assertTrue(Thread.currentThread().getName().contains("[query_capture]"));
                    assertTrue(response.hasReferences());
                    assertEquals(3, context.getResultCount());
                    assertEquals("ROW x = 42", context.getQuery());
                    captured.set(true);
                }
            });
            try {
                pool.executor(ThreadPool.Names.QUERY_CAPTURE).execute(() -> {
                    started.countDown();
                    safeAwait(release);
                });
                safeAwait(started);
                interceptor.wrapAndRun(
                    ActionListener.wrap(r -> assertSame(response, r), e -> fail(e.toString())),
                    new EsqlCaptureContextBuilder(task(), syncEsqlQueryRequest("ROW x = 42")),
                    listener -> listener.onResponse(response)
                );
                assertFalse(captured.get());
            } finally {
                response.close();
                release.countDown();
            }
            assertBusy(() -> {
                assertTrue(captured.get());
                assertFalse(response.hasReferences());
            });
        }
    }

    public void testStreamingCompletionMetadataOnWorker() throws Exception {
        assumeTrue("requires asynchronous capture", QueryCaptureService.ASYNC_FEATURE_FLAG.isEnabled());
        try (var pool = new TestThreadPool(getTestName())) {
            var service = new QueryCaptureService(pool, MeterRegistry.NOOP, new QuerySinkRegistry(List.of()));
            var captured = new AtomicBoolean();
            var info = executionInfo();
            var result = new Result(List.of(), List.of(), Map.of(), EsqlTestUtils.TEST_CFG, DriverCompletionInfo.EMPTY, info, null);
            var publisher = new PageStreamPublisher(10);
            var interceptor = service.interceptor(new QuerySink<EsqlCaptureContext>(() -> "esql") {
                @Override
                public boolean isEnabled() {
                    return true;
                }

                @Override
                public void accept(EsqlCaptureContext context) {
                    assertTrue(Thread.currentThread().getName().contains("[query_capture]"));
                    assertTrue(context instanceof EsqlStreamCaptureContext);
                    assertEquals(0, context.getResultCount());
                    assertTrue(context.getRollupCounters().isPresent());
                    assertEquals(info.overallTook().nanos(), context.getTookInNanos());
                    captured.set(true);
                }
            });
            interceptor.wrapAndRun(
                ActionListener.wrap(r -> assertSame(ActionResponse.Empty.INSTANCE, r), e -> fail(e.toString())),
                new EsqlStreamCaptureContextBuilder(task(), syncEsqlQueryRequest("ROW x = 42"), info, publisher, () -> result),
                listener -> listener.onResponse(ActionResponse.Empty.INSTANCE)
            );
            assertBusy(() -> assertTrue(captured.get()));
        }
    }

    private static Task task() {
        return new Task(1, "transport", "esql", "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static EsqlExecutionInfo executionInfo() {
        var info = new EsqlExecutionInfo(alias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        info.markEndQuery();
        return info;
    }
}
