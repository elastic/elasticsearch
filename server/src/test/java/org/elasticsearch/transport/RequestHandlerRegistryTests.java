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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Span;
import org.elasticsearch.telemetry.tracing.SpanBuilder;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Answers;

import java.util.Set;

import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class RequestHandlerRegistryTests extends ESTestCase {

    private ThreadPool threadPool;
    private TaskManager taskManager;
    private Tracer tracer;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        tracer = tracerThatStoresTheSpanId();
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), tracer, "node");
    }

    @After
    public void teardown() {
        terminate(threadPool);
    }

    public void testHandlerIsRunWithTheSpanOfTheTaskInTheThreadContext() throws Exception {
        registry((request, channel, task) -> {
            assertTrue(threadPool.getThreadContext().getTransient("span_is_present"));
            channel.sendResponse(new TestResponse("response"));
        }).processMessageReceived(new TestRequest("request"), channel());

        assertThat(threadPool.getThreadContext().getTransient("span_is_present"), nullValue());
    }

    private Tracer tracerThatStoresTheSpanId() {
        final Tracer mockTracer = mock(Tracer.class);
        final SpanBuilder mockSpanBuilder = mock(SpanBuilder.class, withSettings().defaultAnswer(Answers.RETURNS_SELF));
        final Span mockSpan = mock(Span.class, withSettings().defaultAnswer(Answers.RETURNS_SELF));
        when(mockTracer.spanBuilder(any())).thenReturn(mockSpanBuilder);
        when(mockSpanBuilder.startSpan()).thenReturn(mockSpan);
        when(mockSpan.storeInThreadContext()).thenAnswer(invocation -> {
            final ThreadContext threadContext = threadPool.getThreadContext();
            final ThreadContext.StoredContext restore = threadContext.newStoredContextPreservingResponseHeaders();
            threadContext.putTransientAllowOverwrite("span_is_present", true);
            return restore;
        });
        return mockTracer;
    }

    private RequestHandlerRegistry<TestRequest> registry(TransportRequestHandler<TestRequest> handler) {
        return new RequestHandlerRegistry<>(
            "test-action",
            TestRequest::new,
            taskManager,
            handler,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            false,
            true
        );
    }

    private static TransportChannel channel() {
        return new TestTransportChannel(ActionListener.noop());
    }
}
