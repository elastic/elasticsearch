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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Traceable;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RequestHandlerRegistryTests extends ESTestCase {

    /** The key under which the tracer leaves the id of the span that is current in the thread context. */
    private static final String CURRENT_SPAN_ID = "test.current_span_id";

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
        final AtomicReference<String> seenByTheHandler = new AtomicReference<>();
        final AtomicReference<Task> taskOfTheHandler = new AtomicReference<>();

        registry((request, channel, task) -> {
            taskOfTheHandler.set(task);
            seenByTheHandler.set(threadPool.getThreadContext().getTransient(CURRENT_SPAN_ID));
            channel.sendResponse(new TestResponse("response"));
        }).processMessageReceived(new TestRequest("request"), channel());

        assertThat(taskOfTheHandler.get(), notNullValue());
        assertThat(seenByTheHandler.get(), equalTo(taskOfTheHandler.get().getSpanId()));
    }

    private Tracer tracerThatStoresTheSpanId() {
        final Tracer mockTracer = mock(Tracer.class);
        when(mockTracer.storeSpanInThreadContext(any())).thenAnswer(invocation -> {
            final ThreadContext threadContext = threadPool.getThreadContext();
            final ThreadContext.StoredContext restore = threadContext.newStoredContextPreservingResponseHeaders();
            threadContext.putTransientAllowOverwrite(CURRENT_SPAN_ID, ((Traceable) invocation.getArgument(0)).getSpanId());
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
