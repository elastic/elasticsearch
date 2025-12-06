/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class TransportActionTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = new ThreadPool(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "TransportActionTests").build(),
            MeterRegistry.NOOP,
            new DefaultBuiltInExecutorBuilders()
        );
    }

    @After
    public void shutdown() throws Exception {
        terminate(threadPool);
    }

    public void testDirectExecuteRunsOnCallingThread() throws ExecutionException, InterruptedException {

        String actionName = randomAlphaOfLength(randomInt(30));
        var testExecutor = new Executor() {
            @Override
            public void execute(Runnable command) {
                fail("executeDirect should not run a TransportAction on a different executor");
            }
        };

        var transportAction = getTestTransportAction(actionName, testExecutor);

        PlainActionFuture<TestResponse> future = new PlainActionFuture<>();

        transportAction.executeDirect(null, new TestRequest(), future);

        var response = future.get();
        assertThat(response, notNullValue());
        assertThat(response.executingThreadName, equalTo(Thread.currentThread().getName()));
        assertThat(response.executingThreadId, equalTo(Thread.currentThread().getId()));
    }

    public void testExecuteRunsOnExecutor() throws ExecutionException, InterruptedException {

        String actionName = randomAlphaOfLength(randomInt(30));

        boolean[] executedOnExecutor = new boolean[1];
        var testExecutor = new Executor() {
            @Override
            public void execute(Runnable command) {
                command.run();
                executedOnExecutor[0] = true;
            }
        };

        var transportAction = getTestTransportAction(actionName, testExecutor);

        PlainActionFuture<TestResponse> future = new PlainActionFuture<>();

        ActionTestUtils.execute(transportAction, null, new TestRequest(), future);

        var response = future.get();
        assertThat(response, notNullValue());
        assertTrue(executedOnExecutor[0]);
    }

    public void testExecuteWithGenericExecutorRunsOnDifferentThread() throws ExecutionException, InterruptedException {

        String actionName = randomAlphaOfLength(randomInt(30));
        var transportAction = getTestTransportAction(actionName, threadPool.executor(ThreadPool.Names.GENERIC));

        PlainActionFuture<TestResponse> future = new PlainActionFuture<>();

        ActionTestUtils.execute(transportAction, null, new TestRequest(), future);

        var response = future.get();
        assertThat(response, notNullValue());
        assertThat(response.executingThreadName, not(equalTo(Thread.currentThread().getName())));
        assertThat(response.executingThreadName, containsString("[generic]"));
        assertThat(response.executingThreadId, not(equalTo(Thread.currentThread().getId())));
    }

    public void testExecuteWithDirectExecutorRunsOnCallingThread() throws ExecutionException, InterruptedException {

        String actionName = randomAlphaOfLength(randomInt(30));
        var transportAction = getTestTransportAction(actionName, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        PlainActionFuture<TestResponse> future = new PlainActionFuture<>();

        ActionTestUtils.execute(transportAction, null, new TestRequest(), future);

        var response = future.get();
        assertThat(response, notNullValue());
        assertThat(response, notNullValue());
        assertThat(response.executingThreadName, equalTo(Thread.currentThread().getName()));
        assertThat(response.executingThreadId, equalTo(Thread.currentThread().getId()));
    }

    private TransportAction<TestRequest, TestResponse> getTestTransportAction(String actionName, Executor executor) {
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        TransportAction<TestRequest, TestResponse> transportAction = new TransportAction<>(
            actionName,
            actionFilters,
            new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet()),
            executor
        ) {
            @Override
            protected void doExecute(Task task, TestRequest request, ActionListener<TestResponse> listener) {
                listener.onResponse(new TestResponse(Thread.currentThread().getName(), Thread.currentThread().getId()));
            }
        };
        return transportAction;
    }

    private static class TestRequest extends LegacyActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    private static class TestResponse extends ActionResponse {

        private final String executingThreadName;
        private final long executingThreadId;

        TestResponse(String executingThreadName, long executingThreadId) {
            this.executingThreadName = executingThreadName;
            this.executingThreadId = executingThreadId;
        }

        @Override
        public void writeTo(StreamOutput out) {}
    }
}
