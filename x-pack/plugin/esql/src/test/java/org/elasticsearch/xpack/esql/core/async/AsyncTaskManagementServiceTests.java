/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.async;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncResultsService;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.StoredAsyncResponse;
import org.elasticsearch.xpack.core.async.StoredAsyncTask;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.core.async.AsyncTaskManagementService.addCompletionListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AsyncTaskManagementServiceTests extends ESSingleNodeTestCase {
    private ClusterService clusterService;
    private TransportService transportService;
    private AsyncResultsService<TestTask, StoredAsyncResponse<TestResponse>> results;

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    public static class TestRequest extends LegacyActionRequest {
        private final String string;
        private final TimeValue keepAlive;

        public TestRequest(String string, TimeValue keepAlive) {
            this.string = string;
            this.keepAlive = keepAlive;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class TestResponse extends ActionResponse {
        private final String string;
        private final String id;

        public TestResponse(String string, String id) {
            this.string = string;
            this.id = id;
        }

        public TestResponse(StreamInput input) throws IOException {
            this.string = input.readOptionalString();
            this.id = input.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(string);
            out.writeOptionalString(id);
        }
    }

    public static class TestTask extends StoredAsyncTask<TestResponse> {
        public volatile AtomicReference<TestResponse> finalResponse = new AtomicReference<>();

        public TestTask(
            long id,
            String type,
            String action,
            String description,
            TaskId parentTaskId,
            Map<String, String> headers,
            Map<String, String> originHeaders,
            AsyncExecutionId asyncExecutionId,
            TimeValue keepAlive
        ) {
            super(id, type, action, description, parentTaskId, headers, originHeaders, asyncExecutionId, keepAlive);
        }

        @Override
        public TestResponse getCurrentResult() {
            return Objects.requireNonNullElseGet(finalResponse.get(), () -> new TestResponse(null, getExecutionId().getEncoded()));
        }
    }

    public static class TestOperation implements AsyncTaskManagementService.AsyncOperation<TestRequest, TestResponse, TestTask> {

        @Override
        public TestTask createTask(
            TestRequest request,
            long id,
            String type,
            String action,
            TaskId parentTaskId,
            Map<String, String> headers,
            Map<String, String> originHeaders,
            AsyncExecutionId asyncExecutionId
        ) {
            return new TestTask(
                id,
                type,
                action,
                request.getDescription(),
                parentTaskId,
                headers,
                originHeaders,
                asyncExecutionId,
                request.keepAlive
            );
        }

        @Override
        public void execute(TestRequest request, TestTask task, ActionListener<TestResponse> listener) {
            if (request.string.equals("die")) {
                listener.onFailure(new IllegalArgumentException("test exception"));
            } else {
                listener.onResponse(new TestResponse("response for [" + request.string + "]", task.getExecutionId().getEncoded()));
            }
        }

        @Override
        public TestResponse initialResponse(TestTask task) {
            return new TestResponse(null, task.getExecutionId().getEncoded());
        }

        @Override
        public TestResponse readResponse(StreamInput inputStream) throws IOException {
            return new TestResponse(inputStream);
        }
    }

    public String index = "test-index";

    @Before
    public void setup() {
        clusterService = getInstanceFromNode(ClusterService.class);
        transportService = getInstanceFromNode(TransportService.class);
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        AsyncTaskIndexService<StoredAsyncResponse<TestResponse>> store = new AsyncTaskIndexService<>(
            index,
            clusterService,
            transportService.getThreadPool().getThreadContext(),
            client(),
            "test",
            in -> new StoredAsyncResponse<>(TestResponse::new, in),
            writableRegistry(),
            bigArrays
        );
        results = new AsyncResultsService<>(
            store,
            false,
            TestTask.class,
            (task, listener, timeout, returnIntermediateResults) -> addCompletionListener(
                transportService.getThreadPool(),
                task,
                listener,
                timeout,
                returnIntermediateResults
            ),
            transportService.getTaskManager(),
            clusterService
        );
    }

    /**
     * Shutdown the executor so we don't leak threads into other test runs.
     */
    @After
    public void shutdownExec() {
        executorService.shutdown();
    }

    private AsyncTaskManagementService<TestRequest, TestResponse, TestTask> createManagementService(
        AsyncTaskManagementService.AsyncOperation<TestRequest, TestResponse, TestTask> operation
    ) {
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        return new AsyncTaskManagementService<>(
            index,
            client(),
            "test_origin",
            writableRegistry(),
            transportService.getTaskManager(),
            "test_action",
            operation,
            TestTask.class,
            clusterService,
            transportService.getThreadPool(),
            bigArrays
        );
    }

    public void testReturnBeforeTimeout() throws Exception {
        AsyncTaskManagementService<TestRequest, TestResponse, TestTask> service = createManagementService(new TestOperation());
        boolean success = randomBoolean();
        boolean keepOnCompletion = randomBoolean();
        CountDownLatch latch = new CountDownLatch(1);
        TestRequest request = new TestRequest(success ? randomAlphaOfLength(10) : "die", TimeValue.timeValueDays(1));
        service.asyncExecute(request, TimeValue.timeValueMinutes(1), keepOnCompletion, ActionListener.wrap(r -> {
            assertThat(success, equalTo(true));
            assertThat(r.string, equalTo("response for [" + request.string + "]"));
            assertThat(r.id, notNullValue());
            latch.countDown();
        }, e -> {
            assertThat(success, equalTo(false));
            assertThat(e.getMessage(), equalTo("test exception"));
            latch.countDown();
        }));
        assertThat(latch.await(10, TimeUnit.SECONDS), equalTo(true));
    }

    public void testReturnAfterTimeout() throws Exception {
        CountDownLatch executionLatch = new CountDownLatch(1);
        AsyncTaskManagementService<TestRequest, TestResponse, TestTask> service = createManagementService(new TestOperation() {
            @Override
            public void execute(TestRequest request, TestTask task, ActionListener<TestResponse> listener) {
                executorService.submit(() -> {
                    try {
                        assertThat(executionLatch.await(10, TimeUnit.SECONDS), equalTo(true));
                    } catch (InterruptedException ex) {
                        fail("Shouldn't be here");
                    }
                    super.execute(request, task, listener);
                });
            }
        });
        boolean success = randomBoolean();
        boolean keepOnCompletion = randomBoolean();
        boolean timeoutOnFirstAttempt = randomBoolean();
        boolean waitForCompletion = randomBoolean();
        CountDownLatch latch = new CountDownLatch(1);
        TestRequest request = new TestRequest(success ? randomAlphaOfLength(10) : "die", TimeValue.timeValueDays(1));
        AtomicReference<TestResponse> responseHolder = new AtomicReference<>();
        service.asyncExecute(request, TimeValue.timeValueMillis(1), keepOnCompletion, ActionTestUtils.assertNoFailureListener(r -> {
            assertThat(r.string, nullValue());
            assertThat(r.id, notNullValue());
            assertThat(responseHolder.getAndSet(r), nullValue());
            latch.countDown();
        }));
        assertThat(latch.await(20, TimeUnit.SECONDS), equalTo(true));

        if (timeoutOnFirstAttempt) {
            logger.trace("Getting an in-flight response");
            // try getting results, but fail with timeout because it is not ready yet
            StoredAsyncResponse<TestResponse> response = getResponse(responseHolder.get().id, TimeValue.timeValueMillis(2));
            assertThat(response.getException(), nullValue());
            assertThat(response.getResponse(), notNullValue());
            assertThat(response.getResponse().id, equalTo(responseHolder.get().id));
            assertThat(response.getResponse().string, nullValue());
        }

        if (waitForCompletion) {
            // now we are waiting for the task to finish
            logger.trace("Waiting for response to complete");
            var getFuture = getResponse(responseHolder.get().id, TimeValue.timeValueSeconds(5), TimeValue.MINUS_ONE);

            executionLatch.countDown();
            var response = safeGet(getFuture);

            if (success) {
                assertThat(response.getException(), nullValue());
                assertThat(response.getResponse(), notNullValue());
                assertThat(response.getResponse().id, equalTo(responseHolder.get().id));
                assertThat(response.getResponse().string, equalTo("response for [" + request.string + "]"));
            } else {
                assertThat(response.getException(), notNullValue());
                assertThat(response.getResponse(), nullValue());
                assertThat(response.getException().getMessage(), equalTo("test exception"));
            }
        } else {
            executionLatch.countDown();
        }

        // finally wait until the task disappears and get the response from the index
        logger.trace("Wait for task to disappear ");
        assertBusy(() -> {
            Task task = transportService.getTaskManager().getTask(AsyncExecutionId.decode(responseHolder.get().id).getTaskId().getId());
            assertThat(task, nullValue());
        });

        logger.trace("Getting the final response from the index");
        StoredAsyncResponse<TestResponse> response = getResponse(responseHolder.get().id, TimeValue.ZERO);
        if (success) {
            assertThat(response.getException(), nullValue());
            assertThat(response.getResponse(), notNullValue());
            assertThat(response.getResponse().string, equalTo("response for [" + request.string + "]"));
        } else {
            assertThat(response.getException(), notNullValue());
            assertThat(response.getResponse(), nullValue());
            assertThat(response.getException().getMessage(), equalTo("test exception"));
        }
    }

    public void testUpdateKeepAliveToTask() throws Exception {
        long now = System.currentTimeMillis();
        CountDownLatch executionLatch = new CountDownLatch(1);
        AsyncTaskManagementService<TestRequest, TestResponse, TestTask> service = createManagementService(new TestOperation() {
            @Override
            public void execute(TestRequest request, TestTask task, ActionListener<TestResponse> listener) {
                executorService.submit(() -> {
                    try {
                        assertThat(executionLatch.await(10, TimeUnit.SECONDS), equalTo(true));
                    } catch (InterruptedException ex) {
                        throw new AssertionError(ex);
                    }
                    super.execute(request, task, listener);
                });
            }
        });
        TestRequest request = new TestRequest(randomAlphaOfLength(10), TimeValue.timeValueHours(1));
        PlainActionFuture<TestResponse> submitResp = new PlainActionFuture<>();
        try {
            service.asyncExecute(request, TimeValue.timeValueMillis(1), true, submitResp);
            String id = submitResp.get().id;
            assertThat(id, notNullValue());
            TimeValue keepAlive = TimeValue.timeValueDays(between(1, 10));
            var resp1 = safeGet(getResponse(id, TimeValue.ZERO, keepAlive));
            assertThat(resp1.getExpirationTime(), greaterThanOrEqualTo(now + keepAlive.millis()));
        } finally {
            executionLatch.countDown();
        }
    }

    private StoredAsyncResponse<TestResponse> getResponse(String id, TimeValue timeout) throws InterruptedException {
        return safeGet(getResponse(id, timeout, TimeValue.MINUS_ONE));
    }

    private PlainActionFuture<StoredAsyncResponse<TestResponse>> getResponse(String id, TimeValue timeout, TimeValue keepAlive) {
        PlainActionFuture<StoredAsyncResponse<TestResponse>> future = new PlainActionFuture<>();
        GetAsyncResultRequest getResultsRequest = new GetAsyncResultRequest(id).setWaitForCompletionTimeout(timeout);
        getResultsRequest.setKeepAlive(keepAlive);
        results.retrieveResult(getResultsRequest, future);
        return future;
    }

}
