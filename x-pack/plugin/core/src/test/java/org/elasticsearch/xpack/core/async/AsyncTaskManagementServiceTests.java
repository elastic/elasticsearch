/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

// TODO: test CRUD operations
public class AsyncTaskManagementServiceTests extends ESSingleNodeTestCase {
    private ClusterService clusterService;
    private TransportService transportService;

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);


    public static class TestRequest extends ActionRequest {
        private final String string;

        public TestRequest(String string) {
            this.string = string;
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

    public static class TestTask extends CancellableTask implements AsyncTask {

        private final Map<String, String> originHeaders;
        private final AsyncExecutionId asyncExecutionId;

        public TestTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers,
                        Map<String, String> originHeaders, AsyncExecutionId asyncExecutionId) {
            super(id, type, action, description, parentTaskId, headers);
            this.originHeaders = originHeaders;
            this.asyncExecutionId = asyncExecutionId;
        }

        @Override
        public boolean shouldCancelChildrenOnCancellation() {
            return true;
        }

        @Override
        public Map<String, String> getOriginHeaders() {
            return originHeaders;
        }

        @Override
        public AsyncExecutionId getExecutionId() {
            return asyncExecutionId;
        }
    }

    public static class TestOperation implements AsyncTaskManagementService.AsyncOperation<TestRequest, TestResponse, TestTask> {

        @Override
        public TestTask createTask(TestRequest request, long id, String type, String action, TaskId parentTaskId,
                                   Map<String, String> headers, Map<String, String> originHeaders, AsyncExecutionId asyncExecutionId) {
            return new TestTask(id, type, action, request.getDescription(), parentTaskId, headers, originHeaders, asyncExecutionId);
        }

        @Override
        public void operation(TestRequest request, TestTask task, ActionListener<TestResponse> listener) {
            if (request.string.equals("die")) {
                listener.onFailure(new IllegalArgumentException("test exception"));
            } else {
                listener.onResponse(new TestResponse("response for [" + request.string + "]", task.asyncExecutionId.getEncoded()));
            }
        }

        @Override
        public TestResponse initialResponse(TestTask task) {
            return new TestResponse(null, task.asyncExecutionId.getEncoded());
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
    }

    /**
     * Shutdown the executor so we don't leak threads into other test runs.
     */
    @After
    public void shutdownExec() {
        executorService.shutdown();
    }

    private AsyncTaskManagementService<TestRequest, TestResponse, TestTask> createService(
        AsyncTaskManagementService.AsyncOperation<TestRequest, TestResponse, TestTask> operation) {
        return new AsyncTaskManagementService<>(index, client(), "test_origin", writableRegistry(),
            transportService.getTaskManager(), "test_action", operation, TestTask.class, clusterService, transportService.getThreadPool());
    }

    public void testReturnBeforeTimeout() throws Exception {
        AsyncTaskManagementService<TestRequest, TestResponse, TestTask> service = createService(new TestOperation());
        boolean success = randomBoolean();
        boolean keepOnCompletion = randomBoolean();
        CountDownLatch latch = new CountDownLatch(1);
        TestRequest request = new TestRequest(success ? randomAlphaOfLength(10) : "die");
        service.asyncExecute(request, TimeValue.timeValueMinutes(1), TimeValue.timeValueMinutes(10), keepOnCompletion,
            ActionListener.wrap(r -> {
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

    @TestLogging(value = "org.elasticsearch.xpack.core.async:trace", reason = "remove me")
    public void testReturnAfterTimeout() throws Exception {
        CountDownLatch executionLatch = new CountDownLatch(1);
        AsyncTaskManagementService<TestRequest, TestResponse, TestTask> service = createService(new TestOperation() {
            @Override
            public void operation(TestRequest request, TestTask task, ActionListener<TestResponse> listener) {
                executorService.submit(() -> {
                    try {
                        assertThat(executionLatch.await(10, TimeUnit.SECONDS), equalTo(true));
                    } catch (InterruptedException ex) {
                        fail("Shouldn't be here");
                    }
                    super.operation(request, task, listener);
                });
            }
        });
        boolean success = randomBoolean();
        boolean keepOnCompletion = randomBoolean();
        CountDownLatch latch = new CountDownLatch(1);
        TestRequest request = new TestRequest(success ? randomAlphaOfLength(10) : "die");
        AtomicReference<TestResponse> responseHolder = new AtomicReference<>();
        service.asyncExecute(request, TimeValue.timeValueMillis(0), TimeValue.timeValueMinutes(10), keepOnCompletion,
            ActionListener.wrap(r -> {
                assertThat(r.string, nullValue());
                assertThat(r.id, notNullValue());
                assertThat(responseHolder.getAndSet(r), nullValue());
                latch.countDown();
            }, e -> {
                fail("Shouldn't be here");
            }));
        assertThat(latch.await(10, TimeUnit.SECONDS), equalTo(true));
        executionLatch.countDown();
        assertThat(responseHolder.get(), notNullValue());
        AsyncExecutionId id = AsyncExecutionId.decode(responseHolder.get().id);
        assertThat(service.getTask(id), notNullValue());

        CountDownLatch responseLatch = new CountDownLatch(1);

        // Wait until task finishes
        assertBusy(() -> {
            TestTask t = service.getTask(id);
            logger.info(t);
            assertThat(t, nullValue());
        });

        ensureGreen(index);
        logger.info("Getting the the response back");
        service.getResponse(id, ActionListener.wrap(
            r -> {
                assertThat(r.string, equalTo("response for [" + request.string + "]"));
                responseLatch.countDown();
            },
            e -> {
                assertThat(e.getMessage(), equalTo("test exception"));
                responseLatch.countDown();
            }));
        assertThat(responseLatch.await(10, TimeUnit.SECONDS), equalTo(true));
    }
}
