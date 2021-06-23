/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncSearchIndexServiceTests.TestAsyncResponse;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.async.AsyncExecutionIdTests.randomAsyncId;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AsyncResultsServiceTests extends ESSingleNodeTestCase {
    private ClusterService clusterService;
    private TaskManager taskManager;
    private AsyncTaskIndexService<TestAsyncResponse> indexService;

    public static class TestTask extends CancellableTask implements AsyncTask {
        private final AsyncExecutionId executionId;
        private final Map<ActionListener<TestAsyncResponse>, TimeValue> listeners = new HashMap<>();
        private long expirationTimeMillis;

        public TestTask(AsyncExecutionId executionId, long id, String type, String action, String description, TaskId parentTaskId,
                        Map<String, String> headers) {
            super(id, type, action, description, parentTaskId, headers);
            this.executionId = executionId;
        }

        @Override
        public boolean shouldCancelChildrenOnCancellation() {
            return false;
        }

        @Override
        public Map<String, String> getOriginHeaders() {
            return null;
        }

        @Override
        public AsyncExecutionId getExecutionId() {
            return executionId;
        }

        @Override
        public void setExpirationTime(long expirationTimeMillis) {
            this.expirationTimeMillis = expirationTimeMillis;
        }

        @Override
        public void cancelTask(TaskManager taskManager, Runnable runnable, String reason) {
            taskManager.cancelTaskAndDescendants(this, reason, true, ActionListener.wrap(runnable));
        }

        public long getExpirationTime() {
            return this.expirationTimeMillis;
        }

        public synchronized void addListener(ActionListener<TestAsyncResponse> listener, TimeValue timeout) {
            if (timeout.getMillis() < 0) {
                listener.onResponse(new TestAsyncResponse(null, expirationTimeMillis));
            } else {
                assertThat(listeners.put(listener, timeout), nullValue());
            }
        }

        private synchronized void onResponse(String response) {
            TestAsyncResponse r = new TestAsyncResponse(response, expirationTimeMillis);
            for (ActionListener<TestAsyncResponse> listener : listeners.keySet()) {
                listener.onResponse(r);
            }
        }

        private synchronized void onFailure(Exception e) {
            for (ActionListener<TestAsyncResponse> listener : listeners.keySet()) {
                listener.onFailure(e);
            }
        }
    }

    public class TestRequest extends TransportRequest {
        private final String string;

        public TestRequest(String string) {
            this.string = string;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            AsyncExecutionId asyncExecutionId = new AsyncExecutionId(randomAlphaOfLength(10),
                new TaskId(clusterService.localNode().getId(), id));
            return new TestTask(asyncExecutionId, id, type, action, string, parentTaskId, headers);
        }
    }

    @Before
    public void setup() {
        clusterService = getInstanceFromNode(ClusterService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        taskManager = transportService.getTaskManager();
        indexService = new AsyncTaskIndexService<>("test", clusterService, transportService.getThreadPool().getThreadContext(),
            client(), ASYNC_SEARCH_ORIGIN, TestAsyncResponse::new, writableRegistry(), bigArrays);

    }

    private AsyncResultsService<TestTask, TestAsyncResponse> createResultsService(boolean updateInitialResultsInStore) {
        return new AsyncResultsService<>(indexService, updateInitialResultsInStore, TestTask.class, TestTask::addListener,
            taskManager, clusterService);
    }

    private DeleteAsyncResultsService createDeleteResultsService() {
        return new DeleteAsyncResultsService(indexService, taskManager);
    }

    public void testRecordNotFound() {
        AsyncResultsService<TestTask, TestAsyncResponse> service = createResultsService(randomBoolean());
        DeleteAsyncResultsService deleteService = createDeleteResultsService();
        PlainActionFuture<TestAsyncResponse> listener = new PlainActionFuture<>();
        service.retrieveResult(new GetAsyncResultRequest(randomAsyncId().getEncoded()), listener);
        assertFutureThrows(listener, ResourceNotFoundException.class);
        PlainActionFuture<AcknowledgedResponse> deleteListener = new PlainActionFuture<>();
        deleteService.deleteResponse(new DeleteAsyncResultRequest(randomAsyncId().getEncoded()), deleteListener);
        assertFutureThrows(listener, ResourceNotFoundException.class);
    }

    public void testRetrieveFromMemoryWithExpiration() throws Exception {
        boolean updateInitialResultsInStore = randomBoolean();
        AsyncResultsService<TestTask, TestAsyncResponse> service = createResultsService(updateInitialResultsInStore);
        TestTask task = (TestTask) taskManager.register("test", "test", new TestRequest("test request"));
        try {
            boolean shouldExpire = randomBoolean();
            long expirationTime = System.currentTimeMillis() + randomLongBetween(100000, 1000000) * (shouldExpire ? -1 : 1);
            task.setExpirationTime(expirationTime);

            if (updateInitialResultsInStore) {
                // we need to store initial result
                PlainActionFuture<IndexResponse> future = new PlainActionFuture<>();
                indexService.createResponse(task.getExecutionId().getDocId(), task.getOriginHeaders(),
                    new TestAsyncResponse(null, task.getExpirationTime()), future);
                future.actionGet(TimeValue.timeValueSeconds(10));
            }

            PlainActionFuture<TestAsyncResponse> listener = new PlainActionFuture<>();
            service.retrieveResult(new GetAsyncResultRequest(task.getExecutionId().getEncoded())
                .setWaitForCompletionTimeout(TimeValue.timeValueSeconds(5)), listener);
            if (randomBoolean()) {
                // Test success
                String expectedResponse = randomAlphaOfLength(10);
                task.onResponse(expectedResponse);
                if (shouldExpire) {
                    assertFutureThrows(listener, ResourceNotFoundException.class);
                } else {
                    TestAsyncResponse response = listener.actionGet(TimeValue.timeValueSeconds(10));
                    assertThat(response, notNullValue());
                    assertThat(response.test, equalTo(expectedResponse));
                    assertThat(response.expirationTimeMillis, equalTo(expirationTime));
                }
            } else {
                // Test Failure
                task.onFailure(new IllegalArgumentException("test exception"));
                assertFutureThrows(listener, IllegalArgumentException.class);
            }
        } finally {
            taskManager.unregister(task);
        }
    }

    public void testAssertExpirationPropagation() throws Exception {
        boolean updateInitialResultsInStore = randomBoolean();
        AsyncResultsService<TestTask, TestAsyncResponse> service = createResultsService(updateInitialResultsInStore);
        TestRequest request = new TestRequest("test request");
        TestTask task = (TestTask) taskManager.register("test", "test", request);
        try {
            long startTime = System.currentTimeMillis();
            task.setExpirationTime(startTime + TimeValue.timeValueMinutes(1).getMillis());

            if (updateInitialResultsInStore) {
                // we need to store initial result
                PlainActionFuture<IndexResponse> future = new PlainActionFuture<>();
                indexService.createResponse(task.getExecutionId().getDocId(), task.getOriginHeaders(),
                    new TestAsyncResponse(null, task.getExpirationTime()), future);
                future.actionGet(TimeValue.timeValueSeconds(10));
            }

            TimeValue newKeepAlive = TimeValue.timeValueDays(1);
            PlainActionFuture<TestAsyncResponse> listener = new PlainActionFuture<>();
            // not waiting for completion, so should return immediately with timeout
            service.retrieveResult(new GetAsyncResultRequest(task.getExecutionId().getEncoded()).setKeepAlive(newKeepAlive), listener);
            listener.actionGet(TimeValue.timeValueSeconds(10));
            assertThat(task.getExpirationTime(), greaterThanOrEqualTo(startTime + newKeepAlive.getMillis()));
            assertThat(task.getExpirationTime(), lessThanOrEqualTo(System.currentTimeMillis() + newKeepAlive.getMillis()));

            if (updateInitialResultsInStore) {
                PlainActionFuture<TestAsyncResponse> future = new PlainActionFuture<>();
                indexService.getResponse(task.executionId, randomBoolean(), future);
                TestAsyncResponse response = future.actionGet(TimeValue.timeValueMinutes(10));
                assertThat(response.getExpirationTime(), greaterThanOrEqualTo(startTime + newKeepAlive.getMillis()));
                assertThat(response.getExpirationTime(), lessThanOrEqualTo(System.currentTimeMillis() + newKeepAlive.getMillis()));
            }
        } finally {
            taskManager.unregister(task);
        }
    }

    public void testRetrieveFromDisk() throws Exception {
        boolean updateInitialResultsInStore = randomBoolean();
        AsyncResultsService<TestTask, TestAsyncResponse> service = createResultsService(updateInitialResultsInStore);
        DeleteAsyncResultsService deleteService = createDeleteResultsService();
        TestRequest request = new TestRequest("test request");
        TestTask task = (TestTask) taskManager.register("test", "test", request);
        try {
            long startTime = System.currentTimeMillis();
            task.setExpirationTime(startTime + TimeValue.timeValueMinutes(1).getMillis());

            if (updateInitialResultsInStore) {
                // we need to store initial result
                PlainActionFuture<IndexResponse> futureCreate = new PlainActionFuture<>();
                indexService.createResponse(task.getExecutionId().getDocId(), task.getOriginHeaders(),
                    new TestAsyncResponse(null, task.getExpirationTime()), futureCreate);
                futureCreate.actionGet(TimeValue.timeValueSeconds(10));

                PlainActionFuture<UpdateResponse> futureUpdate = new PlainActionFuture<>();
                indexService.updateResponse(task.getExecutionId().getDocId(), emptyMap(),
                    new TestAsyncResponse("final_response", task.getExpirationTime()), futureUpdate);
                futureUpdate.actionGet(TimeValue.timeValueSeconds(10));
            } else {
                PlainActionFuture<IndexResponse> futureCreate = new PlainActionFuture<>();
                indexService.createResponse(task.getExecutionId().getDocId(), task.getOriginHeaders(),
                    new TestAsyncResponse("final_response", task.getExpirationTime()), futureCreate);
                futureCreate.actionGet(TimeValue.timeValueSeconds(10));
            }

        } finally {
            taskManager.unregister(task);
        }

        PlainActionFuture<TestAsyncResponse> listener = new PlainActionFuture<>();
        // not waiting for completion, so should return immediately with timeout
        service.retrieveResult(new GetAsyncResultRequest(task.getExecutionId().getEncoded()), listener);
        TestAsyncResponse response = listener.actionGet(TimeValue.timeValueSeconds(10));
        assertThat(response.test, equalTo("final_response"));

        PlainActionFuture<AcknowledgedResponse> deleteListener = new PlainActionFuture<>();
        deleteService.deleteResponse(new DeleteAsyncResultRequest(task.getExecutionId().getEncoded()), deleteListener);
        assertThat(deleteListener.actionGet().isAcknowledged(), equalTo(true));

        deleteListener = new PlainActionFuture<>();
        deleteService.deleteResponse(new DeleteAsyncResultRequest(task.getExecutionId().getEncoded()), deleteListener);
        assertFutureThrows(deleteListener, ResourceNotFoundException.class);
    }
}
