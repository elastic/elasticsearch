/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.migrate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.migrate.MigrateIndexTask.State;
import org.elasticsearch.action.admin.indices.migrate.TransportMigrateIndexAction.ActingOperation;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests that {@link TransportMigrateIndexAction} properly coalesces multiple requests to create the same index. Note that we rely on
 * extending {@link TransportMasterNodeAction} to be "good enough" to make sure that the requests only run on a single node. This just tests
 * closing the gap from "all of these requests run on a single node" to "all requests to migrate the same index block on the first such
 * request."
 */
public class TransportMigrateIndexActionCoalesceTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void after() {
        threadPool.shutdown();
    }

    public void testSingleRequest() throws Exception {
        MigrateIndexResponse response = new MigrateIndexResponse();
        ActionListener<MigrateIndexResponse> listener = listener();

        MockAction action = new MockAction() {
            @Override
            void shortCircuitMigration(ActingOperation operation) {
                operation.listener.onResponse(response);
            }
        };
        MigrateIndexTask task = action.masterOperation(request("test"), listener);
        assertSame("task's operation is the acting operation", task.getOperation(), task.getOperation().getActingOperation());
        assertEquals(State.SUCCESS, task.getState());
        assertSame(response, expectSuccess(listener));

        // Just for completeness sake, we'll test failure anyway. It shouldn't be different, but it is simple enough to check.
        Exception exception = new Exception();
        action = new MockAction() {
            @Override
            void shortCircuitMigration(ActingOperation operation) {
                operation.listener.onFailure(exception);
            }
        };
        task = action.masterOperation(request("test"), listener);
        assertSame("task's operation is the acting operation", task.getOperation(), task.getOperation().getActingOperation());
        assertSame(exception, expectFailure(listener));
        assertEquals(State.FAILED, task.getState());
    }

    /**
     * Tests that concurrent migration requests to different indexes never block one another.
     *
     * We do this by launching a bunch of concurrent requests, all to different indexes and blocking about half of them on startup. We start
     * them all at about the same time and assert that only the unblocked ones finish. Then we unblock the remainder and assert that they
     * all finished.
     */
    public void testManyRequestsToDifferentIndexesDoNotBlockEachOther() throws Exception {
        int requests = between(5, 30);
        MigrateIndexResponse response = new MigrateIndexResponse();
        Exception exception = new Exception();

        List<ActionListener<MigrateIndexResponse>> listeners = IntStream.range(0, requests).mapToObj(i -> listener()).collect(toList());
        CountDownLatch blockLatch = new CountDownLatch(1);
        boolean[] block = new boolean[requests];
        boolean[] succeed = new boolean[requests];
        for (int i = 0; i < requests; i++) {
            block[i] = randomBoolean();
            succeed[i] = randomBoolean();
        }
        MockAction action = new MockAction() {
            @Override
            void shortCircuitMigration(ActingOperation operation) {
                int requestNumber = Integer.parseInt(operation.request.getCreateIndexRequest().index());
                threadPool.generic().execute(() -> {
                    if (block[requestNumber]) {
                        try {
                            blockLatch.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (succeed[requestNumber]) {
                        operation.listener.onResponse(response);
                    } else {
                        operation.listener.onFailure(exception);
                    }
                });
            }
        };

        Map<String, MigrateIndexTask> tasks = Collections.synchronizedMap(new HashMap<>());
        for (int i = 0; i < requests; i++) {
            String indexName = Integer.toString(i);
            ActionListener<MigrateIndexResponse> listener = listeners.get(i);
            threadPool.generic().execute(() -> tasks.put(indexName, action.masterOperation(request(indexName), listener)));
        }

        // Wait until all the tasks are started
        assertBusy(() -> assertThat(tasks.values(), hasSize(requests)));

        for (int i = 0; i < requests; i++) {
            String destinationIndex = Integer.toString(i);
            MigrateIndexTask task = tasks.get(destinationIndex);
            assertNotNull(task);
            assertNotNull("Task should be available as soon as masterOperation returns", task.getOperation());
            assertSame("Task's operation should be the acting operation because there are no other migrations for this index",
                    task.getOperation(), task.getOperation().getActingOperation());
            if (block[i]) {
                assertEquals(State.STARTING, task.getState());
                /* Verify that we haven't yet got a response to this listener. We can't verify that we *won't* get one until we've unblocked
                 * the latch, but with this we can be sort of, mostly, sure that we didn't. Which is good enough for this test. */
                verify(listeners.get(i), never()).onResponse(any());
                verify(listeners.get(i), never()).onFailure(any());
            } else {
                if (succeed[i]) {
                    assertSame(response, expectSuccess(listeners.get(i)));
                    assertEquals(State.SUCCESS, task.getState());
                } else {
                    assertSame(exception, expectFailure(listeners.get(i)));
                    assertEquals(State.FAILED, task.getState());
                }
            }
        }
        blockLatch.countDown();
        for (int i = 0; i < requests; i++) {
            String destinationIndex = Integer.toString(i);
            MigrateIndexTask task = tasks.get(destinationIndex);
            if (succeed[i]) {
                assertSame(response, expectSuccess(listeners.get(i)));
                assertEquals(State.SUCCESS, task.getState());
            } else {
                assertSame(exception, expectFailure(listeners.get(i)));
                assertEquals(State.FAILED, task.getState());
            }
        }
    }

    public void testManyRequestsSameIndexCoalesce() throws Exception {
        int requests = between(5, 30);
        MigrateIndexResponse response = new MigrateIndexResponse();
        Exception exception = new Exception();
        boolean shouldSucceed = randomBoolean();

        List<ActionListener<MigrateIndexResponse>> listeners = IntStream.range(0, requests).mapToObj(i -> listener()).collect(toList());
        AtomicBoolean blockingMainTask = new AtomicBoolean(false);
        CountDownLatch blockLatch = new CountDownLatch(1);
        MockAction action = new MockAction() {
            @Override
            void shortCircuitMigration(ActingOperation operation) {
                threadPool.generic().execute(() -> {
                    try {
                        assertFalse("Tried to start the migration twice! Coalesce failure!", blockingMainTask.getAndSet(true));
                        blockLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    if (shouldSucceed) {
                        operation.listener.onResponse(response);
                    } else {
                        operation.listener.onFailure(exception);
                    }
                });
            }
        };

        List<MigrateIndexTask> tasks = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < requests; i++) {
            ActionListener<MigrateIndexResponse> listener = listeners.get(i);
            threadPool.generic().execute(() -> tasks.add(action.masterOperation(request("test"), listener)));
        }

        // Wait for tasks to start
        assertBusy(() -> assertThat(tasks, hasSize(requests)));

        // Find the single acting operation
        int observingTasksCount = 0;
        ActingOperation actingOperation = null;
        for (MigrateIndexTask task: tasks) {
            assertEquals(State.STARTING, task.getState());
            if (task.getOperation().getActingOperation() == task.getOperation()) {
                actingOperation = task.getOperation().getActingOperation();
            } else {
                observingTasksCount += 1;
            }
        }

        // Make sure the others are observing the acting operation
        for (MigrateIndexTask task: tasks) {
            assertSame(actingOperation, task.getOperation().getActingOperation());
        }
        assertEquals(requests - 1, observingTasksCount);

        for (int i = 0; i < requests; i++) {
            /* Verify that we haven't yet got a response to this listener. We can't verify that we *won't* get one until we've unblocked the
             * latch, but with this we can be sort of, mostly, sure that we didn't. Which is good enough for this test. */
            assertEquals(State.STARTING, tasks.get(i).getState());
            verify(listeners.get(i), never()).onResponse(any());
            verify(listeners.get(i), never()).onFailure(any());
        }
        blockLatch.countDown();
        for (int i = 0; i < requests; i++) {
            if (shouldSucceed) {
                assertSame(response, expectSuccess(listeners.get(i)));
                assertEquals(State.SUCCESS, tasks.get(i).getState());
            } else {
                assertSame(exception, expectFailure(listeners.get(i)));
                assertEquals(State.FAILED, tasks.get(i).getState());
            }
        }
    }

    public void testDifferentRequestsForTheSameIndexFail() throws Exception {
        MockAction action = new MockAction() {
            @Override
            void shortCircuitMigration(ActingOperation operation) {
                // Don't return anything so it looks like the request is still running
            }
        };
        ActionListener<MigrateIndexResponse> mainListener = listener();
        action.masterOperation(request("test"), mainListener);

        ActionListener<MigrateIndexResponse> followerListener = listener();
        MigrateIndexRequest differentRequest = request("test");
        differentRequest.setSourceIndex(differentRequest.getSourceIndex() + "_different");
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.masterOperation(differentRequest, followerListener));
        assertThat(e.getMessage(), containsString("Attempting two concurrent but different migration requests for the same index [test]"));
        /* The error message contains the different parts. It contains the whole request right now, but it also contains the different
         * parts.... */
        assertThat(e.getMessage(), containsString("source=test_0"));
        assertThat(e.getMessage(), containsString("source=test_0_different"));
    }

    private MigrateIndexRequest request(String destIndex) {
        MigrateIndexRequest request = new MigrateIndexRequest(destIndex + "_0", destIndex);
        return request;
    }

    @SuppressWarnings("unchecked")
    private ActionListener<MigrateIndexResponse> listener() {
        return mock(ActionListener.class);
    }

    private MigrateIndexResponse expectSuccess(ActionListener<MigrateIndexResponse> listener) throws Exception {
        ArgumentCaptor<MigrateIndexResponse> onResponseCaptor = ArgumentCaptor.forClass(MigrateIndexResponse.class);
        assertBusy(() -> verify(listener).onResponse(onResponseCaptor.capture()));
        return onResponseCaptor.getValue();
    }

    private Exception expectFailure(ActionListener<MigrateIndexResponse> listener) throws Exception {
        ArgumentCaptor<Exception> onFailureCaptor = ArgumentCaptor.forClass(Exception.class);
        assertBusy(() -> verify(listener).onFailure(onFailureCaptor.capture()));
        return onFailureCaptor.getValue();
    }

    private abstract class MockAction extends TransportMigrateIndexAction {
        public MockAction() {
            super(Settings.EMPTY, mock(TransportService.class), null, TransportMigrateIndexActionCoalesceTests.this.threadPool,
                    new ActionFilters(emptySet()), new IndexNameExpressionResolver(Settings.EMPTY), null, null);
        }

        /**
         * Testing wrapper around the real
         * {@link TransportMigrateIndexAction#masterOperation(Task, MigrateIndexRequest, ClusterState, ActionListener)}.
         */
        public MigrateIndexTask masterOperation(MigrateIndexRequest request, ActionListener<MigrateIndexResponse> listener) {
            MigrateIndexTask task = request.createTask(0, "test", MigrateIndexAction.NAME, null);
            ClusterState emptyState = ClusterState.builder(ClusterName.DEFAULT).build();
            masterOperation(task, request, emptyState, listener);
            return task;
        }

        @Override
        boolean preflightChecks(CreateIndexRequest createIndex, MetaData clustMetaData) {
            return true; // stub all preflight checks to force the migration to coalesce
        }

        @Override
        Client client(Task task) {
            // Not needed and lets us skip setting up a bunch of mocks
            return null;
        }

        @Override
        IndexMetaData sourceIndexMetaData(MetaData clusterMetaData, String name) {
            // Not needed and lets us skip building a bunch of mocks
            return null;
        }

        @Override
        final void start(ActingOperation operation) {
            shortCircuitMigration(operation);
        }

        /**
         * Overridden by tests to simulate certain responses.
         */
        abstract void shortCircuitMigration(ActingOperation operation);
    }
}
