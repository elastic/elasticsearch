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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.migrate.MigrateIndexAction;
import org.elasticsearch.action.admin.indices.migrate.MigrateIndexRequest;
import org.elasticsearch.action.admin.indices.migrate.MigrateIndexResponse;
import org.elasticsearch.action.admin.indices.migrate.MigrateIndexTask;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.empty;
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
            void startMigration(MigrateIndexTask task) {
                task.getListener().onResponse(response);
            }
        };
        action.masterOperation(request("test"), listener);
        assertSame(response, expectSuccess(listener));
        // The task shouldn't be available because it is no longer running
        assertNull(action.getRunningTask("test"));

        // Just for completeness sake, we'll test failure anyway. It shouldn't be different, but it is simple enough to check.
        Exception exception = new Exception();
        action = new MockAction() {
            @Override
            void startMigration(MigrateIndexTask task) {
                task.getListener().onFailure(exception);
            }
        };
        action.masterOperation(request("test"), listener);
        assertSame(exception, expectFailure(listener));
        // The task shouldn't be available because it is no longer running
        assertNull(action.getRunningTask("test"));
    }

    /**
     * Tests that concurrent migration requests to different indexes never block one another.
     *
     * We do this by launching a bunch of concurrent requests, all to different indexes and blocking about half of them until on startup. We
     * start them all at about the same time and assert that only the unblocked ones finish. Then we unblock the remainder and assert that
     * they all finished.
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
            void startMigration(MigrateIndexTask task) {
                int requestNumber = Integer.parseInt(task.getRequest().getCreateIndexRequest().index());
                if (block[requestNumber]) {
                    try {
                        blockLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (succeed[requestNumber]) {
                    task.getListener().onResponse(response);
                } else {
                    task.getListener().onFailure(exception);
                }
            }
        };

        for (int i = 0; i < requests; i++) {
            String indexName = Integer.toString(i);
            ActionListener<MigrateIndexResponse> listener = listeners.get(i);
            threadPool.generic().execute(() -> action.masterOperation(request(indexName), listener));
        }

        for (int i = 0; i < requests; i++) {
            if (block[i]) {
                String destinationIndex = Integer.toString(i);
                assertBusy(() -> {
                    // We're sure that that task is still running so we can assert that it is sane
                    MigrateIndexTask task = action.getRunningTask(destinationIndex);
                    assertNotNull(task);
                    assertThat(task.getDuplicates(), empty());
                });
                /*
                 * Verify that we haven't yet got a response to this listener. We can't verify that we *won't* get one until we've unblocked
                 * the latch, but with this we can be sort of, mostly, sure that we didn't. Which is good enough for this test.
                 */
                verify(listeners.get(i), never()).onResponse(any());
                verify(listeners.get(i), never()).onFailure(any());
            } else {
                if (succeed[i]) {
                    assertSame(response, expectSuccess(listeners.get(i)));
                } else {
                    assertSame(exception, expectFailure(listeners.get(i)));
                }
                // The task shouldn't be available because it is no longer running
                assertNull(action.getRunningTask("test"));
            }
        }
        blockLatch.countDown();
        for (int i = 0; i < requests; i++) {
            if (succeed[i]) {
                assertSame(response, expectSuccess(listeners.get(i)));
            } else {
                assertSame(exception, expectFailure(listeners.get(i)));
            }
            // The task shouldn't be available because it is no longer running
            assertNull(action.getRunningTask("test"));
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
            void startMigration(MigrateIndexTask task) {
                try {
                    assertFalse("Tried to start the migration twice! Coalesce failure!", blockingMainTask.getAndSet(true));
                    blockLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (shouldSucceed) {
                    task.getListener().onResponse(response);
                } else {
                    task.getListener().onFailure(exception);
                }
            }
        };

        for (int i = 0; i < requests; i++) {
            ActionListener<MigrateIndexResponse> listener = listeners.get(i);
            threadPool.generic().execute(() -> action.masterOperation(request("test"), listener));
        }

        /*
         * Wait until one task clearly "wins" and starts to collect duplicates, failing the test if we get multiple tasks collecting
         * duplicates.
         */
        MigrateIndexTask mainTask = null;
        long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        do {
            MigrateIndexTask task = action.getRunningTask("test");
            if (task != null && false == task.getDuplicates().isEmpty()) {
                if (mainTask != null) {
                    fail("Found two 'main' tasks");
                }
                mainTask = task;
            }
            if (System.nanoTime() - timeout > 0) {
                fail("Didn't find a 'main' task after 10 seconds. This is the task we found [" + task + "]");
            }
        } while (mainTask == null);
        // Wait for all of the duplicates to register.
        MigrateIndexTask finalMainTask = mainTask;
        assertBusy(() -> assertEquals(requests - 1, finalMainTask.getDuplicates().size()));
        for (MigrateIndexTask dupe : mainTask.getDuplicates()) {
            assertSame(mainTask, dupe.getWaitingFor());
        }
        for (int i = 0; i < requests; i++) {
            /*
             * Verify that we haven't yet got a response to this listener. We can't verify that we *won't* get one until we've unblocked the
             * latch, but with this we can be sort of, mostly, sure that we didn't. Which is good enough for this test.
             */
            verify(listeners.get(i), never()).onResponse(any());
            verify(listeners.get(i), never()).onFailure(any());
        }
        blockLatch.countDown();
        for (int i = 0; i < requests; i++) {
            if (shouldSucceed) {
                assertSame(response, expectSuccess(listeners.get(i)));
            } else {
                assertSame(exception, expectFailure(listeners.get(i)));
            }
            // The task shouldn't be available because it is no longer running
            assertNull(action.getRunningTask("test"));
        }
    }

    private MigrateIndexRequest request(String destIndex) {
        MigrateIndexRequest request = new MigrateIndexRequest();
        request.setCreateIndexRequest(new CreateIndexRequest(destIndex));
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

    private class MockAction extends TransportMigrateIndexAction {
        public MockAction() {
            super(Settings.EMPTY, mock(TransportService.class), null, TransportMigrateIndexActionCoalesceTests.this.threadPool,
                    new ActionFilters(emptySet()), new IndexNameExpressionResolver(Settings.EMPTY), null);
        }

        /**
         * Testing wrapper around the real
         * {@link TransportMigrateIndexAction#masterOperation(Task, MigrateIndexRequest, ClusterState, ActionListener)}.
         */
        public void masterOperation(MigrateIndexRequest request, ActionListener<MigrateIndexResponse> listener) {
            masterOperation(request.createTask(0, "test", MigrateIndexAction.NAME, null), request, null, listener);
        }

        @Override
        boolean preflightChecks(CreateIndexRequest createIndex, MetaData clustMetaData) {
            return true; // stub all preflight checks to force the migration to coalesce
        }

        @Override
        void startMigration(MigrateIndexTask task) {
            throw new RuntimeException("Tests should override this to short circuit. "
                    + "We aren't interested in testing the entire migration implementation, just the coalescing part.");
        }
    }
}
