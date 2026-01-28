/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.search.SearchTransportService.FETCH_ID_ACTION_NAME;
import static org.elasticsearch.search.fetch.chunk.TransportFetchPhaseCoordinationAction.CHUNKED_FETCH_PHASE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for {@link TransportFetchPhaseCoordinationAction}.
 */
public class TransportFetchPhaseCoordinationActionTests extends ESTestCase {

    private static final ShardId TEST_SHARD_ID = new ShardId(new Index("test-index", "test-uuid"), 0);

    private ThreadPool threadPool;
    private MockTransportService transportService;
    private ActiveFetchPhaseTasks activeFetchPhaseTasks;
    private NamedWriteableRegistry namedWriteableRegistry;
    private TransportFetchPhaseCoordinationAction action;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            CHUNKED_FETCH_PHASE,
            threadPool
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        activeFetchPhaseTasks = new ActiveFetchPhaseTasks();
        namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());

        action = new TransportFetchPhaseCoordinationAction(
            transportService,
            new ActionFilters(Set.of()),
            activeFetchPhaseTasks,
            new NoneCircuitBreakerService(),
            namedWriteableRegistry
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (transportService != null) {
            transportService.close();
        }
        if (threadPool != null) {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testActionType() {
        assertThat(TransportFetchPhaseCoordinationAction.TYPE.name(), equalTo("internal:data/read/search/fetch/coordination"));
    }

    public void testDoExecuteSetsCoordinatorNodeAndTaskIdOnRequest() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        transportService.registerRequestHandler(
            FETCH_ID_ACTION_NAME,
            threadPool.executor(ThreadPool.Names.GENERIC),
            ShardFetchSearchRequest::new,
            (req, channel, task) -> {
                latch.countDown();
                FetchSearchResult result = createFetchSearchResult();
                try {
                    channel.sendResponse(result);
                } finally {
                    result.decRef();
                }
            }
        );

        ShardFetchSearchRequest shardFetchRequest = createShardFetchSearchRequest();
        TransportFetchPhaseCoordinationAction.Request request = new TransportFetchPhaseCoordinationAction.Request(
            shardFetchRequest,
            transportService.getLocalNode(),
            Collections.emptyMap()
        );

        long taskId = 123L;
        Task task = createTask(taskId);
        PlainActionFuture<TransportFetchPhaseCoordinationAction.Response> future = new PlainActionFuture<>();
        action.doExecute(task, request, future);

        assertTrue("Request handler should be called", latch.await(10, TimeUnit.SECONDS));
        assertThat(shardFetchRequest.getCoordinatingNode(), equalTo(transportService.getLocalNode()));
        assertThat(shardFetchRequest.getCoordinatingTaskId(), equalTo(taskId));
    }

    public void testDoExecuteWithParentTaskId() throws Exception {
        AtomicReference<TaskId> capturedParentTaskId = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        transportService.registerRequestHandler(
            FETCH_ID_ACTION_NAME,
            threadPool.executor(ThreadPool.Names.GENERIC),
            ShardFetchSearchRequest::new,
            (req, channel, task) -> {
                capturedParentTaskId.set(req.getParentTask());
                latch.countDown();
                FetchSearchResult result = null;
                try {
                    result = createFetchSearchResult();
                    channel.sendResponse(result);
                } finally {
                    if (result != null) {
                        result.decRef();
                    }
                }
            }
        );

        TransportFetchPhaseCoordinationAction.Request request = new TransportFetchPhaseCoordinationAction.Request(
            createShardFetchSearchRequest(),
            transportService.getLocalNode(),
            Collections.emptyMap()
        );

        TaskId parentTaskId = new TaskId("parent-node", 999L);
        Task task = createTaskWithParent(123L, parentTaskId);
        PlainActionFuture<TransportFetchPhaseCoordinationAction.Response> future = new PlainActionFuture<>();

        action.doExecute(task, request, future);

        assertTrue("Request handler should be called", latch.await(10, TimeUnit.SECONDS));
        assertThat(capturedParentTaskId.get(), equalTo(parentTaskId));
    }

    public void testDoExecuteWithHeaders() throws Exception {
        AtomicReference<String> capturedHeader = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        transportService.registerRequestHandler(
            FETCH_ID_ACTION_NAME,
            threadPool.executor(ThreadPool.Names.GENERIC),
            ShardFetchSearchRequest::new,
            (req, channel, task) -> {
                ThreadContext threadContext = threadPool.getThreadContext();
                capturedHeader.set(threadContext.getHeader("X-Test-Header"));
                latch.countDown();
                FetchSearchResult result = createFetchSearchResult();
                try {
                    channel.sendResponse(result);
                } finally {
                    result.decRef();
                }
            }
        );

        TransportFetchPhaseCoordinationAction.Request request = new TransportFetchPhaseCoordinationAction.Request(
            createShardFetchSearchRequest(),
            transportService.getLocalNode(),
            Map.of("X-Test-Header", "test-value", "X-Another-Header", "another-value")
        );

        PlainActionFuture<TransportFetchPhaseCoordinationAction.Response> future = new PlainActionFuture<>();
        action.doExecute(createTask(123L), request, future);

        assertTrue("Request handler should be called", latch.await(10, TimeUnit.SECONDS));
        assertThat(capturedHeader.get(), equalTo("test-value"));
    }

    public void testDoExecuteReturnsResponseOnSuccess() {
        FetchSearchResult expectedResult = createFetchSearchResult();

        transportService.registerRequestHandler(
            FETCH_ID_ACTION_NAME,
            threadPool.executor(ThreadPool.Names.GENERIC),
            ShardFetchSearchRequest::new,
            (req, channel, task) -> {
                channel.sendResponse(expectedResult);
            }
        );

        TransportFetchPhaseCoordinationAction.Request request = new TransportFetchPhaseCoordinationAction.Request(
            createShardFetchSearchRequest(),
            transportService.getLocalNode(),
            Collections.emptyMap()
        );

        PlainActionFuture<TransportFetchPhaseCoordinationAction.Response> future = new PlainActionFuture<>();
        action.doExecute(createTask(123L), request, future);
        TransportFetchPhaseCoordinationAction.Response response = future.actionGet(10, TimeUnit.SECONDS);

        try {
            assertThat(response, notNullValue());
            assertThat(response.getResult(), notNullValue());
            assertEquals(response.getResult().getContextId(), expectedResult.getContextId());
        } finally {
            expectedResult.decRef();
        }
    }

    public void testDoExecuteHandlesFailure() {
        RuntimeException expectedException = new RuntimeException("Test failure");

        transportService.registerRequestHandler(
            FETCH_ID_ACTION_NAME,
            threadPool.executor(ThreadPool.Names.GENERIC),
            ShardFetchSearchRequest::new,
            (req, channel, task) -> {
                channel.sendResponse(expectedException);
            }
        );

        TransportFetchPhaseCoordinationAction.Request request = new TransportFetchPhaseCoordinationAction.Request(
            createShardFetchSearchRequest(),
            transportService.getLocalNode(),
            Collections.emptyMap()
        );

        PlainActionFuture<TransportFetchPhaseCoordinationAction.Response> future = new PlainActionFuture<>();
        action.doExecute(createTask(123L), request, future);
        Exception caughtException = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
        assertThat(caughtException.getMessage(), equalTo("Test failure"));
    }

    public void testDoExecuteProcessesLastChunkInResponse() {
        transportService.registerRequestHandler(
            FETCH_ID_ACTION_NAME,
            threadPool.executor(ThreadPool.Names.GENERIC),
            ShardFetchSearchRequest::new,
            (req, channel, task) -> {
                FetchSearchResult result = createFetchSearchResult();
                try {

                    BytesStreamOutput out = new BytesStreamOutput();
                    SearchHit hit = createHit(0);
                    hit.writeTo(out);
                    hit.decRef();

                    result.setLastChunkBytes(out.bytes(), 1);
                    result.setLastChunkSequenceStart(0L);

                    channel.sendResponse(result);
                } finally {
                    result.decRef();
                }
            }
        );

        TransportFetchPhaseCoordinationAction.Request request = new TransportFetchPhaseCoordinationAction.Request(
            createShardFetchSearchRequest(),
            transportService.getLocalNode(),
            Collections.emptyMap()
        );

        PlainActionFuture<TransportFetchPhaseCoordinationAction.Response> future = new PlainActionFuture<>();
        action.doExecute(createTask(123L), request, future);
        TransportFetchPhaseCoordinationAction.Response response = future.actionGet(10, TimeUnit.SECONDS);

        assertThat(response, notNullValue());
        assertThat(response.getResult(), notNullValue());
    }

    public void testDoExecutePreservesContextIdInFinalResult() throws Exception {
        ShardSearchContextId expectedContextId = new ShardSearchContextId("expected-session", 12345L);
        SearchShardTarget expectedShardTarget = new SearchShardTarget("node1", TEST_SHARD_ID, null);

        transportService.registerRequestHandler(
            FETCH_ID_ACTION_NAME,
            threadPool.executor(ThreadPool.Names.GENERIC),
            ShardFetchSearchRequest::new,
            (req, channel, task) -> {
                FetchSearchResult result = new FetchSearchResult(expectedContextId, expectedShardTarget);
                try {
                    channel.sendResponse(result);
                } finally {
                    result.decRef();
                }
            }
        );

        TransportFetchPhaseCoordinationAction.Request request = new TransportFetchPhaseCoordinationAction.Request(
            createShardFetchSearchRequest(),
            transportService.getLocalNode(),
            Collections.emptyMap()
        );

        PlainActionFuture<TransportFetchPhaseCoordinationAction.Response> future = new PlainActionFuture<>();
        action.doExecute(createTask(123L), request, future);
        TransportFetchPhaseCoordinationAction.Response response = future.actionGet(10, TimeUnit.SECONDS);

        assertThat(response.getResult().getContextId().getId(), equalTo(expectedContextId.getId()));
        assertThat(response.getResult().getSearchShardTarget(), equalTo(expectedShardTarget));
    }

    private ShardFetchSearchRequest createShardFetchSearchRequest() {
        ShardSearchContextId contextId = new ShardSearchContextId("test", randomLong());

        OriginalIndices originalIndices = new OriginalIndices(
            new String[] { "test-index" },
            IndicesOptions.strictExpandOpenAndForbidClosed()
        );
        List<Integer> docIds = List.of(0, 1, 2, 3, 4);

        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(TEST_SHARD_ID, System.currentTimeMillis(), AliasFilter.EMPTY);

        return new ShardFetchSearchRequest(originalIndices, contextId, shardSearchRequest, docIds, null, null, RescoreDocIds.EMPTY, null);
    }

    private FetchSearchResult createFetchSearchResult() {
        ShardSearchContextId contextId = new ShardSearchContextId("test", randomLong());
        FetchSearchResult result = new FetchSearchResult(contextId, new SearchShardTarget("node", TEST_SHARD_ID, null));
        result.shardResult(SearchHits.unpooled(new SearchHit[0], null, Float.NaN), null);
        return result;
    }

    private SearchHit createHit(int id) {
        SearchHit hit = new SearchHit(id);
        hit.sourceRef(new BytesArray("{\"id\":" + id + "}"));
        return hit;
    }

    private Task createTask(long taskId) {
        return new Task(taskId, "transport", "action", "description", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }

    private Task createTaskWithParent(long taskId, TaskId parentTaskId) {
        return new Task(taskId, "transport", "action", "description", parentTaskId, Collections.emptyMap());
    }
}
