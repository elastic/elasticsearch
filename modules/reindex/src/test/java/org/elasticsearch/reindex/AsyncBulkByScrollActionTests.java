/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ClientScrollableHitSource;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.index.reindex.ScrollableHitSource.Hit;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.index.reindex.WorkerBulkByScrollTaskState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedSet;
import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.elasticsearch.common.BackoffPolicy.constantBackoff;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AsyncBulkByScrollActionTests extends ESTestCase {
    private MyMockClient client;
    private DummyAbstractBulkByScrollRequest testRequest;
    private PlainActionFuture<BulkByScrollResponse> listener;
    private String scrollId;
    private ThreadPool threadPool;
    private ThreadPool clientThreadPool;
    private TaskManager taskManager;
    private BulkByScrollTask testTask;
    private WorkerBulkByScrollTaskState worker;
    private Map<String, String> expectedHeaders = new HashMap<>();
    private DiscoveryNode localNode;
    private TaskId taskId;

    @Before
    public void setupForTest() {
        // Fill the context with something random so we can make sure we inherited it appropriately.
        expectedHeaders.clear();
        expectedHeaders.put(randomSimpleString(random()), randomSimpleString(random()));

        threadPool = new TestThreadPool(getTestName());
        setupClient(threadPool);
        testRequest = new DummyAbstractBulkByScrollRequest(new SearchRequest());
        listener = new PlainActionFuture<>();
        scrollId = null;
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        testTask = (BulkByScrollTask) taskManager.register("don'tcare", "hereeither", testRequest);
        testTask.setWorker(testRequest.getRequestsPerSecond(), null);
        worker = testTask.getWorkerState();

        localNode = DiscoveryNodeUtils.builder("thenode").roles(emptySet()).build();
        taskId = new TaskId(localNode.getId(), testTask.getId());
    }

    private void setupClient(ThreadPool threadPool) {
        if (clientThreadPool != null) {
            terminate(clientThreadPool);
        }
        clientThreadPool = threadPool;
        client = new MyMockClient(new NoOpClient(threadPool));
        client.threadPool().getThreadContext().putHeader(expectedHeaders);
    }

    @After
    public void tearDownAndVerifyCommonStuff() throws Exception {
        terminate(clientThreadPool);
        clientThreadPool = null;
        terminate(threadPool);
    }

    /**
     * Generates a random scrollId and registers it so that when the test
     * finishes we check that it was cleared. Subsequent calls reregister a new
     * random scroll id so it is checked instead.
     */
    private String scrollId() {
        scrollId = randomSimpleString(random(), 1, 10); // Empty strings get special behavior we don't want
        return scrollId;
    }

    public void testStartRetriesOnRejectionAndSucceeds() throws Exception {
        client.searchesToReject = randomIntBetween(0, testRequest.getMaxRetries() - 1);
        DummyAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.start();
        assertBusy(() -> assertEquals(client.searchesToReject + 1, client.searchAttempts.get()));
        if (listener.isDone()) {
            Object result = listener.get();
            fail("Expected listener not to be done but it was and had " + result);
        }
        assertBusy(() -> assertNotNull("There should be a search attempt pending that we didn't reject", client.lastSearch.get()));
        assertEquals(client.searchesToReject, testTask.getStatus().getSearchRetries());
    }

    public void testStartRetriesOnRejectionButFailsOnTooManyRejections() throws Exception {
        client.searchesToReject = testRequest.getMaxRetries() + randomIntBetween(1, 100);
        DummyAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.start();
        assertBusy(() -> assertEquals(testRequest.getMaxRetries() + 1, client.searchAttempts.get()));
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(ExceptionsHelper.stackTrace(e), containsString(EsRejectedExecutionException.class.getSimpleName()));
        assertNull("There shouldn't be a search attempt pending that we didn't reject", client.lastSearch.get());
        assertEquals(testRequest.getMaxRetries(), testTask.getStatus().getSearchRetries());
    }

    public void testStartNextScrollRetriesOnRejectionAndSucceeds() throws Exception {
        // this test primarily tests ClientScrollableHitSource but left it to test integration to status
        client.scrollsToReject = randomIntBetween(0, testRequest.getMaxRetries() - 1);
        // use fail() onResponse handler because mocked search never fires on listener.
        ClientScrollableHitSource hitSource = new ClientScrollableHitSource(
            logger,
            buildTestBackoffPolicy(),
            threadPool,
            testTask.getWorkerState()::countSearchRetry,
            r -> fail(),
            ExceptionsHelper::reThrowIfNotNull,
            new ParentTaskAssigningClient(client, localNode, testTask),
            testRequest.getSearchRequest()
        );
        hitSource.setScroll(scrollId());
        hitSource.startNextScroll(TimeValue.timeValueSeconds(0));
        assertBusy(() -> assertEquals(client.scrollsToReject + 1, client.scrollAttempts.get()));
        if (listener.isDone()) {
            Object result = listener.get();
            fail("Expected listener not to be done but it was and had " + result);
        }
        assertBusy(() -> assertNotNull("There should be a scroll attempt pending that we didn't reject", client.lastScroll.get()));
        assertEquals(client.scrollsToReject, testTask.getStatus().getSearchRetries());
    }

    public void testStartNextScrollRetriesOnRejectionButFailsOnTooManyRejections() throws Exception {
        // this test primarily tests ClientScrollableHitSource but left it to test integration to status
        client.scrollsToReject = testRequest.getMaxRetries() + randomIntBetween(1, 100);
        assertExactlyOnce(onFail -> {
            Consumer<Exception> validingOnFail = e -> {
                assertNotNull(ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class));
                onFail.run();
            };
            ClientScrollableHitSource hitSource = new ClientScrollableHitSource(
                logger,
                buildTestBackoffPolicy(),
                threadPool,
                testTask.getWorkerState()::countSearchRetry,
                r -> fail(),
                validingOnFail,
                new ParentTaskAssigningClient(client, localNode, testTask),
                testRequest.getSearchRequest()
            );
            hitSource.setScroll(scrollId());
            hitSource.startNextScroll(TimeValue.timeValueSeconds(0));
            assertBusy(() -> assertEquals(testRequest.getMaxRetries() + 1, client.scrollAttempts.get()));
        });
        assertNull("There shouldn't be a scroll attempt pending that we didn't reject", client.lastScroll.get());
        assertEquals(testRequest.getMaxRetries(), testTask.getStatus().getSearchRetries());
    }

    public void testScrollResponseSetsTotal() {
        // Default is 0, meaning unstarted
        assertEquals(0, testTask.getStatus().getTotal());

        long total = randomIntBetween(0, Integer.MAX_VALUE);
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), total, emptyList(), null);
        simulateScrollResponse(new DummyAsyncBulkByScrollAction(), 0, 0, response);
        assertEquals(total, testTask.getStatus().getTotal());
    }

    /**
     * Tests that each scroll response is a batch and that the batch is launched properly.
     */
    public void testScrollResponseBatchingBehavior() throws Exception {
        int maxBatches = randomIntBetween(0, 100);
        for (int batches = 1; batches < maxBatches; batches++) {
            Hit hit = new ScrollableHitSource.BasicHit("index", "id", 0);
            ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), 1, singletonList(hit), null);
            DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction();
            simulateScrollResponse(action, System.nanoTime(), 0, response);

            // Use assert busy because the update happens on another thread
            final int expectedBatches = batches;
            assertBusy(() -> assertEquals(expectedBatches, testTask.getStatus().getBatches()));
        }
    }

    public void testBulkResponseSetsLotsOfStatus() throws Exception {
        testRequest.setAbortOnVersionConflict(false);

        int maxBatches = randomIntBetween(0, 100);
        long versionConflicts = 0;
        long created = 0;
        long updated = 0;
        long deleted = 0;

        var action = new DummyAsyncBulkByScrollAction();
        action.setScroll(scrollId());

        for (int batches = 0; batches < maxBatches; batches++) {
            BulkItemResponse[] responses = new BulkItemResponse[randomIntBetween(0, 100)];
            for (int i = 0; i < responses.length; i++) {
                ShardId shardId = new ShardId(new Index("name", "uid"), 0);
                if (rarely()) {
                    versionConflicts++;
                    responses[i] = BulkItemResponse.failure(
                        i,
                        randomFrom(DocWriteRequest.OpType.values()),
                        new Failure(shardId.getIndexName(), "id" + i, new VersionConflictEngineException(shardId, "id", "test"))
                    );
                    continue;
                }
                boolean createdResponse;
                DocWriteRequest.OpType opType;
                switch (randomIntBetween(0, 2)) {
                    case 0 -> {
                        createdResponse = true;
                        opType = DocWriteRequest.OpType.CREATE;
                        created++;
                    }
                    case 1 -> {
                        createdResponse = false;
                        opType = randomFrom(DocWriteRequest.OpType.INDEX, DocWriteRequest.OpType.UPDATE);
                        updated++;
                    }
                    case 2 -> {
                        createdResponse = false;
                        opType = DocWriteRequest.OpType.DELETE;
                        deleted++;
                    }
                    default -> throw new RuntimeException("Bad scenario");
                }
                final int seqNo = randomInt(20);
                final int primaryTerm = randomIntBetween(1, 16);
                final IndexResponse response = new IndexResponse(shardId, "id" + i, seqNo, primaryTerm, randomInt(), createdResponse);
                responses[i] = BulkItemResponse.success(i, opType, response);
            }
            assertExactlyOnce(onSuccess -> action.onBulkResponse(new BulkResponse(responses, 0), onSuccess));
            assertEquals(versionConflicts, testTask.getStatus().getVersionConflicts());
            assertEquals(updated, testTask.getStatus().getUpdated());
            assertEquals(created, testTask.getStatus().getCreated());
            assertEquals(deleted, testTask.getStatus().getDeleted());
            assertEquals(versionConflicts, testTask.getStatus().getVersionConflicts());
        }
    }

    public void testHandlesBulkWithNoScroll() {
        // given a request that should not open scroll
        var maxDocs = between(1, 100);
        testRequest.setMaxDocs(maxDocs);
        testRequest.getSearchRequest().source().size(100);

        // when receiving bulk response
        var responses = randomArray(0, maxDocs, BulkItemResponse[]::new, AsyncBulkByScrollActionTests::createBulkResponse);
        new DummyAsyncBulkByScrollAction().onBulkResponse(new BulkResponse(responses, 0), () -> fail("should not be called"));

        // then should refresh and finish
        assertThat(listener.isDone(), equalTo(true));
        var status = listener.actionGet().getStatus();
        assertThat(status.getCreated() + status.getUpdated() + status.getDeleted(), equalTo((long) responses.length));
    }

    public void testHandlesBulkWhenMaxDocsIsReached() {
        // given a request with max docs
        var size = between(1, 10);
        testRequest.setMaxDocs(size);
        testRequest.getSearchRequest().source().size(100);

        // when receiving bulk response with max docs
        var responses = randomArray(size, size, BulkItemResponse[]::new, AsyncBulkByScrollActionTests::createBulkResponse);
        new DummyAsyncBulkByScrollAction().onBulkResponse(new BulkResponse(responses, 0), () -> fail("should not be called"));

        // then should refresh and finish
        assertThat(listener.isDone(), equalTo(true));
        var status = listener.actionGet().getStatus();
        assertThat(status.getCreated() + status.getUpdated() + status.getDeleted(), equalTo((long) responses.length));
    }

    private static BulkItemResponse createBulkResponse() {
        return BulkItemResponse.success(
            0,
            randomFrom(DocWriteRequest.OpType.values()),
            new IndexResponse(
                new ShardId(new Index("name", "uid"), 0),
                "id",
                randomInt(20),
                randomIntBetween(1, 16),
                randomIntBetween(0, Integer.MAX_VALUE),
                true
            )
        );
    }

    /**
     * Mimicks a ThreadPool rejecting execution of the task.
     */
    public void testThreadPoolRejectionsAbortRequest() throws Exception {
        worker.rethrottle(1);
        setupClient(new TestThreadPool(getTestName()) {
            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                // While we're here we can check that the sleep made it through
                assertThat(delay.nanos(), greaterThan(0L));
                assertThat(delay.seconds(), lessThanOrEqualTo(10L));
                final EsRejectedExecutionException exception = new EsRejectedExecutionException("test");
                if (command instanceof AbstractRunnable) {
                    ((AbstractRunnable) command).onRejection(exception);
                    return null;
                } else {
                    throw exception;
                }
            }
        });
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), 0, emptyList(), null);
        simulateScrollResponse(new DummyAsyncBulkByScrollAction(), System.nanoTime(), 10, response);
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(e.getCause(), instanceOf(EsRejectedExecutionException.class));
        assertThat(e.getCause(), hasToString(containsString("test")));
        assertThat(client.scrollsCleared, contains(scrollId));

        // When the task is rejected we don't increment the throttled timer
        assertEquals(timeValueMillis(0), testTask.getStatus().getThrottled());
    }

    /**
     * Mimicks shard search failures usually caused by the data node serving the
     * scroll request going down.
     */
    public void testShardFailuresAbortRequest() throws Exception {
        SearchFailure shardFailure = new SearchFailure(new RuntimeException("test"));
        ScrollableHitSource.Response scrollResponse = new ScrollableHitSource.Response(
            false,
            singletonList(shardFailure),
            0,
            emptyList(),
            null
        );
        simulateScrollResponse(new DummyAsyncBulkByScrollAction(), System.nanoTime(), 0, scrollResponse);
        BulkByScrollResponse response = listener.get();
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getSearchFailures(), contains(shardFailure));
        assertFalse(response.isTimedOut());
        assertNull(response.getReasonCancelled());
        assertThat(client.scrollsCleared, contains(scrollId));
    }

    /**
     * Mimicks search timeouts.
     */
    public void testSearchTimeoutsAbortRequest() throws Exception {
        ScrollableHitSource.Response scrollResponse = new ScrollableHitSource.Response(true, emptyList(), 0, emptyList(), null);
        simulateScrollResponse(new DummyAsyncBulkByScrollAction(), System.nanoTime(), 0, scrollResponse);
        BulkByScrollResponse response = listener.get();
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getSearchFailures(), empty());
        assertTrue(response.isTimedOut());
        assertNull(response.getReasonCancelled());
        assertThat(client.scrollsCleared, contains(scrollId));
    }

    /**
     * Mimicks bulk indexing failures.
     */
    public void testBulkFailuresAbortRequest() throws Exception {
        Failure failure = new Failure("index", "id", new RuntimeException("test"));
        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction();
        BulkResponse bulkResponse = new BulkResponse(
            new BulkItemResponse[] { BulkItemResponse.failure(0, DocWriteRequest.OpType.CREATE, failure) },
            randomLong()
        );
        action.onBulkResponse(bulkResponse, Assert::fail);
        BulkByScrollResponse response = listener.get();
        assertThat(response.getBulkFailures(), contains(failure));
        assertThat(response.getSearchFailures(), empty());
        assertNull(response.getReasonCancelled());
    }

    /**
     * Mimicks script failures or general wrongness by implementers.
     */
    public void testBuildRequestThrowsException() throws Exception {
        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction() {
            @Override
            protected AbstractAsyncBulkByScrollAction.RequestWrapper<?> buildRequest(Hit doc) {
                throw new RuntimeException("surprise");
            }
        };
        ScrollableHitSource.BasicHit hit = new ScrollableHitSource.BasicHit("index", "id", 0);
        hit.setSource(new BytesArray("{}"), XContentType.JSON);
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), 1, singletonList(hit), null);
        simulateScrollResponse(action, System.nanoTime(), 0, response);
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(e.getCause(), instanceOf(RuntimeException.class));
        assertThat(e.getCause().getMessage(), equalTo("surprise"));
    }

    /**
     * Mimicks bulk rejections. These should be retried and eventually succeed.
     */
    public void testBulkRejectionsRetryWithEnoughRetries() throws Exception {
        int bulksToTry = randomIntBetween(1, 10);
        long retryAttempts = 0;
        for (int i = 0; i < bulksToTry; i++) {
            bulkRetryTestCase(false);
            retryAttempts += testRequest.getMaxRetries();
            assertEquals(retryAttempts, testTask.getStatus().getBulkRetries());
        }
    }

    /**
     * Mimicks bulk rejections. These should be retried but we fail anyway because we run out of retries.
     */
    public void testBulkRejectionsRetryAndFailAnyway() throws Exception {
        bulkRetryTestCase(true);
        assertEquals(testRequest.getMaxRetries(), testTask.getStatus().getBulkRetries());
    }

    public void testScrollDelay() throws Exception {
        /*
         * Replace the thread pool with one that will save the delay sent for the command. We'll use that to check that we used a proper
         * delay for throttling.
         */
        AtomicReference<TimeValue> capturedDelay = new AtomicReference<>();
        AtomicReference<Runnable> capturedCommand = new AtomicReference<>();
        setupClient(new TestThreadPool(getTestName()) {
            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                capturedDelay.set(delay);
                capturedCommand.set(command);
                return new ScheduledCancellable() {
                    private boolean cancelled = false;

                    @Override
                    public long getDelay(TimeUnit unit) {
                        return unit.convert(delay.millis(), TimeUnit.MILLISECONDS);
                    }

                    @Override
                    public int compareTo(Delayed o) {
                        return 0;
                    }

                    @Override
                    public boolean cancel() {
                        cancelled = true;
                        return true;
                    }

                    @Override
                    public boolean isCancelled() {
                        return cancelled;
                    }
                };
            }
        });

        // Set the base for the scroll to wait - this is added to the figure we calculate below
        testRequest.getSearchRequest().scroll(timeValueSeconds(10));

        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction() {
            @Override
            protected RequestWrapper<?> buildRequest(Hit doc) {
                return wrap(new IndexRequest().index("test"));
            }
        };
        action.setScroll(scrollId());

        // Set throttle to 1 request per second to make the math simpler
        worker.rethrottle(1f);
        action.start();

        // create a simulated response.
        SearchHit hit = SearchHit.unpooled(0, "id").sourceRef(new BytesArray("{}"));
        SearchHits hits = SearchHits.unpooled(
            IntStream.range(0, 100).mapToObj(i -> hit).toArray(SearchHit[]::new),
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            0
        );
        SearchResponse searchResponse = SearchResponseUtils.response(hits).scrollId(scrollId()).shards(5, 4, 0).build();
        try {
            client.lastSearch.get().listener.onResponse(searchResponse);

            assertEquals(0, capturedDelay.get().seconds());
            capturedCommand.get().run();

            // So the next request is going to have to wait an extra 100 seconds or so (base was 10 seconds, so 110ish)
            assertThat(client.lastScroll.get().request.scroll().seconds(), either(equalTo(110L)).or(equalTo(109L)));

            // Now we can simulate a response and check the delay that we used for the task
            if (randomBoolean()) {
                client.lastScroll.get().listener.onResponse(searchResponse);
                assertEquals(99, capturedDelay.get().seconds());
            } else {
                // Let's rethrottle between the starting the scroll and getting the response
                worker.rethrottle(10f);
                client.lastScroll.get().listener.onResponse(searchResponse);
                // The delay uses the new throttle
                assertEquals(9, capturedDelay.get().seconds());
            }

            // Running the command ought to increment the delay counter on the task.
            capturedCommand.get().run();
            assertEquals(capturedDelay.get(), testTask.getStatus().getThrottled());
        } finally {
            searchResponse.decRef();
        }
    }

    /**
     * Execute a bulk retry test case. The total number of failures is random and the number of retries attempted is set to
     * testRequest.getMaxRetries and controlled by the failWithRejection parameter.
     */
    private void bulkRetryTestCase(boolean failWithRejection) throws Exception {
        int totalFailures = randomIntBetween(1, testRequest.getMaxRetries());
        int size = randomIntBetween(1, 100);
        testRequest.setMaxRetries(totalFailures - (failWithRejection ? 1 : 0));

        client.bulksToReject = client.bulksAttempts.get() + totalFailures;
        DummyAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.setScroll(scrollId());
        BulkRequest request = new BulkRequest();
        for (int i = 0; i < size + 1; i++) {
            request.add(new IndexRequest("index").id("id" + i));
        }
        if (failWithRejection) {
            action.sendBulkRequest(request, Assert::fail);
            BulkByScrollResponse response = listener.get();
            assertThat(response.getBulkFailures(), hasSize(1));
            assertEquals(response.getBulkFailures().get(0).getStatus(), RestStatus.TOO_MANY_REQUESTS);
            assertThat(response.getSearchFailures(), empty());
            assertNull(response.getReasonCancelled());
        } else {
            assertExactlyOnce(onSuccess -> action.sendBulkRequest(request, onSuccess));
        }
    }

    /**
     * The default retry time matches what we say it is in the javadoc for the request.
     */
    public void testDefaultRetryTimes() {
        Iterator<TimeValue> policy = new DummyAsyncBulkByScrollAction().buildBackoffPolicy().iterator();
        long millis = 0;
        while (policy.hasNext()) {
            millis += policy.next().millis();
        }
        /*
         * This is the total number of milliseconds that a reindex made with the default settings will backoff before attempting one final
         * time. If that request is rejected then the whole process fails with a rejected exception.
         */
        int defaultBackoffBeforeFailing = 59460;
        assertEquals(defaultBackoffBeforeFailing, millis);
    }

    public void testRefreshIsFalseByDefault() throws Exception {
        refreshTestCase(null, true, false);
    }

    public void testRefreshFalseDoesntExecuteRefresh() throws Exception {
        refreshTestCase(false, true, false);
    }

    public void testRefreshTrueExecutesRefresh() throws Exception {
        refreshTestCase(true, true, true);
    }

    public void testRefreshTrueSkipsRefreshIfNoDestinationIndexes() throws Exception {
        refreshTestCase(true, false, false);
    }

    private void refreshTestCase(Boolean refresh, boolean addDestinationIndexes, boolean shouldRefresh) {
        if (refresh != null) {
            testRequest.setRefresh(refresh);
        }
        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction();
        if (addDestinationIndexes) {
            action.addDestinationIndices(singleton("foo"));
        }
        action.refreshAndFinish(emptyList(), emptyList(), false);
        if (shouldRefresh) {
            assertArrayEquals(new String[] { "foo" }, client.lastRefreshRequest.get().indices());
        } else {
            assertNull("No refresh was attempted", client.lastRefreshRequest.get());
        }
    }

    public void testCancelBeforeInitialSearch() throws Exception {
        cancelTaskCase((DummyAsyncBulkByScrollAction action) -> action.start());
    }

    public void testCancelBeforeScrollResponse() throws Exception {
        cancelTaskCase(
            (DummyAsyncBulkByScrollAction action) -> simulateScrollResponse(
                action,
                System.nanoTime(),
                1,
                new ScrollableHitSource.Response(false, emptyList(), between(1, 100000), emptyList(), null)
            )
        );
    }

    public void testCancelBeforeSendBulkRequest() throws Exception {
        cancelTaskCase((DummyAsyncBulkByScrollAction action) -> action.sendBulkRequest(new BulkRequest(), Assert::fail));
    }

    public void testCancelBeforeOnBulkResponse() throws Exception {
        cancelTaskCase(
            (DummyAsyncBulkByScrollAction action) -> action.onBulkResponse(new BulkResponse(new BulkItemResponse[0], 0), Assert::fail)
        );
    }

    public void testCancelBeforeStartNextScroll() throws Exception {
        long now = System.nanoTime();
        cancelTaskCase((DummyAsyncBulkByScrollAction action) -> action.notifyDone(now, null, 0));
    }

    public void testCancelBeforeRefreshAndFinish() throws Exception {
        // Refresh or not doesn't matter - we don't try to refresh.
        testRequest.setRefresh(usually());
        cancelTaskCase((DummyAsyncBulkByScrollAction action) -> action.refreshAndFinish(emptyList(), emptyList(), false));
        assertNull("No refresh was attempted", client.lastRefreshRequest.get());
    }

    /**
     * Tests that we can cancel the request during its throttling delay. This can't use {@link #cancelTaskCase(Consumer)} because it needs
     * to send the request un-canceled and cancel it at a specific time.
     */
    public void testCancelWhileDelayedAfterScrollResponse() throws Exception {
        String reason = randomSimpleString(random());

        /*
         * Replace the thread pool with one that will cancel the task as soon as anything is scheduled, which reindex tries to do when there
         * is a delay.
         */
        setupClient(new TestThreadPool(getTestName()) {
            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                /*
                 * This is called twice:
                 * 1. To schedule the throttling. When that happens we immediately cancel the task.
                 * 2. After the task is canceled.
                 * Both times we use delegate to the standard behavior so the task is scheduled as expected so it can be cancelled and all
                 * that good stuff.
                 */
                if (delay.nanos() > 0) {
                    generic().execute(() -> taskManager.cancel(testTask, reason, () -> {}));
                }
                return super.schedule(command, delay, executor);
            }
        });

        // Send the scroll response which will trigger the custom thread pool above, canceling the request before running the response
        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction();
        boolean previousScrollSet = usually();
        if (previousScrollSet) {
            action.setScroll(scrollId());
        }
        long total = randomIntBetween(0, Integer.MAX_VALUE);
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), total, emptyList(), null);
        // Use a long delay here so the test will time out if the cancellation doesn't reschedule the throttled task
        worker.rethrottle(1);
        simulateScrollResponse(action, System.nanoTime(), 1000, response);

        // Now that we've got our cancel we'll just verify that it all came through all right
        assertEquals(reason, listener.get(10, TimeUnit.SECONDS).getReasonCancelled());
        if (previousScrollSet) {
            // Canceled tasks always start to clear the scroll before they die.
            assertThat(client.scrollsCleared, contains(scrollId));
        }
    }

    private void cancelTaskCase(Consumer<DummyAsyncBulkByScrollAction> testMe) throws Exception {
        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction();
        boolean previousScrollSet = usually();
        if (previousScrollSet) {
            action.setScroll(scrollId());
        }
        String reason = randomSimpleString(random());
        taskManager.cancel(testTask, reason, () -> {});
        testMe.accept(action);
        assertEquals(reason, listener.get().getReasonCancelled());
        if (previousScrollSet) {
            // Canceled tasks always start to clear the scroll before they die.
            assertThat(client.scrollsCleared, contains(scrollId));
        }
    }

    public void testScrollConsumableHitsResponseCanBeConsumedInChunks() {
        List<ScrollableHitSource.BasicHit> hits = new ArrayList<>();
        int numberOfHits = randomIntBetween(0, 300);
        for (int i = 0; i < numberOfHits; i++) {
            hits.add(new ScrollableHitSource.BasicHit("idx", "id-" + i, -1));
        }
        final ScrollableHitSource.Response scrollResponse = new ScrollableHitSource.Response(
            false,
            emptyList(),
            hits.size(),
            hits,
            "scrollid"
        );
        final AbstractAsyncBulkByScrollAction.ScrollConsumableHitsResponse response =
            new AbstractAsyncBulkByScrollAction.ScrollConsumableHitsResponse(new ScrollableHitSource.AsyncResponse() {
                @Override
                public ScrollableHitSource.Response response() {
                    return scrollResponse;
                }

                @Override
                public void done(TimeValue extraKeepAlive) {}
            });

        assertThat(response.remainingHits(), equalTo(numberOfHits));
        assertThat(response.hasRemainingHits(), equalTo(numberOfHits > 0));

        int totalConsumedHits = 0;
        while (response.hasRemainingHits()) {
            final int numberOfHitsToConsume;
            final List<? extends ScrollableHitSource.Hit> consumedHits;
            if (randomBoolean()) {
                numberOfHitsToConsume = numberOfHits - totalConsumedHits;
                consumedHits = response.consumeRemainingHits();
            } else {
                numberOfHitsToConsume = randomIntBetween(1, numberOfHits - totalConsumedHits);
                consumedHits = response.consumeHits(numberOfHitsToConsume);
            }

            assertThat(consumedHits.size(), equalTo(numberOfHitsToConsume));
            assertThat(consumedHits, equalTo(hits.subList(totalConsumedHits, totalConsumedHits + numberOfHitsToConsume)));
            totalConsumedHits += numberOfHitsToConsume;

            assertThat(response.remainingHits(), equalTo(numberOfHits - totalConsumedHits));
        }

        assertThat(response.consumeRemainingHits().isEmpty(), equalTo(true));
    }

    public void testScrollConsumableHitsResponseErrorHandling() {
        List<ScrollableHitSource.BasicHit> hits = new ArrayList<>();
        int numberOfHits = randomIntBetween(2, 300);
        for (int i = 0; i < numberOfHits; i++) {
            hits.add(new ScrollableHitSource.BasicHit("idx", "id-" + i, -1));
        }

        final ScrollableHitSource.Response scrollResponse = new ScrollableHitSource.Response(
            false,
            emptyList(),
            hits.size(),
            hits,
            "scrollid"
        );
        final AbstractAsyncBulkByScrollAction.ScrollConsumableHitsResponse response =
            new AbstractAsyncBulkByScrollAction.ScrollConsumableHitsResponse(new ScrollableHitSource.AsyncResponse() {
                @Override
                public ScrollableHitSource.Response response() {
                    return scrollResponse;
                }

                @Override
                public void done(TimeValue extraKeepAlive) {}
            });

        assertThat(response.remainingHits(), equalTo(numberOfHits));
        assertThat(response.hasRemainingHits(), equalTo(true));

        expectThrows(IllegalArgumentException.class, () -> response.consumeHits(-1));
        expectThrows(IllegalArgumentException.class, () -> response.consumeHits(numberOfHits + 1));

        if (randomBoolean()) {
            response.consumeHits(numberOfHits - 1);
            // Unable to consume more than remaining hits
            expectThrows(IllegalArgumentException.class, () -> response.consumeHits(response.remainingHits() + 1));
            response.consumeHits(1);
        } else {
            response.consumeRemainingHits();
        }

        expectThrows(IllegalArgumentException.class, () -> response.consumeHits(1));
    }

    public void testEnableScrollByDefault() {
        var preparedSearchRequest = AbstractAsyncBulkByScrollAction.prepareSearchRequest(testRequest, false, false);
        assertThat(preparedSearchRequest.scroll(), notNullValue());
    }

    public void testEnableScrollWhenMaxDocsIsGreaterThenScrollSize() {
        testRequest.setMaxDocs(between(101, 1000));
        testRequest.getSearchRequest().source().size(100);

        var preparedSearchRequest = AbstractAsyncBulkByScrollAction.prepareSearchRequest(testRequest, false, false);

        assertThat(preparedSearchRequest.scroll(), notNullValue());
    }

    public void testDisableScrollWhenMaxDocsIsLessThenScrollSize() {
        testRequest.setMaxDocs(between(1, 100));
        testRequest.getSearchRequest().source().size(100);

        var preparedSearchRequest = AbstractAsyncBulkByScrollAction.prepareSearchRequest(testRequest, false, false);

        assertThat(preparedSearchRequest.scroll(), nullValue());
    }

    public void testEnableScrollWhenProceedOnVersionConflict() {
        testRequest.setMaxDocs(between(1, 110));
        testRequest.getSearchRequest().source().size(100);
        testRequest.setAbortOnVersionConflict(false);

        var preparedSearchRequest = AbstractAsyncBulkByScrollAction.prepareSearchRequest(testRequest, false, false);

        assertThat(preparedSearchRequest.scroll(), notNullValue());
    }

    /**
     * Simulate a scroll response by setting the scroll id and firing the onScrollResponse method.
     */
    private void simulateScrollResponse(
        DummyAsyncBulkByScrollAction action,
        long lastBatchTime,
        int lastBatchSize,
        ScrollableHitSource.Response response
    ) {
        action.setScroll(scrollId());
        action.onScrollResponse(
            lastBatchTime,
            lastBatchSize,
            new AbstractAsyncBulkByScrollAction.ScrollConsumableHitsResponse(new ScrollableHitSource.AsyncResponse() {
                @Override
                public ScrollableHitSource.Response response() {
                    return response;
                }

                @Override
                public void done(TimeValue extraKeepAlive) {
                    fail();
                }
            })
        );
    }

    private class DummyAsyncBulkByScrollAction extends AbstractAsyncBulkByScrollAction<
        DummyAbstractBulkByScrollRequest,
        DummyTransportAsyncBulkByScrollAction> {
        DummyAsyncBulkByScrollAction() {
            super(
                testTask,
                randomBoolean(),
                randomBoolean(),
                AsyncBulkByScrollActionTests.this.logger,
                new ParentTaskAssigningClient(client, localNode, testTask),
                client.threadPool(),
                testRequest,
                listener,
                null,
                null
            );
        }

        @Override
        protected AbstractAsyncBulkByScrollAction.RequestWrapper<?> buildRequest(Hit doc) {
            throw new UnsupportedOperationException("Use another override to test this.");
        }
    }

    /**
     * An extension to {@linkplain DummyAsyncBulkByScrollAction} that uses a 0 delaying backoff policy.
     */
    private class DummyActionWithoutBackoff extends DummyAsyncBulkByScrollAction {
        @Override
        BackoffPolicy buildBackoffPolicy() {
            return buildTestBackoffPolicy();
        }
    }

    private BackoffPolicy buildTestBackoffPolicy() {
        // Force a backoff time of 0 to prevent sleeping
        return constantBackoff(timeValueMillis(0), testRequest.getMaxRetries());
    }

    private static class DummyTransportAsyncBulkByScrollAction extends TransportAction<
        DummyAbstractBulkByScrollRequest,
        BulkByScrollResponse> {

        protected DummyTransportAsyncBulkByScrollAction(String actionName, ActionFilters actionFilters, TaskManager taskManager) {
            super(actionName, actionFilters, taskManager, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        }

        @Override
        protected void doExecute(Task task, DummyAbstractBulkByScrollRequest request, ActionListener<BulkByScrollResponse> listener) {
            // no-op
        }
    }

    private static class DummyAbstractBulkByScrollRequest extends AbstractBulkByScrollRequest<DummyAbstractBulkByScrollRequest> {

        DummyAbstractBulkByScrollRequest(SearchRequest searchRequest) {
            super(searchRequest, true);
        }

        @Override
        public DummyAbstractBulkByScrollRequest forSlice(TaskId slicingTask, SearchRequest slice, int totalSlices) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected DummyAbstractBulkByScrollRequest self() {
            return this;
        }
    }

    private class MyMockClient extends FilterClient {
        private final List<String> scrollsCleared = new ArrayList<>();
        private final AtomicInteger bulksAttempts = new AtomicInteger();
        private final AtomicInteger searchAttempts = new AtomicInteger();
        private final AtomicInteger scrollAttempts = new AtomicInteger();
        private final AtomicReference<RefreshRequest> lastRefreshRequest = new AtomicReference<>();
        /**
         * Last search attempt that wasn't rejected outright.
         */
        private final AtomicReference<RequestAndListener<SearchRequest, SearchResponse>> lastSearch = new AtomicReference<>();
        /**
         * Last scroll attempt that wasn't rejected outright.
         */
        private final AtomicReference<RequestAndListener<SearchScrollRequest, SearchResponse>> lastScroll = new AtomicReference<>();
        /**
         * Set of all scrolls we've already used. Used to check that we don't reuse the same request twice.
         */
        private final Set<SearchScrollRequest> usedScolls = synchronizedSet(newSetFromMap(new IdentityHashMap<>()));

        private int bulksToReject = 0;
        private int searchesToReject = 0;
        private int scrollsToReject = 0;

        MyMockClient(Client in) {
            super(in);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> responseActionListener
        ) {
            if (false == expectedHeaders.equals(threadPool().getThreadContext().getHeaders())) {
                responseActionListener.onFailure(
                    new RuntimeException("Expected " + expectedHeaders + " but got " + threadPool().getThreadContext().getHeaders())
                );
                return;
            }

            if (request instanceof ClearScrollRequest) {
                assertEquals(TaskId.EMPTY_TASK_ID, request.getParentTask());
            } else {
                assertEquals(taskId, request.getParentTask());
            }
            if (request instanceof RefreshRequest) {
                lastRefreshRequest.set((RefreshRequest) request);
                responseActionListener.onResponse(null);
                return;
            }
            if (request instanceof SearchRequest) {
                if (searchAttempts.incrementAndGet() <= searchesToReject) {
                    responseActionListener.onFailure(wrappedRejectedException());
                    return;
                }
                lastSearch.set(new RequestAndListener<>((SearchRequest) request, (ActionListener<SearchResponse>) responseActionListener));
                return;
            }
            if (request instanceof SearchScrollRequest scroll) {
                boolean newRequest = usedScolls.add(scroll);
                assertTrue("We can't reuse scroll requests", newRequest);
                if (scrollAttempts.incrementAndGet() <= scrollsToReject) {
                    responseActionListener.onFailure(wrappedRejectedException());
                    return;
                }
                lastScroll.set(new RequestAndListener<>(scroll, (ActionListener<SearchResponse>) responseActionListener));
                return;
            }
            if (request instanceof ClearScrollRequest clearScroll) {
                scrollsCleared.addAll(clearScroll.getScrollIds());
                responseActionListener.onResponse((Response) new ClearScrollResponse(true, clearScroll.getScrollIds().size()));
                return;
            }
            if (request instanceof BulkRequest bulk) {
                int toReject;
                if (bulksAttempts.incrementAndGet() > bulksToReject) {
                    toReject = -1;
                } else {
                    toReject = randomIntBetween(0, bulk.requests().size() - 1);
                }
                BulkItemResponse[] responses = new BulkItemResponse[bulk.requests().size()];
                for (int i = 0; i < bulk.requests().size(); i++) {
                    DocWriteRequest<?> item = bulk.requests().get(i);
                    DocWriteResponse response;
                    ShardId shardId = new ShardId(new Index(item.index(), "uuid"), 0);
                    if (item instanceof IndexRequest index) {
                        response = new IndexResponse(
                            shardId,
                            index.id() == null ? "dummy_id" : index.id(),
                            randomInt(20),
                            randomIntBetween(1, 16),
                            randomIntBetween(0, Integer.MAX_VALUE),
                            true
                        );
                    } else if (item instanceof UpdateRequest update) {
                        response = new UpdateResponse(
                            shardId,
                            update.id(),
                            randomNonNegativeLong(),
                            randomIntBetween(1, Integer.MAX_VALUE),
                            randomIntBetween(0, Integer.MAX_VALUE),
                            Result.CREATED
                        );
                    } else if (item instanceof DeleteRequest delete) {
                        response = new DeleteResponse(
                            shardId,
                            delete.id(),
                            randomInt(20),
                            randomIntBetween(1, 16),
                            randomIntBetween(0, Integer.MAX_VALUE),
                            true
                        );
                    } else {
                        throw new RuntimeException("Unknown request:  " + item);
                    }
                    if (i == toReject) {
                        responses[i] = BulkItemResponse.failure(
                            i,
                            item.opType(),
                            new Failure(response.getIndex(), response.getId(), new EsRejectedExecutionException())
                        );
                    } else {
                        responses[i] = BulkItemResponse.success(i, item.opType(), response);
                    }
                }
                responseActionListener.onResponse((Response) new BulkResponse(responses, 1));
                return;
            }
            super.doExecute(action, request, responseActionListener);
        }

        private Exception wrappedRejectedException() {
            Exception e = new EsRejectedExecutionException();
            int wraps = randomIntBetween(0, 4);
            for (int i = 0; i < wraps; i++) {
                switch (randomIntBetween(0, 2)) {
                    case 0 -> {
                        e = new SearchPhaseExecutionException("test", "test failure", e, new ShardSearchFailure[0]);
                        continue;
                    }
                    case 1 -> {
                        e = new ReduceSearchPhaseException("test", "test failure", e, new ShardSearchFailure[0]);
                        continue;
                    }
                    case 2 -> {
                        e = new ElasticsearchException(e);
                        continue;
                    }
                }
            }
            return e;
        }
    }

    private static class RequestAndListener<Request extends ActionRequest, Response> {
        private final Request request;
        private final ActionListener<Response> listener;

        RequestAndListener(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }
    }

    /**
     * Assert that calling the consumer invokes the runnable exactly once.
     */
    private void assertExactlyOnce(CheckedConsumer<Runnable, Exception> consumer) throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        consumer.accept(() -> assertTrue(called.compareAndSet(false, true)));
        assertBusy(() -> assertTrue(called.get()));
    }
}
