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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.action.bulk.BackoffPolicy.constantBackoff;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AsyncBulkByScrollActionTests extends ESTestCase {
    private MyMockClient client;
    private ThreadPool threadPool;
    private DummyAbstractBulkByScrollRequest testRequest;
    private SearchRequest firstSearchRequest;
    private PlainActionFuture<BulkIndexByScrollResponse> listener;
    private String scrollId;
    private TaskManager taskManager;
    private BulkByScrollTask testTask;
    private Map<String, String> expectedHeaders = new HashMap<>();
    private DiscoveryNode localNode;
    private TaskId taskId;

    @Before
    public void setupForTest() {
        client = new MyMockClient(new NoOpClient(getTestName()));
        threadPool = new TestThreadPool(getTestName());
        firstSearchRequest = new SearchRequest();
        testRequest = new DummyAbstractBulkByScrollRequest(firstSearchRequest);
        listener = new PlainActionFuture<>();
        scrollId = null;
        taskManager = new TaskManager(Settings.EMPTY);
        testTask = (BulkByScrollTask) taskManager.register("don'tcare", "hereeither", testRequest);

        // Fill the context with something random so we can make sure we inherited it appropriately.
        expectedHeaders.clear();
        expectedHeaders.put(randomSimpleString(random()), randomSimpleString(random()));
        threadPool.getThreadContext().newStoredContext();
        threadPool.getThreadContext().putHeader(expectedHeaders);
        localNode = new DiscoveryNode("thenode", new LocalTransportAddress("dead.end:666"), emptyMap(), emptySet(), Version.CURRENT);
        taskId = new TaskId(localNode.getId(), testTask.getId());
    }

    @After
    public void tearDownAndVerifyCommonStuff() {
        client.close();
        threadPool.shutdown();
    }

    /**
     * Generates a random scrollId and registers it so that when the test
     * finishes we check that it was cleared. Subsequent calls reregister a new
     * random scroll id so it is checked instead.
     */
    private String scrollId() {
        scrollId = randomSimpleString(random(), 1, 1000); // Empty strings get special behavior we don't want
        return scrollId;
    }

    public void testStartRetriesOnRejectionAndSucceeds() throws Exception {
        client.searchesToReject = randomIntBetween(0, testRequest.getMaxRetries() - 1);
        DummyAbstractAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
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
        DummyAbstractAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.start();
        assertBusy(() -> assertEquals(testRequest.getMaxRetries() + 1, client.searchAttempts.get()));
        assertBusy(() -> assertTrue(listener.isDone()));
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(ExceptionsHelper.stackTrace(e), containsString(EsRejectedExecutionException.class.getSimpleName()));
        assertNull("There shouldn't be a search attempt pending that we didn't reject", client.lastSearch.get());
        assertEquals(testRequest.getMaxRetries(), testTask.getStatus().getSearchRetries());
    }

    public void testStartNextScrollRetriesOnRejectionAndSucceeds() throws Exception {
        client.scrollsToReject = randomIntBetween(0, testRequest.getMaxRetries() - 1);
        DummyAbstractAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.setScroll(scrollId());
        action.startNextScroll(timeValueNanos(System.nanoTime()), 0);
        assertBusy(() -> assertEquals(client.scrollsToReject + 1, client.scrollAttempts.get()));
        if (listener.isDone()) {
            Object result = listener.get();
            fail("Expected listener not to be done but it was and had " + result);
        }
        assertBusy(() -> assertNotNull("There should be a scroll attempt pending that we didn't reject", client.lastScroll.get()));
        assertEquals(client.scrollsToReject, testTask.getStatus().getSearchRetries());
    }

    public void testStartNextScrollRetriesOnRejectionButFailsOnTooManyRejections() throws Exception {
        client.scrollsToReject = testRequest.getMaxRetries() + randomIntBetween(1, 100);
        DummyAbstractAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.setScroll(scrollId());
        action.startNextScroll(timeValueNanos(System.nanoTime()), 0);
        assertBusy(() -> assertEquals(testRequest.getMaxRetries() + 1, client.scrollAttempts.get()));
        assertBusy(() -> assertTrue(listener.isDone()));
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(ExceptionsHelper.stackTrace(e), containsString(EsRejectedExecutionException.class.getSimpleName()));
        assertNull("There shouldn't be a scroll attempt pending that we didn't reject", client.lastScroll.get());
        assertEquals(testRequest.getMaxRetries(), testTask.getStatus().getSearchRetries());
    }

    public void testScrollResponseSetsTotal() {
        // Default is 0, meaning unstarted
        assertEquals(0, testTask.getStatus().getTotal());

        long total = randomIntBetween(0, Integer.MAX_VALUE);
        InternalSearchHits hits = new InternalSearchHits(null, total, 0);
        InternalSearchResponse searchResponse = new InternalSearchResponse(hits, null, null, null, false, false);
        new DummyAbstractAsyncBulkByScrollAction().onScrollResponse(timeValueSeconds(0), 0,
                new SearchResponse(searchResponse, scrollId(), 5, 4, randomLong(), null));
        assertEquals(total, testTask.getStatus().getTotal());
    }

    /**
     * Tests that each scroll response is a batch and that the batch is launched properly.
     */
    public void testScrollResponseBatchingBehavior() throws Exception {
        int maxBatches = randomIntBetween(0, 100);
        for (int batches = 1; batches < maxBatches; batches++) {
            InternalSearchHit hit = new InternalSearchHit(0, "id", new Text("type"), emptyMap());
            InternalSearchHits hits = new InternalSearchHits(new InternalSearchHit[] { hit }, 0, 0);
            InternalSearchResponse searchResponse = new InternalSearchResponse(hits, null, null, null, false, false);
            DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
            action.onScrollResponse(timeValueNanos(System.nanoTime()), 0,
                    new SearchResponse(searchResponse, scrollId(), 5, 4, randomLong(), null));

            // Use assert busy because the update happens on another thread
            final int expectedBatches = batches;
            assertBusy(() -> assertEquals(expectedBatches, testTask.getStatus().getBatches()));

            /*
             * Also while we're here check that we preserved the headers from the last request. assertBusy because no requests might have
             * come in yet.
             */
            assertBusy(() -> assertEquals(expectedHeaders, client.lastHeaders.get()));
        }
    }

    public void testBulkResponseSetsLotsOfStatus() {
        testRequest.setAbortOnVersionConflict(false);
        int maxBatches = randomIntBetween(0, 100);
        long versionConflicts = 0;
        long created = 0;
        long updated = 0;
        long deleted = 0;
        for (int batches = 0; batches < maxBatches; batches++) {
            BulkItemResponse[] responses = new BulkItemResponse[randomIntBetween(0, 100)];
            for (int i = 0; i < responses.length; i++) {
                ShardId shardId = new ShardId(new Index("name", "uid"), 0);
                String opType;
                if (rarely()) {
                    opType = randomSimpleString(random());
                    versionConflicts++;
                    responses[i] = new BulkItemResponse(i, opType, new Failure(shardId.getIndexName(), "type", "id" + i,
                            new VersionConflictEngineException(shardId, "type", "id", "test")));
                    continue;
                }
                boolean createdResponse;
                switch (randomIntBetween(0, 2)) {
                case 0:
                    opType = randomFrom("index", "create");
                    createdResponse = true;
                    created++;
                    break;
                case 1:
                    opType = randomFrom("index", "create");
                    createdResponse = false;
                    updated++;
                    break;
                case 2:
                    opType = "delete";
                    createdResponse = false;
                    deleted++;
                    break;
                default:
                    throw new RuntimeException("Bad scenario");
                }
                responses[i] = new BulkItemResponse(i, opType, new IndexResponse(shardId, "type", "id" + i, randomInt(), createdResponse));
            }
            new DummyAbstractAsyncBulkByScrollAction().onBulkResponse(timeValueNanos(System.nanoTime()), new BulkResponse(responses, 0));
            assertEquals(versionConflicts, testTask.getStatus().getVersionConflicts());
            assertEquals(updated, testTask.getStatus().getUpdated());
            assertEquals(created, testTask.getStatus().getCreated());
            assertEquals(deleted, testTask.getStatus().getDeleted());
            assertEquals(versionConflicts, testTask.getStatus().getVersionConflicts());
        }
    }

    /**
     * Mimicks a ThreadPool rejecting execution of the task.
     */
    public void testThreadPoolRejectionsAbortRequest() throws Exception {
        testTask.rethrottle(1);
        threadPool.shutdown();
        threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ScheduledFuture<?> schedule(TimeValue delay, String name, Runnable command) {
                // While we're here we can check that the sleep made it through
                assertThat(delay.nanos(), greaterThan(0L));
                assertThat(delay.seconds(), lessThanOrEqualTo(10L));
                ((AbstractRunnable) command).onRejection(new EsRejectedExecutionException("test"));
                return null;
            }
        };
        InternalSearchHits hits = new InternalSearchHits(null, 0, 0);
        InternalSearchResponse searchResponse = new InternalSearchResponse(hits, null, null, null, false, false);
        new DummyAbstractAsyncBulkByScrollAction().onScrollResponse(timeValueNanos(System.nanoTime()), 10,
                new SearchResponse(searchResponse, scrollId(), 5, 4, randomLong(), null));
        try {
            listener.get();
            fail("Expected a failure");
        } catch (ExecutionException e) {
            assertThat(e.getMessage(), equalTo("EsRejectedExecutionException[test]"));
        }
        assertThat(client.scrollsCleared, contains(scrollId));

        // When the task is rejected we don't increment the throttled timer
        assertEquals(timeValueMillis(0), testTask.getStatus().getThrottled());
    }

    /**
     * Mimicks shard search failures usually caused by the data node serving the
     * scroll request going down.
     */
    public void testShardFailuresAbortRequest() throws Exception {
        ShardSearchFailure shardFailure = new ShardSearchFailure(new RuntimeException("test"));
        InternalSearchResponse internalResponse = new InternalSearchResponse(null, null, null, null, false, null);
        new DummyAbstractAsyncBulkByScrollAction().onScrollResponse(timeValueNanos(System.nanoTime()), 0,
                new SearchResponse(internalResponse, scrollId(), 5, 4, randomLong(), new ShardSearchFailure[] { shardFailure }));
        BulkIndexByScrollResponse response = listener.get();
        assertThat(response.getIndexingFailures(), emptyCollectionOf(Failure.class));
        assertThat(response.getSearchFailures(), contains(shardFailure));
        assertFalse(response.isTimedOut());
        assertNull(response.getReasonCancelled());
        assertThat(client.scrollsCleared, contains(scrollId));
    }

    /**
     * Mimicks search timeouts.
     */
    public void testSearchTimeoutsAbortRequest() throws Exception {
        InternalSearchResponse internalResponse = new InternalSearchResponse(null, null, null, null, true, null);
        new DummyAbstractAsyncBulkByScrollAction().onScrollResponse(timeValueNanos(System.nanoTime()), 0,
                new SearchResponse(internalResponse, scrollId(), 5, 4, randomLong(), new ShardSearchFailure[0]));
        BulkIndexByScrollResponse response = listener.get();
        assertThat(response.getIndexingFailures(), emptyCollectionOf(Failure.class));
        assertThat(response.getSearchFailures(), emptyCollectionOf(ShardSearchFailure.class));
        assertTrue(response.isTimedOut());
        assertNull(response.getReasonCancelled());
        assertThat(client.scrollsCleared, contains(scrollId));
    }

    /**
     * Mimicks bulk indexing failures.
     */
    public void testBulkFailuresAbortRequest() throws Exception {
        Failure failure = new Failure("index", "type", "id", new RuntimeException("test"));
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        BulkResponse bulkResponse = new BulkResponse(new BulkItemResponse[] {new BulkItemResponse(0, "index", failure)}, randomLong());
        action.onBulkResponse(timeValueNanos(System.nanoTime()), bulkResponse);
        BulkIndexByScrollResponse response = listener.get();
        assertThat(response.getIndexingFailures(), contains(failure));
        assertThat(response.getSearchFailures(), emptyCollectionOf(ShardSearchFailure.class));
        assertNull(response.getReasonCancelled());
    }

    /**
     * Mimicks script failures or general wrongness by implementers.
     */
    public void testListenerReceiveBuildBulkExceptions() throws Exception {
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction() {
            @Override
            protected BulkRequest buildBulk(Iterable<SearchHit> docs) {
                throw new RuntimeException("surprise");
            }
        };
        InternalSearchHit hit = new InternalSearchHit(0, "id", new Text("type"), emptyMap());
        InternalSearchHits hits = new InternalSearchHits(new InternalSearchHit[] {hit}, 0, 0);
        InternalSearchResponse internalResponse = new InternalSearchResponse(hits, null, null, null, false, false);
        SearchResponse searchResponse = new SearchResponse(internalResponse, scrollId(), 5, 4, randomLong(), null);
        action.onScrollResponse(timeValueNanos(System.nanoTime()), 0, searchResponse);
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
        threadPool.shutdown();
        threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ScheduledFuture<?> schedule(TimeValue delay, String name, Runnable command) {
                capturedDelay.set(delay);
                capturedCommand.set(command);
                return null;
            }
        };

        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        action.setScroll(scrollId());

        // Set the base for the scroll to wait - this is added to the figure we calculate below
        firstSearchRequest.scroll(timeValueSeconds(10));

        // Set throttle to 1 request per second to make the math simpler
        testTask.rethrottle(1f);
        // Make the last batch look nearly instant but have 100 documents
        action.startNextScroll(timeValueNanos(System.nanoTime()), 100);

        // So the next request is going to have to wait an extra 100 seconds or so (base was 10 seconds, so 110ish)
        assertThat(client.lastScroll.get().request.scroll().keepAlive().seconds(), either(equalTo(110L)).or(equalTo(109L)));

        // Now we can simulate a response and check the delay that we used for the task
        InternalSearchHit hit = new InternalSearchHit(0, "id", new Text("type"), emptyMap());
        InternalSearchHits hits = new InternalSearchHits(new InternalSearchHit[] { hit }, 0, 0);
        InternalSearchResponse internalResponse = new InternalSearchResponse(hits, null, null, null, false, false);
        SearchResponse searchResponse = new SearchResponse(internalResponse, scrollId(), 5, 4, randomLong(), null);

        if (randomBoolean()) {
            client.lastScroll.get().listener.onResponse(searchResponse);
            // The delay is still 100ish seconds because there hasn't been much time between when we requested the bulk and when we got it.
            assertThat(capturedDelay.get().seconds(), either(equalTo(100L)).or(equalTo(99L)));
        } else {
            // Let's rethrottle between the starting the scroll and getting the response
            testTask.rethrottle(10f);
            client.lastScroll.get().listener.onResponse(searchResponse);
            // The delay uses the new throttle
            assertThat(capturedDelay.get().seconds(), either(equalTo(10L)).or(equalTo(9L)));
        }

        // Running the command ought to increment the delay counter on the task.
        capturedCommand.get().run();
        assertEquals(capturedDelay.get(), testTask.getStatus().getThrottled());
    }

    /**
     * Execute a bulk retry test case. The total number of failures is random and the number of retries attempted is set to
     * testRequest.getMaxRetries and controled by the failWithRejection parameter.
     */
    private void bulkRetryTestCase(boolean failWithRejection) throws Exception {
        int totalFailures = randomIntBetween(1, testRequest.getMaxRetries());
        int size = randomIntBetween(1, 100);
        testRequest.setMaxRetries(totalFailures - (failWithRejection ? 1 : 0));

        client.bulksToReject = client.bulksAttempts.get() + totalFailures;
        /*
         * When we get a successful bulk response we usually start the next scroll request but lets just intercept that so we don't have to
         * deal with it. We just wait for it to happen.
         */
        CountDownLatch successLatch = new CountDownLatch(1);
        DummyAbstractAsyncBulkByScrollAction action = new DummyActionWithoutBackoff() {
            @Override
            void startNextScroll(TimeValue lastBatchStartTime, int lastBatchSize) {
                successLatch.countDown();
            }
        };
        BulkRequest request = new BulkRequest();
        for (int i = 0; i < size + 1; i++) {
            request.add(new IndexRequest("index", "type", "id" + i));
        }
        action.sendBulkRequest(timeValueNanos(System.nanoTime()), request);
        if (failWithRejection) {
            BulkIndexByScrollResponse response = listener.get();
            assertThat(response.getIndexingFailures(), hasSize(1));
            assertEquals(response.getIndexingFailures().get(0).getStatus(), RestStatus.TOO_MANY_REQUESTS);
            assertThat(response.getSearchFailures(), emptyCollectionOf(ShardSearchFailure.class));
            assertNull(response.getReasonCancelled());
        } else {
            successLatch.await(10, TimeUnit.SECONDS);
        }
    }

    /**
     * The default retry time matches what we say it is in the javadoc for the request.
     */
    public void testDefaultRetryTimes() {
        Iterator<TimeValue> policy = new DummyAbstractAsyncBulkByScrollAction().buildBackoffPolicy().iterator();
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
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        if (addDestinationIndexes) {
            action.addDestinationIndices(singleton("foo"));
        }
        action.startNormalTermination(emptyList(), emptyList(), false);
        if (shouldRefresh) {
            assertArrayEquals(new String[] {"foo"}, client.lastRefreshRequest.get().indices());
        } else {
            assertNull("No refresh was attempted", client.lastRefreshRequest.get());
        }
    }

    public void testCancelBeforeInitialSearch() throws Exception {
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) -> action.start());
    }

    public void testCancelBeforeScrollResponse() throws Exception {
        // We bail so early we don't need to pass in a half way valid response.
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) -> action.onScrollResponse(timeValueNanos(System.nanoTime()), 1,
                null));
    }

    public void testCancelBeforeSendBulkRequest() throws Exception {
        // We bail so early we don't need to pass in a half way valid request.
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) -> action.sendBulkRequest(timeValueNanos(System.nanoTime()), null));
    }

    public void testCancelBeforeOnBulkResponse() throws Exception {
        // We bail so early we don't need to pass in a half way valid response.
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) ->
                action.onBulkResponse(timeValueNanos(System.nanoTime()), new BulkResponse(new BulkItemResponse[0], 0)));
    }

    public void testCancelBeforeStartNextScroll() throws Exception {
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) -> action.startNextScroll(timeValueNanos(System.nanoTime()), 0));
    }

    public void testCancelBeforeStartNormalTermination() throws Exception {
        // Refresh or not doesn't matter - we don't try to refresh.
        testRequest.setRefresh(usually());
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) -> action.startNormalTermination(emptyList(), emptyList(), false));
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
        threadPool.shutdown();
        threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ScheduledFuture<?> schedule(TimeValue delay, String name, Runnable command) {
                /*
                 * This is called twice:
                 * 1. To schedule the throttling. When that happens we immediately cancel the task.
                 * 2. After the task is canceled.
                 * Both times we use delegate to the standard behavior so the task is scheduled as expected so it can be cancelled and all
                 * that good stuff.
                 */
                if (delay.nanos() > 0) {
                    generic().execute(() -> taskManager.cancel(testTask, reason, (Set<String> s) -> {}));
                }
                return super.schedule(delay, name, command);
            }
        };

        // Send the scroll response which will trigger the custom thread pool above, canceling the request before running the response
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        boolean previousScrollSet = usually();
        if (previousScrollSet) {
            action.setScroll(scrollId());
        }
        long total = randomIntBetween(0, Integer.MAX_VALUE);
        InternalSearchHits hits = new InternalSearchHits(null, total, 0);
        InternalSearchResponse searchResponse = new InternalSearchResponse(hits, null, null, null, false, false);
        // Use a long delay here so the test will time out if the cancellation doesn't reschedule the throttled task
        SearchResponse scrollResponse = new SearchResponse(searchResponse, scrollId(), 5, 4, randomLong(), null);
        testTask.rethrottle(1);
        action.onScrollResponse(timeValueNanos(System.nanoTime()), 1000, scrollResponse);

        // Now that we've got our cancel we'll just verify that it all came through all right
        assertEquals(reason, listener.get(10, TimeUnit.SECONDS).getReasonCancelled());
        if (previousScrollSet) {
            // Canceled tasks always start to clear the scroll before they die.
            assertThat(client.scrollsCleared, contains(scrollId));
        }
    }

    private void cancelTaskCase(Consumer<DummyAbstractAsyncBulkByScrollAction> testMe) throws Exception {
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        boolean previousScrollSet = usually();
        if (previousScrollSet) {
            action.setScroll(scrollId());
        }
        String reason = randomSimpleString(random());
        taskManager.cancel(testTask, reason, (Set<String> s) -> {});
        testMe.accept(action);
        assertEquals(reason, listener.get().getReasonCancelled());
        if (previousScrollSet) {
            // Canceled tasks always start to clear the scroll before they die.
            assertThat(client.scrollsCleared, contains(scrollId));
        }
    }

    private class DummyAbstractAsyncBulkByScrollAction
            extends AbstractAsyncBulkByScrollAction<DummyAbstractBulkByScrollRequest> {
        public DummyAbstractAsyncBulkByScrollAction() {
            super(testTask, logger, new ParentTaskAssigningClient(client, localNode, testTask), threadPool, testRequest, firstSearchRequest,
                    listener);
        }

        @Override
        protected BulkRequest buildBulk(Iterable<SearchHit> docs) {
            return new BulkRequest();
        }

        @Override
        protected BulkIndexByScrollResponse buildResponse(TimeValue took, List<Failure> indexingFailures,
                List<ShardSearchFailure> searchFailures, boolean timedOut) {
            return new BulkIndexByScrollResponse(took, task.getStatus(), indexingFailures, searchFailures, timedOut);
        }
    }

    /**
     * An extension to {@linkplain DummyAbstractAsyncBulkByScrollAction} that uses a 0 delaying backoff policy.
     */
    private class DummyActionWithoutBackoff extends DummyAbstractAsyncBulkByScrollAction {
        @Override
        BackoffPolicy buildBackoffPolicy() {
            // Force a backoff time of 0 to prevent sleeping
            return constantBackoff(timeValueMillis(0), testRequest.getMaxRetries());
        }
    }

    private static class DummyAbstractBulkByScrollRequest extends AbstractBulkByScrollRequest<DummyAbstractBulkByScrollRequest> {
        public DummyAbstractBulkByScrollRequest(SearchRequest searchRequest) {
            super(searchRequest);
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
        private final AtomicReference<Map<String, String>> lastHeaders = new AtomicReference<>();
        private final AtomicReference<RefreshRequest> lastRefreshRequest = new AtomicReference<>();
        /**
         * Last search attempt that wasn't rejected outright.
         */
        private final AtomicReference<RequestAndListener<SearchRequest, SearchResponse>> lastSearch = new AtomicReference<>();
        /**
         * Last scroll attempt that wasn't rejected outright.
         */
        private final AtomicReference<RequestAndListener<SearchScrollRequest, SearchResponse>> lastScroll = new AtomicReference<>();


        private int bulksToReject = 0;
        private int searchesToReject = 0;
        private int scrollsToReject = 0;

        public MyMockClient(Client in) {
            super(in);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest<Request>, Response extends ActionResponse,
                RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
                Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            lastHeaders.set(threadPool.getThreadContext().getHeaders());
            if (request instanceof ClearScrollRequest) {
                assertEquals(TaskId.EMPTY_TASK_ID, request.getParentTask());
            } else {
                assertEquals(taskId, request.getParentTask());
            }
            if (request instanceof RefreshRequest) {
                lastRefreshRequest.set((RefreshRequest) request);
                listener.onResponse(null);
                return;
            }
            if (request instanceof SearchRequest) {
                if (searchAttempts.incrementAndGet() <= searchesToReject) {
                    listener.onFailure(wrappedRejectedException());
                    return;
                }
                lastSearch.set(new RequestAndListener<>((SearchRequest) request, (ActionListener<SearchResponse>) listener));
                return;
            }
            if (request instanceof SearchScrollRequest) {
                if (scrollAttempts.incrementAndGet() <= scrollsToReject) {
                    listener.onFailure(wrappedRejectedException());
                    return;
                }
                lastScroll.set(new RequestAndListener<>((SearchScrollRequest) request, (ActionListener<SearchResponse>) listener));
                return;
            }
            if (request instanceof ClearScrollRequest) {
                ClearScrollRequest clearScroll = (ClearScrollRequest) request;
                scrollsCleared.addAll(clearScroll.getScrollIds());
                listener.onResponse((Response) new ClearScrollResponse(true, clearScroll.getScrollIds().size()));
                return;
            }
            if (request instanceof BulkRequest) {
                BulkRequest bulk = (BulkRequest) request;
                int toReject;
                if (bulksAttempts.incrementAndGet() > bulksToReject) {
                    toReject = -1;
                } else {
                    toReject = randomIntBetween(0, bulk.requests().size() - 1);
                }
                BulkItemResponse[] responses = new BulkItemResponse[bulk.requests().size()];
                for (int i = 0; i < bulk.requests().size(); i++) {
                    ActionRequest<?> item = bulk.requests().get(i);
                    String opType;
                    DocWriteResponse response;
                    ShardId shardId = new ShardId(new Index(((ReplicationRequest<?>) item).index(), "uuid"), 0);
                    if (item instanceof IndexRequest) {
                        IndexRequest index = (IndexRequest) item;
                        opType = index.opType().lowercase();
                        response = new IndexResponse(shardId, index.type(), index.id(), randomIntBetween(0, Integer.MAX_VALUE),
                                true);
                    } else if (item instanceof UpdateRequest) {
                        UpdateRequest update = (UpdateRequest) item;
                        opType = "update";
                        response = new UpdateResponse(shardId, update.type(), update.id(),
                                randomIntBetween(0, Integer.MAX_VALUE), true);
                    } else if (item instanceof DeleteRequest) {
                        DeleteRequest delete = (DeleteRequest) item;
                        opType = "delete";
                        response = new DeleteResponse(shardId, delete.type(), delete.id(), randomIntBetween(0, Integer.MAX_VALUE),
                                true);
                    } else {
                        throw new RuntimeException("Unknown request:  " + item);
                    }
                    if (i == toReject) {
                        responses[i] = new BulkItemResponse(i, opType,
                                new Failure(response.getIndex(), response.getType(), response.getId(), new EsRejectedExecutionException()));
                    } else {
                        responses[i] = new BulkItemResponse(i, opType, response);
                    }
                }
                listener.onResponse((Response) new BulkResponse(responses, 1));
                return;
            }
            super.doExecute(action, request, listener);
        }

        private Throwable wrappedRejectedException() {
            Exception e = new EsRejectedExecutionException();
            int wraps = randomIntBetween(0, 4);
            for (int i = 0; i < wraps; i++) {
                switch (randomIntBetween(0, 2)) {
                case 0:
                    e = new SearchPhaseExecutionException("test", "test failure", e, new ShardSearchFailure[0]);
                    continue;
                case 1:
                    e = new ReduceSearchPhaseException("test", "test failure", e, new ShardSearchFailure[0]);
                    continue;
                case 2:
                    e = new ElasticsearchException(e);
                    continue;
                }
            }
            return e;
        }
    }

    private static class RequestAndListener<Request extends ActionRequest<Request>, Response> {
        private final Request request;
        private final ActionListener<Response> listener;

        public RequestAndListener(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }
    }
}
