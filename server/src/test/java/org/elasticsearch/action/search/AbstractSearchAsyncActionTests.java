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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class AbstractSearchAsyncActionTests extends ESTestCase {

    private final List<Tuple<String, String>> resolvedNodes = new ArrayList<>();
    private final Set<Long> releasedContexts = new CopyOnWriteArraySet<>();

    private AbstractSearchAsyncAction<SearchPhaseResult> createAction(SearchRequest request,
                                                                      MainSearchTask mainSearchTask,
                                                                      ArraySearchPhaseResults<SearchPhaseResult> results,
                                                                      ActionListener<SearchResponse> listener,
                                                                      final boolean controlled,
                                                                      final AtomicLong expected,
                                                                      final BiConsumer<SearchActionListener<SearchPhaseResult>,
                                                                          ShardId> shardOp,
                                                                      Executor executor,
                                                                      final SearchPhase nextPhase) {
        final Runnable runnable;
        final TransportSearchAction.SearchTimeProvider timeProvider;
        if (controlled) {
            runnable = () -> expected.set(randomNonNegativeLong());
            timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, expected::get);
        } else {
            runnable = () -> {
                long elapsed = spinForAtLeastNMilliseconds(randomIntBetween(1, 10));
                expected.set(elapsed);
            };
            timeProvider = new TransportSearchAction.SearchTimeProvider(
                    0,
                    System.nanoTime(),
                    System::nanoTime);
        }

        BiFunction<String, String, Transport.Connection> nodeIdToConnection = (cluster, node) -> {
            resolvedNodes.add(Tuple.tuple(cluster, node));
            return null;
        };

        ShardId shardId1 = new ShardId("index", "uuid", 0);
        ShardRouting shardRouting1 = TestShardRouting.newShardRouting(shardId1, "node", true, ShardRoutingState.STARTED);
        ShardRouting shardRouting1Replica = TestShardRouting.newShardRouting(shardId1, "another_node", false, ShardRoutingState.STARTED);
        SearchShardIterator iterator1 = new SearchShardIterator(null, shardId1, Arrays.asList(shardRouting1, shardRouting1Replica), null);
        ShardId shardId2 = new ShardId("index", "uuid", 1);
        ShardRouting shardRouting2 = TestShardRouting.newShardRouting(shardId2, "node", true, ShardRoutingState.STARTED);
        ShardRouting shardRouting2Replica = TestShardRouting.newShardRouting(shardId2, "another_node", false, ShardRoutingState.STARTED);
        SearchShardIterator iterator2 = new SearchShardIterator(null, shardId2, Arrays.asList(shardRouting2, shardRouting2Replica), null);
        return new AbstractSearchAsyncAction<SearchPhaseResult>("test", logger, null, nodeIdToConnection,
                Collections.singletonMap("foo", new AliasFilter(new MatchAllQueryBuilder())), Collections.singletonMap("foo", 2.0f),
                Collections.singletonMap("name", Sets.newHashSet("bar", "baz")), executor, request, listener,
                new GroupShardsIterator<>(Arrays.asList(iterator1, iterator2)),
                timeProvider, 0, mainSearchTask,
                results, request.getMaxConcurrentShardRequests(),
                SearchResponse.Clusters.EMPTY) {
            @Override
            protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, final SearchPhaseContext context) {
                return nextPhase;
            }

            @Override
            protected void executePhaseOnShard(final SearchShardIterator shardIt, final ShardRouting shard,
                                               final SearchActionListener<SearchPhaseResult> listener) {
                shardOp.accept(listener, shard.shardId());
            }

            @Override
            long buildTookInMillis() {
                runnable.run();
                return super.buildTookInMillis();
            }

            @Override
            public void sendReleaseSearchContext(long contextId, Transport.Connection connection, OriginalIndices originalIndices) {
                releasedContexts.add(contextId);
            }
        };
    }

    public void testTookWithControlledClock() {
        runTestTook(true);
    }

    public void testTookWithRealClock() {
        runTestTook(false);
    }

    private void runTestTook(final boolean controlled) {
        final AtomicLong expected = new AtomicLong();
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(new SearchRequest(),
            new MainSearchTask(0, "n/a", "n/a", ()-> "test", null, Collections.emptyMap()),
            new ArraySearchPhaseResults<>(10), null, controlled, expected, (listener, shard) -> {}, null, null);
        final long actual = action.buildTookInMillis();
        if (controlled) {
            // with a controlled clock, we can assert the exact took time
            assertThat(actual, equalTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        } else {
            // with a real clock, the best we can say is that it took as long as we spun for
            assertThat(actual, greaterThanOrEqualTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        }
    }

    public void testBuildShardSearchTransportRequest() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean()).preference("_shards:1,3");
        final AtomicLong expected = new AtomicLong();
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest,
            new MainSearchTask(0, "n/a", "n/a", ()-> "test", null, Collections.emptyMap()),
            new ArraySearchPhaseResults<>(10), null, false, expected, (listener, shard) -> {}, null, null);
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        SearchShardIterator iterator = new SearchShardIterator(clusterAlias, new ShardId(new Index("name", "foo"), 1),
            Collections.emptyList(), new OriginalIndices(new String[] {"name", "name1"}, IndicesOptions.strictExpand()));
        ShardSearchRequest shardSearchTransportRequest = action.buildShardSearchRequest(iterator);
        assertEquals(IndicesOptions.strictExpand(), shardSearchTransportRequest.indicesOptions());
        assertArrayEquals(new String[] {"name", "name1"}, shardSearchTransportRequest.indices());
        assertEquals(new MatchAllQueryBuilder(), shardSearchTransportRequest.getAliasFilter().getQueryBuilder());
        assertEquals(2.0f, shardSearchTransportRequest.indexBoost(), 0.0f);
        assertArrayEquals(new String[] {"name", "name1"}, shardSearchTransportRequest.indices());
        assertArrayEquals(new String[] {"bar", "baz"}, shardSearchTransportRequest.indexRoutings());
        assertEquals("_shards:1,3", shardSearchTransportRequest.preference());
        assertEquals(clusterAlias, shardSearchTransportRequest.getClusterAlias());
    }

    public void testBuildSearchResponse() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean());
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest,
            new MainSearchTask(0, "n/a", "n/a", ()-> "test", null, Collections.emptyMap()),
            new ArraySearchPhaseResults<>(10), null, false, new AtomicLong(), (listener, shard) -> {}, null, null);
        String scrollId = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, scrollId);
        assertEquals(scrollId, searchResponse.getScrollId());
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
    }

    public void testBuildSearchResponseAllowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest,
            new MainSearchTask(0, "n/a", "n/a", ()-> "test", null, Collections.emptyMap()),
            new ArraySearchPhaseResults<>(10), null, false, new AtomicLong(), (listener, shard) -> {}, null, null);
        action.onShardFailure(0, new SearchShardTarget("node", new ShardId("index", "index-uuid", 0), null, OriginalIndices.NONE),
            new IllegalArgumentException());
        String scrollId = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, scrollId);
        assertEquals(scrollId, searchResponse.getScrollId());
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
    }

    public void testBuildSearchResponseDisallowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        Set<Long> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        int numFailures = randomIntBetween(1, 5);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, numFailures);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest,
            new MainSearchTask(0, "n/a", "n/a", ()-> "test", null, Collections.emptyMap()), phaseResults, listener, false, new AtomicLong(),
            (searchActionListener, shard) -> {}, null, null);
        for (int i = 0; i < numFailures; i++) {
            ShardId failureShardId = new ShardId("index", "index-uuid", i);
            String failureClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String failureNodeId = randomAlphaOfLengthBetween(5, 10);
            action.onShardFailure(i, new SearchShardTarget(failureNodeId, failureShardId, failureClusterAlias, OriginalIndices.NONE),
                new IllegalArgumentException());
        }
        action.buildSearchResponse(InternalSearchResponse.empty(), randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10));
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException)exception.get();
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
        assertEquals(numFailures, searchPhaseExecutionException.shardFailures().length);
        for (ShardSearchFailure shardSearchFailure : searchPhaseExecutionException.shardFailures()) {
            assertThat(shardSearchFailure.getCause(), instanceOf(IllegalArgumentException.class));
        }
        assertEquals(nodeLookups, resolvedNodes);
        assertEquals(requestIds, releasedContexts);
    }

    public void testOnPhaseFailure() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        Set<Long> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, 0);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest,
            new MainSearchTask(0, "n/a", "n/a", ()-> "test", null, Collections.emptyMap()), phaseResults, listener, false, new AtomicLong(),
            (searchActionListener, shard) -> {}, null, null);
        action.getTask().getStatus().phaseStarted("test", -1);
        action.onPhaseFailure(new SearchPhase("test") {
            @Override
            public void run() {

            }
        }, "message", null);
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException)exception.get();
        assertEquals("message", searchPhaseExecutionException.getMessage());
        assertEquals("test", searchPhaseExecutionException.getPhaseName());
        assertEquals(0, searchPhaseExecutionException.shardFailures().length);
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
        assertEquals(nodeLookups, resolvedNodes);
        assertEquals(requestIds, releasedContexts);

        MainSearchTaskStatus status = action.getTask().getStatus();
        assertNull(status.getCurrentPhase());
        List<MainSearchTaskStatus.PhaseInfo> completedPhases = status.getCompletedPhases();
        assertEquals(1, completedPhases.size());
        MainSearchTaskStatus.PhaseInfo phaseInfo = completedPhases.get(0);
        assertEquals("test", phaseInfo.getName());
        assertEquals(-1, phaseInfo.getExpectedOps());
        assertEquals(0, phaseInfo.getProcessedShards().size());
        assertThat(phaseInfo.getFailure(), instanceOf(SearchPhaseExecutionException.class));
        assertEquals("message", phaseInfo.getFailure().getMessage());
    }

    public void testShardNotAvailableWithDisallowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        int numShards = randomIntBetween(2, 10);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults =
            new ArraySearchPhaseResults<>(numShards);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest,
            new MainSearchTask(0, "n/a", "n/a", ()-> "test", null, Collections.emptyMap()), phaseResults, listener, false, new AtomicLong(),
            (searchActionListener, shard) -> {}, null, null);
        // skip one to avoid the "all shards failed" failure.
        SearchShardIterator skipIterator = new SearchShardIterator(null, null, Collections.emptyList(), null);
        skipIterator.resetAndSkip();
        action.skipShard(skipIterator);
        action.getTask().getStatus().phaseStarted("test", -1);
        // expect at least 2 shards, so onPhaseDone should report failure.
        action.onPhaseDone();
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException)exception.get();
        assertEquals("Partial shards failure (" + (numShards - 1) + " shards unavailable)",
            searchPhaseExecutionException.getMessage());
        assertEquals("test", searchPhaseExecutionException.getPhaseName());
        assertEquals(0, searchPhaseExecutionException.shardFailures().length);
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);

        MainSearchTaskStatus status = action.getTask().getStatus();
        assertNull(status.getCurrentPhase());
        List<MainSearchTaskStatus.PhaseInfo> completedPhases = status.getCompletedPhases();
        assertEquals(1, completedPhases.size());
        MainSearchTaskStatus.PhaseInfo phaseInfo = completedPhases.get(0);
        assertEquals("test", phaseInfo.getName());
        assertThat(phaseInfo.getFailure(), instanceOf(SearchPhaseExecutionException.class));
        assertEquals(-1, phaseInfo.getExpectedOps());
        assertEquals(0, phaseInfo.getProcessedShards().size());
    }

    public void testPartialResultsProgressReporting() throws Exception {
        ShardNotFoundException exception = new ShardNotFoundException(new ShardId("", "", 0));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {

            AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
                searchRequest,
                new MainSearchTask(0, "n/a", "n/a", ()-> "test", null, Collections.emptyMap()),
                new ArraySearchPhaseResults<>(2),
                ActionListener.wrap(r -> {}, e -> {}),
                false,
                new AtomicLong(),
                (searchActionListener, shard) -> {
                    if (shard.id() == 0) {
                        searchActionListener.onFailure(exception);
                    } else {
                        SearchPhaseResult searchPhaseResult = new SearchPhaseResult() {};
                        searchPhaseResult.setTaskInfo(new TaskInfo(new TaskId("node_id", 1), "type", "action",
                            null, null, -1, -1, true, null, Collections.emptyMap()));
                        searchActionListener.onResponse(searchPhaseResult);
                    }
                },
                executorService,
                new SearchPhase("next") {
                    @Override
                    public void run() {

                    }
                });

            action.start();

            assertBusy(() -> {
                MainSearchTaskStatus status = action.getTask().getStatus();
                assertNull(status.getCurrentPhase());
                List<MainSearchTaskStatus.PhaseInfo> completedPhases = status.getCompletedPhases();
                assertEquals(1, completedPhases.size());
                MainSearchTaskStatus.PhaseInfo phaseInfo = completedPhases.get(0);
                assertNull(phaseInfo.getFailure());
                assertEquals("test", phaseInfo.getName());
                assertEquals(2, phaseInfo.getExpectedOps());
                assertEquals(2, phaseInfo.getProcessedShards().size());
                for (MainSearchTaskStatus.ShardInfo shardInfo : phaseInfo.getProcessedShards()) {
                    assertEquals("index", shardInfo.getSearchShardTarget().getShardId().getIndexName());
                    assertThat(shardInfo.getSearchShardTarget().getNodeId(), endsWith("node"));
                    if (shardInfo.getSearchShardTarget().getShardId().id() == 0) {
                        assertSame(exception, shardInfo.getFailure());
                        assertNull(shardInfo.getTaskInfo());
                    } else {
                        assertEquals(1, shardInfo.getSearchShardTarget().getShardId().id());
                        TaskInfo taskInfo = shardInfo.getTaskInfo();
                        assertEquals(1, taskInfo.getTaskId().getId());
                        assertEquals("node_id", taskInfo.getTaskId().getNodeId());
                        assertEquals("action", taskInfo.getAction());
                    }
                }
            });
        } finally {
            ThreadPool.terminate(executorService, 5, TimeUnit.SECONDS);
        }
    }

    public void testPartialShardFailureProgressReporting() throws Exception {
        ShardNotFoundException exception = new ShardNotFoundException(new ShardId("", "", 0));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(false);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {

            AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
                searchRequest,
                new MainSearchTask(0, "n/a", "n/a", ()-> "test", null, Collections.emptyMap()),
                new ArraySearchPhaseResults<>(2),
                ActionListener.wrap(r -> {}, e -> {}),
                false,
                new AtomicLong(),
                (searchActionListener, shard) -> {
                    if (shard.id() == 0) {
                        searchActionListener.onFailure(exception);
                    } else {
                        SearchPhaseResult searchPhaseResult = new SearchPhaseResult() {};
                        searchPhaseResult.setTaskInfo(new TaskInfo(new TaskId("node_id", 1), "type", "action",
                            null, null, -1, -1, true, null, Collections.emptyMap()));
                        searchActionListener.onResponse(searchPhaseResult);
                    }
                },
                executorService,
                new SearchPhase("next") {
                    @Override
                    public void run() {

                    }
                });

            action.start();

            assertBusy(() -> {
                MainSearchTaskStatus status = action.getTask().getStatus();
                assertNull(status.getCurrentPhase());
                List<MainSearchTaskStatus.PhaseInfo> completedPhases = status.getCompletedPhases();
                assertEquals(1, completedPhases.size());
                MainSearchTaskStatus.PhaseInfo phaseInfo = completedPhases.get(0);
                assertEquals("test", phaseInfo.getName());
                assertThat(phaseInfo.getFailure(), instanceOf(SearchPhaseExecutionException.class));
                assertEquals(2, phaseInfo.getExpectedOps());
                assertEquals(2, phaseInfo.getProcessedShards().size());
                for (MainSearchTaskStatus.ShardInfo shardInfo : phaseInfo.getProcessedShards()) {
                    assertEquals("index", shardInfo.getSearchShardTarget().getShardId().getIndexName());
                    assertThat(shardInfo.getSearchShardTarget().getNodeId(), endsWith("node"));
                    if (shardInfo.getSearchShardTarget().getShardId().id() == 0) {
                        assertSame(exception, shardInfo.getFailure());
                        assertNull(shardInfo.getTaskInfo());
                    } else {
                        assertEquals(1, shardInfo.getSearchShardTarget().getShardId().id());
                        TaskInfo taskInfo = shardInfo.getTaskInfo();
                        assertEquals(1, taskInfo.getTaskId().getId());
                        assertEquals("node_id", taskInfo.getTaskId().getNodeId());
                        assertEquals("action", taskInfo.getAction());
                    }
                }
            });
        } finally {
            ThreadPool.terminate(executorService, 5, TimeUnit.SECONDS);
        }
    }

    public void testPhasesProgressReporting() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(randomBoolean());
        MainSearchTask mainSearchTask = new MainSearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
                searchRequest,
                mainSearchTask,
                new ArraySearchPhaseResults<>(2),
                ActionListener.wrap(r -> { }, e -> { }),
                false,
                new AtomicLong(),
                (searchActionListener, shard) -> {
                    try {
                        //block while performing phase on the shards so that we can check the status while the phase is being executed
                        latch.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    SearchPhaseResult searchPhaseResult = new SearchPhaseResult() {};
                    searchPhaseResult.setTaskInfo(new TaskInfo(new TaskId("node", 1), "type", "action", null, null, -1, -1, true, null,
                        Collections.emptyMap()));
                    searchActionListener.onResponse(searchPhaseResult);
                },
                executorService,
                new SearchPhase("next") {
                    @Override
                    public void run() {
                        mainSearchTask.getStatus().phaseStarted("next", 10);
                        mainSearchTask.getStatus().phaseCompleted("next");
                    }
                });
            new Thread(action::start).start();

            assertSame(mainSearchTask, action.getTask());

            assertBusy(() -> {
                MainSearchTaskStatus status = action.getTask().getStatus();
                MainSearchTaskStatus.PhaseInfo currentPhase = status.getCurrentPhase();
                assertNotNull(currentPhase);
                assertEquals("test", currentPhase.getName());
                assertEquals(2, currentPhase.getExpectedOps());
                assertNull(currentPhase.getFailure());
                assertEquals(0, currentPhase.getProcessedShards().size());
                assertEquals(0, status.getCompletedPhases().size());
            });

            //stop waiting on the shard operations and let the phase complete
            latch.countDown();

            assertBusy(() -> {
                MainSearchTaskStatus status = action.getTask().getStatus();
                assertNull(status.getCurrentPhase());
                List<MainSearchTaskStatus.PhaseInfo> completedPhases = status.getCompletedPhases();
                assertEquals(2, completedPhases.size());
                {
                    MainSearchTaskStatus.PhaseInfo phaseInfo = completedPhases.get(0);
                    assertNull(phaseInfo.getFailure());
                    assertEquals("test", phaseInfo.getName());
                    assertEquals(2, phaseInfo.getExpectedOps());
                    assertEquals(2, phaseInfo.getProcessedShards().size());
                    {
                        MainSearchTaskStatus.ShardInfo shardInfo = phaseInfo.getProcessedShards().get(0);
                        assertNull(shardInfo.getFailure());
                        assertEquals("index", shardInfo.getSearchShardTarget().getIndex());
                        assertEquals(0, shardInfo.getSearchShardTarget().getShardId().getId());
                        TaskInfo taskInfo = shardInfo.getTaskInfo();
                        assertEquals(1, taskInfo.getTaskId().getId());
                        assertEquals("node", taskInfo.getTaskId().getNodeId());
                        assertEquals("action", taskInfo.getAction());
                    }
                    {
                        MainSearchTaskStatus.ShardInfo shardInfo = phaseInfo.getProcessedShards().get(1);
                        assertNull(shardInfo.getFailure());
                        assertEquals("index", shardInfo.getSearchShardTarget().getIndex());
                        assertEquals(1, shardInfo.getSearchShardTarget().getShardId().getId());
                        TaskInfo taskInfo = shardInfo.getTaskInfo();
                        assertEquals(1, taskInfo.getTaskId().getId());
                        assertEquals("node", taskInfo.getTaskId().getNodeId());
                        assertEquals("action", taskInfo.getAction());
                    }
                }
                {
                    MainSearchTaskStatus.PhaseInfo phaseInfo = completedPhases.get(1);
                    assertNull(phaseInfo.getFailure());
                    assertEquals("next", phaseInfo.getName());
                    assertEquals(10, phaseInfo.getExpectedOps());
                    assertEquals(0, phaseInfo.getProcessedShards().size());
                }
            });
        } finally {
            ThreadPool.terminate(executorService, 5, TimeUnit.SECONDS);
        }
    }

    public void testNoShardsProgressReporting() {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(randomBoolean());
        MainSearchTask mainSearchTask = new MainSearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            mainSearchTask,
            new ArraySearchPhaseResults<>(0),
            ActionListener.wrap(r -> { }, e -> { }),
            false,
            new AtomicLong(),
            (searchActionListener, shard) -> {},
            null,
            new SearchPhase("next") {
                @Override
                public void run() {
                }
            });

        action.start();

        MainSearchTaskStatus status = action.getTask().getStatus();
        assertNull(status.getCurrentPhase());
        List<MainSearchTaskStatus.PhaseInfo> completedPhases = status.getCompletedPhases();
        assertEquals(1, completedPhases.size());
        MainSearchTaskStatus.PhaseInfo phaseInfo = completedPhases.get(0);
        assertNull(phaseInfo.getFailure());
        assertEquals("test", phaseInfo.getName());
        assertEquals(0, phaseInfo.getExpectedOps());
        assertEquals(0, phaseInfo.getProcessedShards().size());
    }

    private static ArraySearchPhaseResults<SearchPhaseResult> phaseResults(Set<Long> requestIds,
                                                                                              List<Tuple<String, String>> nodeLookups,
                                                                                              int numFailures) {
        int numResults = randomIntBetween(1, 10);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(numResults + numFailures);

        for (int i = 0; i < numResults; i++) {
            long requestId = randomLong();
            requestIds.add(requestId);
            SearchPhaseResult phaseResult = new PhaseResult(requestId);
            String resultClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String resultNodeId = randomAlphaOfLengthBetween(5, 10);
            ShardId resultShardId = new ShardId("index", "index-uuid", i);
            nodeLookups.add(Tuple.tuple(resultClusterAlias, resultNodeId));
            phaseResult.setSearchShardTarget(new SearchShardTarget(resultNodeId, resultShardId, resultClusterAlias, OriginalIndices.NONE));
            phaseResult.setShardIndex(i);
            phaseResults.consumeResult(phaseResult);
        }
        return phaseResults;
    }

    private static final class PhaseResult extends SearchPhaseResult {
        PhaseResult(long requestId) {
            this.requestId = requestId;
        }
    }
}
