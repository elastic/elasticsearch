/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchProgressListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class RejectionActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReductionRejectionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("thread_pool.search.size", 1)
            .put("thread_pool.search.queue_size", 1)
            .put("thread_pool.get.size", 1)
            .put("thread_pool.get.queue_size", 1)
            .build();
    }

    public void testSimulatedSearchRejectionLoad() throws Throwable {
        for (int i = 0; i < 10; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "1").get();
        }

        int numberOfAsyncOps = randomIntBetween(200, 700);
        final CountDownLatch latch = new CountDownLatch(numberOfAsyncOps);
        final CopyOnWriteArrayList<Object> responses = new CopyOnWriteArrayList<>();
        for (int i = 0; i < numberOfAsyncOps; i++) {
            prepareSearch("test").setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("field", "1"))
                .execute(new LatchedActionListener<>(new ActionListener<>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        responses.add(searchResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        responses.add(e);
                    }
                }, latch));
        }
        // Bounded wait: some search requests can be stuck indefinitely (see #146022), in which case we'd
        // rather fail fast with a clear timeout than have the whole suite hang until it hits its own timeout.
        try {
            safeAwait(latch, TimeValue.ONE_MINUTE);
        } catch (AssertionError e) {
            // Temporary measures to help us debug https://github.com/elastic/elasticsearch/issues/153848
            logger.error(
                "Search requests stuck: numAsyncOps: {}, responses: {}, count: {}",
                numberOfAsyncOps,
                responses.size(),
                latch.getCount(),
                e
            );
            dumpStuckSearchDiagnostics();
            throw e;
        }

        // validate all responses
        for (Object response : responses) {
            if (response instanceof SearchResponse searchResponse) {
                for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                    assertThat(
                        failure.reason().toLowerCase(Locale.ENGLISH),
                        anyOf(containsString("cancelled"), containsString("rejected"))
                    );
                }
            } else {
                Exception t = (Exception) response;
                Throwable unwrap = ExceptionsHelper.unwrapCause(t);
                if (unwrap instanceof SearchPhaseExecutionException e) {
                    for (ShardSearchFailure failure : e.shardFailures()) {
                        assertThat(
                            failure.reason().toLowerCase(Locale.ENGLISH),
                            anyOf(containsString("cancelled"), containsString("rejected"))
                        );
                    }
                } else if ((unwrap instanceof EsRejectedExecutionException) == false) {
                    throw new AssertionError("unexpected failure", (Throwable) response);
                }
            }
        }
        assertThat(responses.size(), equalTo(numberOfAsyncOps));
    }

    /**
     * Forces a remote reduction rejection while the same search's coordinator reduction holds a shard-completion callback.
     */
    public void testRemoteReductionRejectionWithPendingCoordinatorReduction() throws Exception {
        var dataNodes = Arrays.stream(internalCluster().getNodeNames())
            .filter(name -> internalCluster().getInstance(ClusterService.class, name).localNode().canContainData())
            .toList();
        assertEquals(2, dataNodes.size());
        String coordinator = dataNodes.get(0);
        String remote = dataNodes.get(1);
        for (int i = 0; i < 2; i++) {
            String index = i == 0 ? "rejection-local" : "rejection-remote";
            createIndex(
                index,
                Settings.builder()
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.require._name", dataNodes.get(i))
                    .build()
            );
            ensureGreen(index);
            for (int shard = 0; shard < 3; shard++) {
                prepareIndex(index).setRouting(routingKeyForShard(index, shard)).setSource("field", "1").get();
            }
        }
        refresh("rejection-local", "rejection-remote");

        var coordinatorMergeStarted = new CountDownLatch(1);
        var releaseCoordinatorMerge = new CountDownLatch(1);
        var remoteResultsReceived = new CountDownLatch(3);
        var searchCompleted = new CountDownLatch(1);
        var searchFailure = new AtomicReference<Exception>();
        var remoteQueriesCompleted = new AtomicInteger();
        var remoteExecutor = internalCluster().getInstance(ThreadPool.class, remote).executor(ThreadPool.Names.SEARCH);
        long rejectedBefore = searchRejections(remote);
        ReductionRejectionPlugin.beforeRemoteQuery = () -> safeAwait(coordinatorMergeStarted);
        ReductionRejectionPlugin.afterRemoteQuery = () -> {
            if (remoteQueriesCompleted.incrementAndGet() == 3) {
                // This callback runs on the sole search worker. Occupy the sole queue slot so that the
                // reduction scheduled by the third successful shard result is rejected, not the shard query.
                remoteExecutor.execute(() -> {});
            }
        };

        var progressListener = new SearchProgressListener() {
            @Override
            protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
                coordinatorMergeStarted.countDown();
                safeAwait(releaseCoordinatorMerge);
            }

            @Override
            protected void onQueryResult(int shardIndex, QuerySearchResult result) {
                if (result.getSearchShardTarget().getShardId().getIndexName().equals("rejection-remote")) {
                    // The batched response handler records the reduction failure before delivering these results.
                    remoteResultsReceived.countDown();
                }
            }
        };
        var request = new SearchRequest("rejection-local", "rejection-remote") {
            @Override
            public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
                task.setProgressListener(progressListener);
                return task;
            }
        };
        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        request.setBatchedReduceSize(2);
        request.setMaxConcurrentShardRequests(1);
        request.setPreFilterShardSize(Integer.MAX_VALUE);
        try {
            internalCluster().client(coordinator).execute(TransportSearchAction.TYPE, request, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse response) {
                    response.decRef();
                    searchCompleted.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    searchFailure.set(e);
                    searchCompleted.countDown();
                }
            });
            safeAwait(remoteResultsReceived);
            assertEquals(3, remoteQueriesCompleted.get());
            assertEquals("only the remote partial reduction should be rejected", rejectedBefore + 1, searchRejections(remote));
            releaseCoordinatorMerge.countDown();
            safeAwait(searchCompleted, TimeValue.timeValueSeconds(10));
            assertNotNull("the remote reduction rejection must fail the search", searchFailure.get());
        } finally {
            coordinatorMergeStarted.countDown();
            releaseCoordinatorMerge.countDown();
            ReductionRejectionPlugin.beforeRemoteQuery = () -> {};
            ReductionRejectionPlugin.afterRemoteQuery = () -> {};
        }
    }

    private long searchRejections(String node) {
        return internalCluster().getInstance(ThreadPool.class, node)
            .stats()
            .stats()
            .stream()
            .filter(stats -> stats.name().equals(ThreadPool.Names.SEARCH))
            .findFirst()
            .orElseThrow()
            .rejected();
    }

    /** Allows the test to order real shard queries and saturate the remote executor immediately before a reduction. */
    public static class ReductionRejectionPlugin extends Plugin {
        private static volatile Runnable beforeRemoteQuery = () -> {};
        private static volatile Runnable afterRemoteQuery = () -> {};

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (indexModule.getIndex().getName().equals("rejection-remote")) {
                indexModule.addSearchOperationListener(new SearchOperationListener() {
                    @Override
                    public void onPreQueryPhase(SearchContext searchContext) {
                        beforeRemoteQuery.run();
                    }

                    @Override
                    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
                        afterRemoteQuery.run();
                    }
                });
            }
        }
    }

    private void dumpStuckSearchDiagnostics() {
        StringBuilder sb = new StringBuilder();
        var threadBean = ManagementFactory.getThreadMXBean();
        sb.append("thread dump:\n");
        for (var info : threadBean.dumpAllThreads(true, true)) {
            sb.append(Strings.format("[%s]: %s\n", info.getThreadName(), info.getThreadState()));
            if (info.getLockInfo() != null) {
                sb.append(Strings.format(" waiting on %s", info.getLockInfo()));
                if (info.getLockOwnerName() != null) {
                    sb.append(Strings.format(" held by [%s]", info.getLockOwnerName()));
                }
            }
            sb.append('\n');
            for (var frame : info.getStackTrace()) {
                sb.append("\tat ").append(frame).append('\n');
            }
            sb.append('\n');
        }
        logger.error("Thread dump: {}", sb);
    }

}
