/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.StoppableExecutorServiceWrapper;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class SearchWithRejectionsIT extends ESIntegTestCase {

    private static final int SEARCH_QUEUE_SIZE = 1;

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("thread_pool.search.size", 1)
            .put("thread_pool.search.queue_size", SEARCH_QUEUE_SIZE)
            .build();
    }

    public void testOpenContextsAfterRejections() throws Exception {
        createIndex("test");
        ensureGreen("test");
        final int docs = scaledRandomIntBetween(20, 50);
        for (int i = 0; i < docs; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value").get();
        }
        assertThat(indicesAdmin().prepareStats("test").get().getTotal().getSearch().getOpenContexts(), equalTo(0L));
        refresh();

        int numSearches = 10;
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Future<SearchResponse>[] responses = new Future[numSearches];
        SearchType searchType = randomFrom(SearchType.DEFAULT, SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH);
        logger.info("search type is {}", searchType);
        for (int i = 0; i < numSearches; i++) {
            responses[i] = prepareSearch().setQuery(matchAllQuery()).setSearchType(searchType).execute();
        }
        for (int i = 0; i < numSearches; i++) {
            try {
                responses[i].get().decRef();
            } catch (Exception t) {}
        }
        assertBusyOpenContexts("test", 0L);
    }

    public void testScrollContextSurvivesQueueRejection() throws Exception {
        createIndex("test-scroll", 1, 0);
        ensureGreen("test-scroll");
        final int numDocs = 10;
        Set<String> expectedIds = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            expectedIds.add(id);
            prepareIndex("test-scroll").setId(id).setSource("field", "value").get();
        }
        refresh();

        SearchResponse openResponse = prepareSearch("test-scroll").setQuery(matchAllQuery())
            .setSize(1)
            .setScroll(TimeValue.timeValueMinutes(5))
            .get();
        String scrollId = openResponse.getScrollId();
        Set<String> seenIds = new HashSet<>();
        try {
            collectIds(openResponse, seenIds);
            assertBusyOpenContexts("test-scroll", 1L);

            String primaryNodeId = clusterService().state().routingTable().index("test-scroll").shard(0).primaryShard().currentNodeId();
            String primaryNodeName = clusterService().state().nodes().get(primaryNodeId).getName();
            ThreadPool primaryThreadPool = internalCluster().getInstance(ThreadPool.class, primaryNodeName);

            // Closing unblockSearchPool releases the blocked SEARCH threads so later scrolls can run.
            try (Releasable unblockSearchPool = blockSearchThreadPool(primaryThreadPool)) {
                // Bounded wait: if rejection fails to surface, do not deadlock the suite.
                Exception e = expectThrows(Exception.class, () -> {
                    SearchResponse response = client().prepareSearchScroll(scrollId)
                        .setScroll(TimeValue.timeValueMinutes(5))
                        .get(SAFE_AWAIT_TIMEOUT);
                    response.decRef();
                });
                assertThat(ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class), notNullValue());
            }

            assertBusyOpenContexts("test-scroll", 1L);

            assertBusy(() -> {
                try {
                    while (seenIds.size() < numDocs) {
                        SearchResponse scrollResponse = client().prepareSearchScroll(scrollId)
                            .setScroll(TimeValue.timeValueMinutes(5))
                            .get();
                        try {
                            if (scrollResponse.getHits().getHits().length == 0) {
                                break;
                            }
                            collectIds(scrollResponse, seenIds);
                        } finally {
                            scrollResponse.decRef();
                        }
                    }
                } catch (Exception e) {
                    if (ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class) != null) {
                        throw new AssertionError("retry scroll after rejection", e);
                    }
                    throw new AssertionError(e);
                }
                assertThat(seenIds, equalTo(expectedIds));
            }, 10, TimeUnit.SECONDS);
        } finally {
            openResponse.decRef();
            client().prepareClearScroll().addScrollId(scrollId).get();
        }
    }

    /**
     * Blocks all SEARCH threads and fills the queue on {@code threadPool} so the next submission is rejected.
     * Caller must close the returned releasable to unblock those threads.
     */
    private Releasable blockSearchThreadPool(ThreadPool threadPool) {
        final CountDownLatch block = new CountDownLatch(1);
        final int threads = threadPool.info(ThreadPool.Names.SEARCH).getMax();
        final CountDownLatch started = new CountDownLatch(threads);
        // Stoppable wrapper: shutdown() is a no-op so we cannot tear down the node-owned SEARCH pool.
        // Not try-with-resources: on this branch ExecutorService is not AutoCloseable.
        final ExecutorService searchExecutor = new StoppableExecutorServiceWrapper(threadPool.executor(ThreadPool.Names.SEARCH));
        try {
            AtomicInteger submitted = new AtomicInteger();
            // Submit one execution per thread, plus enough to block all queue slots
            int executions = threads + SEARCH_QUEUE_SIZE;
            assertBusy(() -> {
                int toSubmit = executions - submitted.get();
                for (int i = 0; i < toSubmit; ++i) {
                    try {
                        searchExecutor.execute(() -> {
                            started.countDown();
                            awaitQuietly(block);
                        });
                        submitted.incrementAndGet();
                    } catch (EsRejectedExecutionException e) {
                        // one of the search threads was already in use by some transient thing, we'll try resubmitting
                    }
                }
                assertThat("Could not saturate the search thread pool and queue", submitted.get(), equalTo(executions));
            });
            safeAwait(started);
            expectThrows(EsRejectedExecutionException.class, () -> searchExecutor.execute(() -> {}));
        } catch (Throwable t) {
            // The cluster is shared by the whole suite, so never leave SEARCH threads blocked on a setup failure.
            block.countDown();
            throw new AssertionError("failed to saturate SEARCH pool", t);
        }
        return () -> {
            if (block.getCount() > 0) {
                block.countDown();
            }
        };
    }

    private void assertBusyOpenContexts(String index, long expected) throws Exception {
        assertBusy(
            () -> assertThat(indicesAdmin().prepareStats(index).get().getTotal().getSearch().getOpenContexts(), equalTo(expected)),
            2,
            TimeUnit.SECONDS
        );
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void collectIds(SearchResponse response, Set<String> seenIds) {
        for (SearchHit hit : response.getHits()) {
            assertTrue("duplicate hit id " + hit.getId(), seenIds.add(hit.getId()));
        }
    }
}
