/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.AbstractSearchCancellationTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.test.AbstractSearchCancellationTestCase.ScriptedBlockPlugin.SEARCH_BLOCK_SCRIPT_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SearchCancellationIT extends AbstractSearchCancellationTestCase {

    @Override
    // TODO all tests need to be updated to work with concurrent search
    protected boolean enableConcurrentSearch() {
        return false;
    }

    public void testCancellationDuringQueryPhase() throws Exception {

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        logger.info("Executing search");
        ActionFuture<SearchResponse> searchResponse = prepareSearch("test").setQuery(
            scriptQuery(new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap()))
        ).execute();

        awaitForBlock(plugins);
        cancelSearch(TransportSearchAction.TYPE.name());
        disableBlocks(plugins);
        logger.info("Segments {}", Strings.toString(indicesAdmin().prepareSegments("test").get()));
        ensureSearchWasCancelled(searchResponse);
    }

    public void testCancellationDuringFetchPhase() throws Exception {

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        logger.info("Executing search");
        ActionFuture<SearchResponse> searchResponse = prepareSearch("test").addScriptField(
            "test_field",
            new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap())
        ).execute();

        awaitForBlock(plugins);
        cancelSearch(TransportSearchAction.TYPE.name());
        disableBlocks(plugins);
        logger.info("Segments {}", Strings.toString(indicesAdmin().prepareSegments("test").get()));
        ensureSearchWasCancelled(searchResponse);
    }

    public void testCancellationDuringAggregation() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        // This test is only meaningful with at least 2 shards to trigger reduce
        int numberOfShards = between(2, 5);
        createIndex("test", numberOfShards, 0);
        indexTestData();

        logger.info("Executing search");
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("test_agg");
        if (randomBoolean()) {
            termsAggregationBuilder.script(
                new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.TERM_SCRIPT_NAME, Collections.emptyMap())
            );
        } else {
            termsAggregationBuilder.field("field.keyword");
        }

        ActionFuture<SearchResponse> searchResponse = prepareSearch("test").setQuery(matchAllQuery())
            .addAggregation(
                termsAggregationBuilder.subAggregation(
                    new ScriptedMetricAggregationBuilder("sub_agg").initScript(
                        new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.INIT_SCRIPT_NAME, Collections.emptyMap())
                    )
                        .mapScript(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.MAP_SCRIPT_NAME, Collections.emptyMap()))
                        .combineScript(
                            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.COMBINE_SCRIPT_NAME, Collections.emptyMap())
                        )
                        .reduceScript(
                            new Script(
                                ScriptType.INLINE,
                                "mockscript",
                                ScriptedBlockPlugin.REDUCE_BLOCK_SCRIPT_NAME,
                                Collections.emptyMap()
                            )
                        )
                )
            )
            .execute();
        awaitForBlock(plugins);
        cancelSearch(TransportSearchAction.TYPE.name());
        disableBlocks(plugins);
        ensureSearchWasCancelled(searchResponse);
    }

    public void testCancellationOfScrollSearches() throws Exception {

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        logger.info("Executing search");
        ActionFuture<SearchResponse> searchResponse = prepareSearch("test").setScroll(TimeValue.timeValueSeconds(10))
            .setSize(5)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap())))
            .execute();

        awaitForBlock(plugins);
        cancelSearch(TransportSearchAction.TYPE.name());
        disableBlocks(plugins);
        SearchResponse response = ensureSearchWasCancelled(searchResponse);
        if (response != null) {
            // The response might not have failed on all shards - we need to clean scroll
            logger.info("Cleaning scroll with id {}", response.getScrollId());
            client().prepareClearScroll().addScrollId(response.getScrollId()).get();
        }
    }

    public void testCancellationOfScrollSearchesOnFollowupRequests() throws Exception {

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        // Disable block so the first request would pass
        disableBlocks(plugins);

        logger.info("Executing search");
        TimeValue keepAlive = TimeValue.timeValueSeconds(5);
        String scrollId;
        SearchResponse searchResponse = prepareSearch("test").setScroll(keepAlive)
            .setSize(2)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap())))
            .get();
        try {
            assertNotNull(searchResponse.getScrollId());

            // Enable block so the second request would block
            for (ScriptedBlockPlugin plugin : plugins) {
                plugin.reset();
                plugin.enableBlock();
            }

            scrollId = searchResponse.getScrollId();
            logger.info("Executing scroll with id {}", scrollId);
        } finally {
            searchResponse.decRef();
        }
        ActionFuture<SearchResponse> scrollResponse = client().prepareSearchScroll(searchResponse.getScrollId())
            .setScroll(keepAlive)
            .execute();

        awaitForBlock(plugins);
        cancelSearch(TransportSearchScrollAction.TYPE.name());
        disableBlocks(plugins);

        SearchResponse response = ensureSearchWasCancelled(scrollResponse);
        if (response != null) {
            // The response didn't fail completely - update scroll id
            scrollId = response.getScrollId();
        }
        logger.info("Cleaning scroll with id {}", scrollId);
        client().prepareClearScroll().addScrollId(scrollId).get();
    }

    public void testCancelMultiSearch() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();
        ActionFuture<MultiSearchResponse> multiSearchResponse = client().prepareMultiSearch()
            .add(
                prepareSearch("test").addScriptField(
                    "test_field",
                    new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap())
                )
            )
            .execute();
        MultiSearchResponse response = null;
        try {
            awaitForBlock(plugins);
            cancelSearch(TransportMultiSearchAction.TYPE.name());
            disableBlocks(plugins);
            response = multiSearchResponse.actionGet();
            for (MultiSearchResponse.Item item : response) {
                if (item.getFailure() != null) {
                    assertThat(ExceptionsHelper.unwrap(item.getFailure(), TaskCancelledException.class), notNullValue());
                } else {
                    assertFailures(item.getResponse());
                    for (ShardSearchFailure shardFailure : item.getResponse().getShardFailures()) {
                        assertThat(ExceptionsHelper.unwrap(shardFailure.getCause(), TaskCancelledException.class), notNullValue());
                    }
                }
            }
        } finally {
            if (response != null) response.decRef();
        }
    }

    public void testCancelFailedSearchWhenPartialResultDisallowed() throws Exception {
        // TODO: make this test compatible with batched execution, currently the exceptions are slightly different with batched
        updateClusterSettings(Settings.builder().put(SearchService.BATCHED_QUERY_PHASE.getKey(), false));
        // Have at least two nodes so that we have parallel execution of two request guaranteed even if max concurrent requests per node
        // are limited to 1
        internalCluster().ensureAtLeastNumDataNodes(2);
        int numberOfShards = between(2, 5);
        createIndex("test", numberOfShards, 0);
        indexTestData();

        // Define (but don't run) the search request, expecting a partial shard failure. We will run it later.
        Thread searchThread = new Thread(() -> {
            logger.info("Executing search");
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                prepareSearch("test").setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap())))
                    .setAllowPartialSearchResults(false)
                    .setSize(1000)
            );
            assertThat(e.getMessage(), containsString("Partial shards failure"));
        });

        // When the search request executes, block all shards except 1.
        final List<SearchShardBlockingPlugin> searchShardBlockingPlugins = initSearchShardBlockingPlugin();
        AtomicBoolean letOneShardProceed = new AtomicBoolean();
        // Ensure we have at least one task waiting on the latch
        CountDownLatch waitingTaskLatch = new CountDownLatch(1);
        CountDownLatch shardTaskLatch = new CountDownLatch(1);
        for (SearchShardBlockingPlugin plugin : searchShardBlockingPlugins) {
            plugin.setRunOnNewReaderContext((ReaderContext c) -> {
                if (letOneShardProceed.compareAndSet(false, true)) {
                    // Let one shard continue.
                } else {
                    // Signal that we have a task waiting on the latch
                    waitingTaskLatch.countDown();
                    safeAwait(shardTaskLatch); // Block the other shards.
                }
            });
        }

        // For the shard that was allowed to proceed, have a single query-execution thread throw an exception.
        final List<ScriptedBlockPlugin> plugins = initBlockFactory();
        AtomicBoolean oneThreadWillError = new AtomicBoolean();
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
            plugin.setBeforeExecution(() -> {
                if (oneThreadWillError.compareAndSet(false, true)) {
                    // wait for some task to get to the latch
                    safeAwait(waitingTaskLatch);
                    // then throw the exception
                    throw new IllegalStateException("This will cancel the ContextIndexSearcher.search task");
                }
            });
        }

        // Now run the search request.
        logger.info("Starting search thread");
        searchThread.start();

        try {
            assertBusy(() -> {
                final List<SearchTask> coordinatorSearchTask = getCoordinatorSearchTasks();
                logger.info("Checking tasks: {}", coordinatorSearchTask);
                assertThat("The Coordinator should have one SearchTask.", coordinatorSearchTask, hasSize(1));
                assertTrue("The SearchTask should be cancelled.", coordinatorSearchTask.get(0).isCancelled());
                for (var shardQueryTask : getShardQueryTasks()) {
                    assertTrue("All SearchShardTasks should then be cancelled", shardQueryTask.isCancelled());
                }
            }, 30, TimeUnit.SECONDS);
        } finally {
            shardTaskLatch.countDown(); // unblock the shardTasks, allowing the test to conclude.
            searchThread.join();
            plugins.forEach(plugin -> plugin.setBeforeExecution(() -> {}));
            searchShardBlockingPlugins.forEach(plugin -> plugin.setRunOnNewReaderContext((ReaderContext c) -> {}));
        }
    }

    List<SearchTask> getCoordinatorSearchTasks() {
        List<SearchTask> tasks = new ArrayList<>();
        for (String nodeName : internalCluster().getNodeNames()) {
            TransportService transportService = internalCluster().getInstance(TransportService.class, nodeName);
            for (Task task : transportService.getTaskManager().getCancellableTasks().values()) {
                if (task.getAction().equals(TransportSearchAction.TYPE.name())) {
                    tasks.add((SearchTask) task);
                }
            }
        }
        return tasks;
    }

    List<SearchShardTask> getShardQueryTasks() {
        List<SearchShardTask> tasks = new ArrayList<>();
        for (String nodeName : internalCluster().getNodeNames()) {
            TransportService transportService = internalCluster().getInstance(TransportService.class, nodeName);
            for (Task task : transportService.getTaskManager().getCancellableTasks().values()) {
                if (task.getAction().equals(SearchTransportService.QUERY_ACTION_NAME)) {
                    tasks.add((SearchShardTask) task);
                }
            }
        }
        return tasks;
    }
}
