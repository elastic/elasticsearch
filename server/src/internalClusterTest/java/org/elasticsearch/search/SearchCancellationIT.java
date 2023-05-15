/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
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

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class SearchCancellationIT extends AbstractSearchCancellationTestCase {

    public void testCancellationDuringQueryPhase() throws Exception {

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        logger.info("Executing search");
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap())))
            .execute();

        awaitForBlock(plugins);
        cancelSearch(SearchAction.NAME);
        disableBlocks(plugins);
        logger.info("Segments {}", Strings.toString(client().admin().indices().prepareSegments("test").get()));
        ensureSearchWasCancelled(searchResponse);
    }

    public void testCancellationDuringFetchPhase() throws Exception {

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        logger.info("Executing search");
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .addScriptField("test_field", new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap()))
            .execute();

        awaitForBlock(plugins);
        cancelSearch(SearchAction.NAME);
        disableBlocks(plugins);
        logger.info("Segments {}", Strings.toString(client().admin().indices().prepareSegments("test").get()));
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

        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
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
        cancelSearch(SearchAction.NAME);
        disableBlocks(plugins);
        ensureSearchWasCancelled(searchResponse);
    }

    public void testCancellationOfScrollSearches() throws Exception {

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        logger.info("Executing search");
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setScroll(TimeValue.timeValueSeconds(10))
            .setSize(5)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap())))
            .execute();

        awaitForBlock(plugins);
        cancelSearch(SearchAction.NAME);
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
        SearchResponse searchResponse = client().prepareSearch("test")
            .setScroll(keepAlive)
            .setSize(2)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap())))
            .get();

        assertNotNull(searchResponse.getScrollId());

        // Enable block so the second request would block
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }

        String scrollId = searchResponse.getScrollId();
        logger.info("Executing scroll with id {}", scrollId);
        ActionFuture<SearchResponse> scrollResponse = client().prepareSearchScroll(searchResponse.getScrollId())
            .setScroll(keepAlive)
            .execute();

        awaitForBlock(plugins);
        cancelSearch(SearchScrollAction.NAME);
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
        ActionFuture<MultiSearchResponse> msearchResponse = client().prepareMultiSearch()
            .add(
                client().prepareSearch("test")
                    .addScriptField(
                        "test_field",
                        new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap())
                    )
            )
            .execute();
        awaitForBlock(plugins);
        cancelSearch(MultiSearchAction.NAME);
        disableBlocks(plugins);
        for (MultiSearchResponse.Item item : msearchResponse.actionGet()) {
            if (item.getFailure() != null) {
                assertThat(ExceptionsHelper.unwrap(item.getFailure(), TaskCancelledException.class), notNullValue());
            } else {
                assertFailures(item.getResponse());
                for (ShardSearchFailure shardFailure : item.getResponse().getShardFailures()) {
                    assertThat(ExceptionsHelper.unwrap(shardFailure.getCause(), TaskCancelledException.class), notNullValue());
                }
            }
        }
    }

    public void testCancelFailedSearchWhenPartialResultDisallowed() throws Exception {
        final List<ScriptedBlockPlugin> plugins = initBlockFactory();
        int numberOfShards = between(2, 5);
        AtomicBoolean failed = new AtomicBoolean();
        CountDownLatch queryLatch = new CountDownLatch(1);
        CountDownLatch cancelledLatch = new CountDownLatch(1);
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
            plugin.setBeforeExecution(() -> {
                try {
                    queryLatch.await(); // block the query until we get a search task
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                if (failed.compareAndSet(false, true)) {
                    throw new IllegalStateException("simulated");
                }
                try {
                    cancelledLatch.await(); // block the query until the search is cancelled
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            });
        }
        createIndex("test", numberOfShards, 0);
        indexTestData();
        Thread searchThread = new Thread(() -> {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test")
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SEARCH_BLOCK_SCRIPT_NAME, Collections.emptyMap())))
                    .setAllowPartialSearchResults(false)
                    .setSize(1000)
                    .get()
            );
            assertThat(e.getMessage(), containsString("Partial shards failure"));
        });
        searchThread.start();
        try {
            assertBusy(() -> assertThat(getSearchTasks(), hasSize(1)));
            queryLatch.countDown();
            assertBusy(() -> {
                final List<SearchTask> searchTasks = getSearchTasks();
                // The search request can complete before the "cancelledLatch" is latched if the second shard request is sent
                // after the request was cancelled (i.e., the child task is not allowed to start after the parent was cancelled).
                if (searchTasks.isEmpty() == false) {
                    assertThat(searchTasks, hasSize(1));
                    assertTrue(searchTasks.get(0).isCancelled());
                }
            }, 30, TimeUnit.SECONDS);
        } finally {
            for (ScriptedBlockPlugin plugin : plugins) {
                plugin.setBeforeExecution(() -> {});
            }
            cancelledLatch.countDown();
            searchThread.join();
        }
    }

    List<SearchTask> getSearchTasks() {
        List<SearchTask> tasks = new ArrayList<>();
        for (String nodeName : internalCluster().getNodeNames()) {
            TransportService transportService = internalCluster().getInstance(TransportService.class, nodeName);
            for (Task task : transportService.getTaskManager().getCancellableTasks().values()) {
                if (task.getAction().equals(SearchAction.NAME)) {
                    tasks.add((SearchTask) task);
                }
            }
        }
        return tasks;
    }

}
