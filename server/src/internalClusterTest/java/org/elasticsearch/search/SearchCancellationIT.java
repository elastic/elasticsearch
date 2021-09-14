/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.LeafStoredFieldsLookup;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.search.SearchCancellationIT.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class SearchCancellationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ScriptedBlockPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        boolean lowLevelCancellation = randomBoolean();
        logger.info("Using lowLevelCancellation: {}", lowLevelCancellation);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), lowLevelCancellation)
            .build();
    }

    private void indexTestData() {
        for (int i = 0; i < 5; i++) {
            // Make sure we have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(client().prepareIndex("test").setId(Integer.toString(i * 5 + j)).setSource("field", "value"));
            }
            assertNoFailures(bulkRequestBuilder.get());
        }
    }

    private List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    private void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
        int numberOfShards = getNumShards("test").numPrimaries;
        assertBusy(() -> {
            int numberOfBlockedPlugins = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                numberOfBlockedPlugins += plugin.hits.get();
            }
            logger.info("The plugin blocked on {} out of {} shards", numberOfBlockedPlugins, numberOfShards);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        });
    }

    private void disableBlocks(List<ScriptedBlockPlugin> plugins) throws Exception {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    private void cancelSearch(String action) {
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().setActions(action).get();
        assertThat(listTasksResponse.getTasks(), hasSize(1));
        TaskInfo searchTask = listTasksResponse.getTasks().get(0);

        logger.info("Cancelling search");
        CancelTasksResponse cancelTasksResponse = client().admin().cluster().prepareCancelTasks().setTaskId(searchTask.getTaskId()).get();
        assertThat(cancelTasksResponse.getTasks(), hasSize(1));
        assertThat(cancelTasksResponse.getTasks().get(0).getTaskId(), equalTo(searchTask.getTaskId()));
    }

    private SearchResponse ensureSearchWasCancelled(ActionFuture<SearchResponse> searchResponse) {
        try {
            SearchResponse response = searchResponse.actionGet();
            logger.info("Search response {}", response);
            assertNotEquals("At least one shard should have failed", 0, response.getFailedShards());
            for (ShardSearchFailure failure : response.getShardFailures()) {
                // We should have fail because the search has been cancel. The status of the exceptions should be 400.
                assertThat(ExceptionsHelper.status(failure.getCause()), equalTo(RestStatus.BAD_REQUEST));
            }
            return response;
        } catch (SearchPhaseExecutionException ex) {
            // We should have fail because the search has been cancel. The status of the response should be 400.
            assertThat(ExceptionsHelper.status(ex), equalTo(RestStatus.BAD_REQUEST));
            logger.info("All shards failed with", ex);
            return null;
        }
    }

    public void testCancellationDuringQueryPhase() throws Exception {

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        logger.info("Executing search");
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test").setQuery(
            scriptQuery(new Script(
                ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
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
            .addScriptField("test_field",
                new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())
            ).execute();

        awaitForBlock(plugins);
        cancelSearch(SearchAction.NAME);
        disableBlocks(plugins);
        logger.info("Segments {}", Strings.toString(client().admin().indices().prepareSegments("test").get()));
        ensureSearchWasCancelled(searchResponse);
    }

    public void testCancellationOfScrollSearches() throws Exception {

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        logger.info("Executing search");
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setScroll(TimeValue.timeValueSeconds(10))
            .setSize(5)
            .setQuery(
                scriptQuery(new Script(
                    ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
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
            .setQuery(
                scriptQuery(new Script(
                    ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
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
            .setScroll(keepAlive).execute();

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
        ActionFuture<MultiSearchResponse> msearchResponse = client().prepareMultiSearch().add(client().prepareSearch("test")
            .addScriptField("test_field", new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
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
        createIndex("test", Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());
        indexTestData();
        Thread searchThread = new Thread(() -> {
            SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () ->
                client().prepareSearch("test")
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                    .setAllowPartialSearchResults(false).setSize(1000).get());
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

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_block";

        private final AtomicInteger hits = new AtomicInteger();

        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        private final AtomicReference<Runnable> beforeExecution = new AtomicReference<>();

        public void reset() {
            hits.set(0);
        }

        public void disableBlock() {
            shouldBlock.set(false);
        }

        public void enableBlock() {
            shouldBlock.set(true);
        }

        public void setBeforeExecution(Runnable runnable) {
            beforeExecution.set(runnable);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                final Runnable runnable = beforeExecution.get();
                if (runnable != null) {
                    runnable.run();
                }
                LeafStoredFieldsLookup fieldsLookup = (LeafStoredFieldsLookup) params.get("_fields");
                LogManager.getLogger(SearchCancellationIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    assertBusy(() -> assertFalse(shouldBlock.get()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }
}
