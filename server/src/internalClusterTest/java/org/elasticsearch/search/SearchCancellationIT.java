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
import org.elasticsearch.action.DocWriteRequest;
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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.timeseries.TimeSeriesAggregationBuilder;
import org.elasticsearch.search.lookup.LeafStoredFieldsLookup;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.BeforeClass;

import java.time.Instant;
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

import static org.elasticsearch.index.IndexSettings.TIME_SERIES_END_TIME;
import static org.elasticsearch.index.IndexSettings.TIME_SERIES_START_TIME;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.search.SearchCancellationIT.ScriptedBlockPlugin.SEARCH_BLOCK_SCRIPT_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class SearchCancellationIT extends ESIntegTestCase {

    private static boolean lowLevelCancellation;

    @BeforeClass
    public static void init() {
        lowLevelCancellation = randomBoolean();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ScriptedBlockPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
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
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
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
        CancelTasksResponse cancelTasksResponse = client().admin()
            .cluster()
            .prepareCancelTasks()
            .setTargetTaskId(searchTask.taskId())
            .get();
        assertThat(cancelTasksResponse.getTasks(), hasSize(1));
        assertThat(cancelTasksResponse.getTasks().get(0).taskId(), equalTo(searchTask.taskId()));
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
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
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

    public void testCancellationDuringTimeSeriesAggregation() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        int numberOfShards = between(2, 5);
        long now = Instant.now().toEpochMilli();
        int numberOfRefreshes = between(1, 5);
        // After a few initial checks we check every 2048 - number of shards records so we need to ensure all
        // shards have enough records to trigger a check
        int numberOfDocsPerRefresh = numberOfShards * between(3000, 3500) / numberOfRefreshes;
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
                    .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
                    .put(TIME_SERIES_START_TIME.getKey(), now)
                    .put(TIME_SERIES_END_TIME.getKey(), now + (long) numberOfRefreshes * numberOfDocsPerRefresh + 1)
                    .build()
            ).setMapping("""
                {
                  "properties": {
                    "@timestamp": {"type": "date", "format": "epoch_millis"},
                    "dim": {"type": "keyword", "time_series_dimension": true}
                  }
                }
                """)
        );

        for (int i = 0; i < numberOfRefreshes; i++) {
            // Make sure we sometimes have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < numberOfDocsPerRefresh; j++) {
                bulkRequestBuilder.add(
                    client().prepareIndex("test")
                        .setOpType(DocWriteRequest.OpType.CREATE)
                        .setSource(
                            "@timestamp",
                            now + (long) i * numberOfDocsPerRefresh + j,
                            "val",
                            (double) j,
                            "dim",
                            String.valueOf(j % 100)
                        )
                );
            }
            assertNoFailures(bulkRequestBuilder.get());
        }

        logger.info("Executing search");
        TimeSeriesAggregationBuilder timeSeriesAggregationBuilder = new TimeSeriesAggregationBuilder("test_agg");
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addAggregation(
                timeSeriesAggregationBuilder.subAggregation(
                    new ScriptedMetricAggregationBuilder("sub_agg").initScript(
                        new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.INIT_SCRIPT_NAME, Collections.emptyMap())
                    )
                        .mapScript(
                            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.MAP_BLOCK_SCRIPT_NAME, Collections.emptyMap())
                        )
                        .combineScript(
                            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.COMBINE_SCRIPT_NAME, Collections.emptyMap())
                        )
                        .reduceScript(
                            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.REDUCE_FAIL_SCRIPT_NAME, Collections.emptyMap())
                        )
                )
            )
            .execute();
        awaitForBlock(plugins);
        cancelSearch(SearchAction.NAME);
        disableBlocks(plugins);

        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, searchResponse::actionGet);
        assertThat(ExceptionsHelper.status(ex), equalTo(RestStatus.BAD_REQUEST));
        logger.info("All shards failed with", ex);
        if (lowLevelCancellation) {
            // Ensure that we cancelled in TimeSeriesIndexSearcher and not in reduce phase
            assertThat(ExceptionsHelper.stackTrace(ex), containsString("TimeSeriesIndexSearcher"));
        }

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
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
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

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        static final String SEARCH_BLOCK_SCRIPT_NAME = "search_block";
        static final String INIT_SCRIPT_NAME = "init";
        static final String MAP_SCRIPT_NAME = "map";
        static final String MAP_BLOCK_SCRIPT_NAME = "map_block";
        static final String COMBINE_SCRIPT_NAME = "combine";
        static final String REDUCE_SCRIPT_NAME = "reduce";
        static final String REDUCE_FAIL_SCRIPT_NAME = "reduce_fail";
        static final String REDUCE_BLOCK_SCRIPT_NAME = "reduce_block";
        static final String TERM_SCRIPT_NAME = "term";

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
            return Map.of(
                SEARCH_BLOCK_SCRIPT_NAME,
                this::searchBlockScript,
                INIT_SCRIPT_NAME,
                this::nullScript,
                MAP_SCRIPT_NAME,
                this::nullScript,
                MAP_BLOCK_SCRIPT_NAME,
                this::mapBlockScript,
                COMBINE_SCRIPT_NAME,
                this::nullScript,
                REDUCE_BLOCK_SCRIPT_NAME,
                this::blockScript,
                REDUCE_SCRIPT_NAME,
                this::termScript,
                REDUCE_FAIL_SCRIPT_NAME,
                this::reduceFailScript,
                TERM_SCRIPT_NAME,
                this::termScript
            );
        }

        private Object searchBlockScript(Map<String, Object> params) {
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
        }

        private Object reduceFailScript(Map<String, Object> params) {
            fail("Shouldn't reach reduce");
            return true;
        }

        private Object nullScript(Map<String, Object> params) {
            return null;
        }

        private Object blockScript(Map<String, Object> params) {
            final Runnable runnable = beforeExecution.get();
            if (runnable != null) {
                runnable.run();
            }
            if (shouldBlock.get()) {
                LogManager.getLogger(SearchCancellationIT.class).info("Blocking in reduce");
            }
            hits.incrementAndGet();
            try {
                assertBusy(() -> assertFalse(shouldBlock.get()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 42;
        }

        private Object mapBlockScript(Map<String, Object> params) {
            final Runnable runnable = beforeExecution.get();
            if (runnable != null) {
                runnable.run();
            }
            if (shouldBlock.get()) {
                LogManager.getLogger(SearchCancellationIT.class).info("Blocking in map");
            }
            hits.incrementAndGet();
            try {
                assertBusy(() -> assertFalse(shouldBlock.get()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 1;
        }

        private Object termScript(Map<String, Object> params) {
            return 1;
        }
    }
}
