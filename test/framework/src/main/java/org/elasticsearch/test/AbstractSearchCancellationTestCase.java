/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.lookup.LeafStoredFieldsLookup;
import org.elasticsearch.tasks.TaskInfo;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class AbstractSearchCancellationTestCase extends ESIntegTestCase {

    protected static boolean lowLevelCancellation;

    @BeforeClass
    public static void init() {
        lowLevelCancellation = randomBoolean();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ScriptedBlockPlugin.class, SearchShardBlockingPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        logger.info("Using lowLevelCancellation: {}", lowLevelCancellation);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), lowLevelCancellation)
            .build();
    }

    protected void indexTestData() {
        for (int i = 0; i < 5; i++) {
            // Make sure we have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(prepareIndex("test").setId(Integer.toString(i * 5 + j)).setSource("field", "value"));
            }
            assertNoFailures(bulkRequestBuilder.get());
        }
    }

    protected List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            pluginsService.filterPlugins(ScriptedBlockPlugin.class).forEach(plugins::add);
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    protected void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
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

    protected void disableBlocks(List<ScriptedBlockPlugin> plugins) throws Exception {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    protected void cancelSearch(String action) {
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().setActions(action).get();
        assertThat(listTasksResponse.getTasks(), hasSize(1));
        TaskInfo searchTask = listTasksResponse.getTasks().get(0);

        logger.info("Cancelling search");
        ListTasksResponse cancelTasksResponse = clusterAdmin().prepareCancelTasks().setTargetTaskId(searchTask.taskId()).get();
        assertThat(cancelTasksResponse.getTasks(), hasSize(1));
        assertThat(cancelTasksResponse.getTasks().get(0).taskId(), equalTo(searchTask.taskId()));
    }

    protected SearchResponse ensureSearchWasCancelled(ActionFuture<SearchResponse> searchResponse) {
        SearchResponse response = null;
        try {
            response = searchResponse.actionGet();
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
        } finally {
            if (response != null) response.decRef();
        }
    }

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        public static final String SEARCH_BLOCK_SCRIPT_NAME = "search_block";
        public static final String INIT_SCRIPT_NAME = "init";
        public static final String MAP_SCRIPT_NAME = "map";
        public static final String MAP_BLOCK_SCRIPT_NAME = "map_block";
        public static final String COMBINE_SCRIPT_NAME = "combine";
        static final String REDUCE_SCRIPT_NAME = "reduce";
        public static final String REDUCE_FAIL_SCRIPT_NAME = "reduce_fail";
        public static final String REDUCE_BLOCK_SCRIPT_NAME = "reduce_block";
        public static final String TERM_SCRIPT_NAME = "term";

        private final AtomicInteger hits = new AtomicInteger();

        private final Semaphore shouldBlock = new Semaphore(Integer.MAX_VALUE);

        private final AtomicReference<Runnable> beforeExecution = new AtomicReference<>();

        public void reset() {
            hits.set(0);
        }

        public void disableBlock() {
            shouldBlock.release(Integer.MAX_VALUE);
        }

        public void enableBlock() {
            try {
                shouldBlock.acquire(Integer.MAX_VALUE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
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

        public void logIfBlocked(String logMessage) {
            if (shouldBlock.tryAcquire(1) == false) {
                LogManager.getLogger(AbstractSearchCancellationTestCase.class).info(logMessage);
            } else {
                shouldBlock.release(1);
            }
        }

        public void waitForLock(int timeout, TimeUnit timeUnit) {
            try {
                assertTrue(shouldBlock.tryAcquire(timeout, timeUnit));
                shouldBlock.release(1);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private Object searchBlockScript(Map<String, Object> params) {
            final Runnable runnable = beforeExecution.get();
            if (runnable != null) {
                runnable.run();
            }
            LeafStoredFieldsLookup fieldsLookup = (LeafStoredFieldsLookup) params.get("_fields");
            LogManager.getLogger(AbstractSearchCancellationTestCase.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
            hits.incrementAndGet();
            waitForLock(10, TimeUnit.SECONDS);
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
            logIfBlocked("Blocking in reduce");
            hits.incrementAndGet();
            waitForLock(10, TimeUnit.SECONDS);
            return 42;
        }

        private Object mapBlockScript(Map<String, Object> params) {
            final Runnable runnable = beforeExecution.get();
            if (runnable != null) {
                runnable.run();
            }
            logIfBlocked("Blocking in map");
            hits.incrementAndGet();
            waitForLock(10, TimeUnit.SECONDS);
            return 1;
        }

        private Object termScript(Map<String, Object> params) {
            return 1;
        }
    }

    protected List<SearchShardBlockingPlugin> initSearchShardBlockingPlugin() {
        List<SearchShardBlockingPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            pluginsService.filterPlugins(SearchShardBlockingPlugin.class).forEach(plugins::add);
        }
        return plugins;
    }

    public static class SearchShardBlockingPlugin extends Plugin {
        private final AtomicReference<Consumer<ReaderContext>> runOnNewReaderContext = new AtomicReference<>();

        public void setRunOnNewReaderContext(Consumer<ReaderContext> consumer) {
            runOnNewReaderContext.set(consumer);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onNewReaderContext(ReaderContext c) {
                    if (runOnNewReaderContext.get() != null) {
                        runOnNewReaderContext.get().accept(c);
                    }
                }
            });
        }
    }
}
