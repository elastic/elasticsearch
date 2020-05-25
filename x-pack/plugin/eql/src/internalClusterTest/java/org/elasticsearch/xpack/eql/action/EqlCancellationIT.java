/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.junit.After;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class EqlCancellationIT extends AbstractEqlIntegTestCase {

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    /**
     * Shutdown the executor so we don't leak threads into other test runs.
     */
    @After
    public void shutdownExec() {
        executorService.shutdown();
    }

    public void testCancellation() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setMapping("val", "type=integer", "event_type", "type=keyword", "@timestamp", "type=date")
            .get());
        createIndex("idx_unmapped");

        int numDocs = randomIntBetween(6, 20);

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            int fieldValue = randomIntBetween(0, 10);
            builders.add(client().prepareIndex("test").setSource(
                jsonBuilder().startObject()
                    .field("val", fieldValue).field("event_type", "my_event").field("@timestamp", "2020-04-09T12:35:48Z")
                    .endObject()));
        }

        indexRandom(true, builders);
        boolean cancelDuringSearch = randomBoolean();
        List<SearchBlockPlugin> plugins = initBlockFactory(cancelDuringSearch, cancelDuringSearch == false);
        EqlSearchRequest request = new EqlSearchRequest().indices("test").query("my_event where val=1").eventCategoryField("event_type");
        String id = randomAlphaOfLength(10);
        logger.trace("Preparing search");
        // We might perform field caps on the same thread if it is local client, so we cannot use the standard mechanism
        Future<EqlSearchResponse> future = executorService.submit(() ->
            client().filterWithHeader(Collections.singletonMap(Task.X_OPAQUE_ID, id)).execute(EqlSearchAction.INSTANCE, request).get()
        );
        logger.trace("Waiting for block to be established");
        if (cancelDuringSearch) {
            awaitForBlockedSearches(plugins, "test");
        } else {
            awaitForBlockedFieldCaps(plugins);
        }
        logger.trace("Block is established");
        ListTasksResponse tasks = client().admin().cluster().prepareListTasks().setActions(EqlSearchAction.NAME).get();
        TaskId taskId = null;
        for (TaskInfo task : tasks.getTasks()) {
            if (id.equals(task.getHeaders().get(Task.X_OPAQUE_ID))) {
                taskId = task.getTaskId();
                break;
            }
        }
        assertNotNull(taskId);
        logger.trace("Cancelling task " + taskId);
        CancelTasksResponse response = client().admin().cluster().prepareCancelTasks().setTaskId(taskId).get();
        assertThat(response.getTasks(), hasSize(1));
        assertThat(response.getTasks().get(0).getAction(), equalTo(EqlSearchAction.NAME));
        logger.trace("Task is cancelled " + taskId);
        disableBlocks(plugins);
        Exception exception = expectThrows(Exception.class, future::get);
        Throwable inner = ExceptionsHelper.unwrap(exception, SearchPhaseExecutionException.class);
        if (cancelDuringSearch) {
            // Make sure we cancelled inside search
            assertNotNull(inner);
            assertThat(inner, instanceOf(SearchPhaseExecutionException.class));
            assertThat(inner.getCause(), instanceOf(TaskCancelledException.class));
        } else {
            // Make sure we were not cancelled inside search
            assertNull(inner);
            assertThat(getNumberOfContexts(plugins), equalTo(0));
            Throwable cancellationException = ExceptionsHelper.unwrap(exception, TaskCancelledException.class);
            assertNotNull(cancellationException);
        }
    }

    private List<SearchBlockPlugin> initBlockFactory(boolean searchBlock, boolean fieldCapsBlock) {
        List<SearchBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(SearchBlockPlugin.class));
        }
        for (SearchBlockPlugin plugin : plugins) {
            plugin.reset();
            if (searchBlock) {
                plugin.enableSearchBlock();
            }
            if (fieldCapsBlock) {
                plugin.enableFieldCapBlock();
            }
        }
        return plugins;
    }

    private void disableBlocks(List<SearchBlockPlugin> plugins) {
        for (SearchBlockPlugin plugin : plugins) {
            plugin.disableSearchBlock();
            plugin.disableFieldCapBlock();
        }
    }

    private void awaitForBlockedSearches(List<SearchBlockPlugin> plugins, String index) throws Exception {
        int numberOfShards = getNumShards(index).numPrimaries;
        assertBusy(() -> {
            int numberOfBlockedPlugins = getNumberOfContexts(plugins);
            logger.trace("The plugin blocked on {} out of {} shards", numberOfBlockedPlugins, numberOfShards);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        });
    }

    private int getNumberOfContexts(List<SearchBlockPlugin> plugins) throws Exception {
        int count = 0;
        for (SearchBlockPlugin plugin : plugins) {
            count += plugin.contexts.get();
        }
        return count;
    }

    private int getNumberOfFieldCaps(List<SearchBlockPlugin> plugins) throws Exception {
        int count = 0;
        for (SearchBlockPlugin plugin : plugins) {
            count += plugin.fieldCaps.get();
        }
        return count;
    }

    private void awaitForBlockedFieldCaps(List<SearchBlockPlugin> plugins) throws Exception {
        assertBusy(() -> {
            int numberOfBlockedPlugins = getNumberOfFieldCaps(plugins);
            logger.trace("The plugin blocked on {} nodes", numberOfBlockedPlugins);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        });
    }

    public static class SearchBlockPlugin extends LocalStateEQLXPackPlugin {
        protected final Logger logger = LogManager.getLogger(getClass());

        private final AtomicInteger contexts = new AtomicInteger();

        private final AtomicInteger fieldCaps = new AtomicInteger();

        private final AtomicBoolean shouldBlockOnSearch = new AtomicBoolean(false);

        private final AtomicBoolean shouldBlockOnFieldCapabilities = new AtomicBoolean(false);

        private final String nodeId;

        public void reset() {
            contexts.set(0);
            fieldCaps.set(0);
        }

        public void disableSearchBlock() {
            shouldBlockOnSearch.set(false);
        }

        public void enableSearchBlock() {
            shouldBlockOnSearch.set(true);
        }


        public void disableFieldCapBlock() {
            shouldBlockOnFieldCapabilities.set(false);
        }

        public void enableFieldCapBlock() {
            shouldBlockOnFieldCapabilities.set(true);
        }

        public SearchBlockPlugin(Settings settings, Path configPath) throws Exception {
            super(settings, configPath);
            nodeId = settings.get("node.name");
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onNewContext(SearchContext context) {
                    contexts.incrementAndGet();
                    try {
                        logger.trace("blocking search on " + nodeId);
                        assertBusy(() -> assertFalse(shouldBlockOnSearch.get()));
                        logger.trace("unblocking search on " + nodeId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            List<ActionFilter> list = new ArrayList<>(super.getActionFilters());
            list.add(new ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task, String action, Request request, ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain) {
                    if (action.equals(FieldCapabilitiesAction.NAME)) {
                        try {
                            fieldCaps.incrementAndGet();
                            logger.trace("blocking field caps on " + nodeId);
                            assertBusy(() -> assertFalse(shouldBlockOnFieldCapabilities.get()));
                            logger.trace("unblocking field caps on " + nodeId);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
            return list;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(SearchBlockPlugin.class);
    }

}
