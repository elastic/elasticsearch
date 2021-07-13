/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * IT tests that can block SQL execution at different places
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 0, numClientNodes = 0, maxNumDataNodes = 0)
public abstract class AbstractSqlBlockingIntegTestCase extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        return settings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateSQLXPackPlugin.class, SearchBlockPlugin.class);
    }

    protected List<SearchBlockPlugin> initBlockFactory(boolean searchBlock, boolean fieldCapsBlock) {
        List<SearchBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
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

    protected void disableBlocks(List<SearchBlockPlugin> plugins) {
        disableFieldCapBlocks(plugins);
        disableSearchBlocks(plugins);
    }

    protected void disableSearchBlocks(List<SearchBlockPlugin> plugins) {
        for (SearchBlockPlugin plugin : plugins) {
            plugin.disableSearchBlock();
        }
    }

    protected void disableFieldCapBlocks(List<SearchBlockPlugin> plugins) {
        for (SearchBlockPlugin plugin : plugins) {
            plugin.disableFieldCapBlock();
        }
    }

    protected void awaitForBlockedSearches(List<SearchBlockPlugin> plugins, String index) throws Exception {
        int numberOfShards = getNumShards(index).numPrimaries;
        assertBusy(() -> {
            int numberOfBlockedPlugins = getNumberOfContexts(plugins);
            logger.trace("The plugin blocked on {} out of {} shards", numberOfBlockedPlugins, numberOfShards);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        });
    }

    protected int getNumberOfContexts(List<SearchBlockPlugin> plugins) throws Exception {
        int count = 0;
        for (SearchBlockPlugin plugin : plugins) {
            count += plugin.contexts.get();
        }
        return count;
    }

    protected int getNumberOfFieldCaps(List<SearchBlockPlugin> plugins) throws Exception {
        int count = 0;
        for (SearchBlockPlugin plugin : plugins) {
            count += plugin.fieldCaps.get();
        }
        return count;
    }

    protected void awaitForBlockedFieldCaps(List<SearchBlockPlugin> plugins) throws Exception {
        assertBusy(() -> {
            int numberOfBlockedPlugins = getNumberOfFieldCaps(plugins);
            logger.trace("The plugin blocked on {} nodes", numberOfBlockedPlugins);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        });
    }

    public static class SearchBlockPlugin extends Plugin implements ActionPlugin {
        protected final Logger logger = LogManager.getLogger(getClass());

        private final AtomicInteger contexts = new AtomicInteger();

        private final AtomicInteger fieldCaps = new AtomicInteger();

        private final AtomicBoolean shouldBlockOnSearch = new AtomicBoolean(false);

        private final AtomicBoolean shouldBlockOnFieldCapabilities = new AtomicBoolean(false);

        private final String nodeId;

        private final ExecutorService executorService = Executors.newFixedThreadPool(1);

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
            nodeId = settings.get("node.name");
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onNewReaderContext(ReaderContext readerContext) {
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
            List<ActionFilter> list = new ArrayList<>();
            list.add(new ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain) {

                    if (action.equals(FieldCapabilitiesAction.NAME)) {
                        final Consumer<Response> actionWrapper = resp -> {
                            try {
                                fieldCaps.incrementAndGet();
                                logger.trace("blocking field caps on " + nodeId);
                                assertBusy(() -> assertFalse(shouldBlockOnFieldCapabilities.get()));
                                logger.trace("unblocking field caps on " + nodeId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                listener.onResponse(resp);
                            }
                            logger.trace("unblocking field caps on " + nodeId);
                        };
                        chain.proceed(task, action, request,
                            ActionListener.wrap(resp -> executorService.execute(() -> actionWrapper.accept(resp)), listener::onFailure)
                        );
                    } else {
                        chain.proceed(task, action, request, listener);
                    }
                }
            });
            return list;
        }

        @Override
        public void close() throws IOException {
            List<Runnable> runnables = executorService.shutdownNow();
            assertTrue(runnables.isEmpty());
        }
    }

    protected TaskId findTaskWithXOpaqueId(String id, String action) {
        TaskInfo taskInfo = getTaskInfoWithXOpaqueId(id, action);
        if (taskInfo != null) {
            return taskInfo.getTaskId();
        } else {
             return null;
        }
    }

    protected TaskInfo getTaskInfoWithXOpaqueId(String id, String action) {
        ListTasksResponse tasks = client().admin().cluster().prepareListTasks().setActions(action).get();
        for (TaskInfo task : tasks.getTasks()) {
            if (id.equals(task.getHeaders().get(Task.X_OPAQUE_ID))) {
                return task;
            }
        }
        return null;
    }

    protected TaskId cancelTaskWithXOpaqueId(String id, String action) {
        TaskId taskId = findTaskWithXOpaqueId(id, action);
        assertNotNull(taskId);
        logger.trace("Cancelling task " + taskId);
        CancelTasksResponse response = client().admin().cluster().prepareCancelTasks().setTaskId(taskId).get();
        assertThat(response.getTasks(), hasSize(1));
        assertThat(response.getTasks().get(0).getAction(), equalTo(action));
        logger.trace("Task is cancelled " + taskId);
        return taskId;
    }

}
