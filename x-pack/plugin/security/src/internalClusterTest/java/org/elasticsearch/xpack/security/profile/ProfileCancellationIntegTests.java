/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_USER_NAME;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class ProfileCancellationIntegTests extends AbstractProfileIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SearchBlockPlugin.class);
        return List.copyOf(plugins);
    }

    public void testSuggestProfilesCancellation() throws Exception {
        doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);

        final String xOpaqueId = randomAlphaOfLength(10);
        final Request request = new Request("GET", "/_security/profile/_suggest");
        RequestOptions.Builder options = request.getOptions()
            .toBuilder()
            .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING))
            .addHeader(Task.X_OPAQUE_ID_HTTP_HEADER, xOpaqueId);
        request.setOptions(options);

        // Stall the search
        enableSearchBlock();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> error = new AtomicReference<>();
        final Cancellable cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                error.set(exception);
                latch.countDown();
            }
        });

        // Assert that suggest task and search sub-tasks are initiated
        final Set<Long> taskIds = ConcurrentHashMap.newKeySet();
        assertBusy(() -> {
            final List<Task> tasks = getTasksForXOpaqueId(xOpaqueId);
            final List<String> taskActions = tasks.stream().map(Task::getAction).toList();
            assertThat(
                taskActions,
                hasItems(equalTo(SuggestProfilesAction.NAME), equalTo(SearchAction.NAME), startsWith(SearchAction.NAME))
            );
            tasks.forEach(t -> taskIds.add(t.getId()));
        }, 20, TimeUnit.SECONDS);

        // Cancel the suggest request and all tasks should be cancelled
        cancellable.cancel();
        assertBusy(() -> {
            final List<CancellableTask> cancellableTasks = getCancellableTasksForXOpaqueId(xOpaqueId);
            cancellableTasks.forEach(cancellableTask -> {
                assertThat(
                    "task " + cancellableTask.getId() + "/" + cancellableTask.getAction() + " not cancelled",
                    cancellableTask.isCancelled(),
                    is(true)
                );
                taskIds.remove(cancellableTask.getId());
            });
            assertThat(taskIds, empty());
        }, 20, TimeUnit.SECONDS);

        disableSearchBlock();
        latch.await();
        assertThat(error.get(), instanceOf(CancellationException.class));
    }

    private List<Task> getTasksForXOpaqueId(String xOpaqueId) {
        final ArrayList<Task> tasks = new ArrayList<>();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            tasks.addAll(
                transportService.getTaskManager()
                    .getTasks()
                    .values()
                    .stream()
                    .filter(t -> xOpaqueId.equals(t.headers().get(Task.X_OPAQUE_ID_HTTP_HEADER)))
                    .toList()
            );
        }
        return tasks;
    }

    private List<CancellableTask> getCancellableTasksForXOpaqueId(String xOpaqueId) {
        final ArrayList<CancellableTask> cancellableTasks = new ArrayList<>();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            cancellableTasks.addAll(
                transportService.getTaskManager()
                    .getCancellableTasks()
                    .values()
                    .stream()
                    .filter(t -> xOpaqueId.equals(t.headers().get(Task.X_OPAQUE_ID_HTTP_HEADER)))
                    .toList()
            );
        }
        return cancellableTasks;
    }

    private void enableSearchBlock() {
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            pluginsService.filterPlugins(SearchBlockPlugin.class).forEach(SearchBlockPlugin::enableSearchBlock);
        }
    }

    private void disableSearchBlock() {
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            pluginsService.filterPlugins(SearchBlockPlugin.class).forEach(SearchBlockPlugin::disableSearchBlock);
        }
    }

    public static class SearchBlockPlugin extends Plugin implements ActionPlugin {
        protected static final Logger logger = LogManager.getLogger(SearchBlockPlugin.class);

        private final String nodeId;
        private final AtomicBoolean shouldBlockOnSearch = new AtomicBoolean(false);

        public SearchBlockPlugin(Settings settings, Path configPath) throws Exception {
            nodeId = settings.get("node.name");
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onNewReaderContext(ReaderContext readerContext) {
                    try {
                        logger.info("blocking search on " + nodeId);
                        assertBusy(() -> assertFalse(shouldBlockOnSearch.get()));
                        logger.info("unblocking search on " + nodeId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        void enableSearchBlock() {
            shouldBlockOnSearch.set(true);
        }

        void disableSearchBlock() {
            shouldBlockOnSearch.set(false);
        }
    }
}
