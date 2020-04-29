/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.search.AsyncSearchMaintenanceService.ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING;

public final class AsyncSearch extends Plugin implements ActionPlugin {
    public static final String INDEX = ".async-search";
    private final Settings settings;

    public AsyncSearch(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(SubmitAsyncSearchAction.INSTANCE, TransportSubmitAsyncSearchAction.class),
            new ActionHandler<>(GetAsyncSearchAction.INSTANCE, TransportGetAsyncSearchAction.class),
            new ActionHandler<>(DeleteAsyncSearchAction.INSTANCE, TransportDeleteAsyncSearchAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
            new RestSubmitAsyncSearchAction(),
            new RestGetAsyncSearchAction(),
            new RestDeleteAsyncSearchAction()
        );
    }

    @Override
    public Collection<Object> createComponents(Client client,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService,
                                               ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry,
                                               Environment environment,
                                               NodeEnvironment nodeEnvironment,
                                               NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        if (DiscoveryNode.isDataNode(environment.settings())) {
            // only data nodes should be eligible to run the maintenance service.
            AsyncTaskIndexService<AsyncSearchResponse> indexService =
                new AsyncTaskIndexService<>(AsyncSearch.INDEX, clusterService, threadPool.getThreadContext(), client, ASYNC_SEARCH_ORIGIN,
                    AsyncSearchResponse::new, namedWriteableRegistry);
            AsyncSearchMaintenanceService maintenanceService =
                new AsyncSearchMaintenanceService(clusterService, nodeEnvironment.nodeId(), settings, threadPool, indexService);
            return Collections.singletonList(maintenanceService);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING);
    }
}
