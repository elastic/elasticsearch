/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.async;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class AsyncResultsIndexPlugin extends Plugin implements SystemIndexPlugin {

    protected final Settings settings;

    public AsyncResultsIndexPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.singletonList(new SystemIndexDescriptor(XPackPlugin.ASYNC_RESULTS_INDEX, this.getClass().getSimpleName()));
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        List<Object> components = new ArrayList<>();
        if (DiscoveryNode.isDataNode(environment.settings())) {
            // only data nodes should be eligible to run the maintenance service.
            AsyncTaskIndexService<AsyncSearchResponse> indexService = new AsyncTaskIndexService<>(
                XPackPlugin.ASYNC_RESULTS_INDEX,
                clusterService,
                threadPool.getThreadContext(),
                client,
                ASYNC_SEARCH_ORIGIN,
                AsyncSearchResponse::new,
                namedWriteableRegistry
            );
            AsyncTaskMaintenanceService maintenanceService = new AsyncTaskMaintenanceService(
                clusterService,
                nodeEnvironment.nodeId(),
                settings,
                threadPool,
                indexService
            );
            components.add(maintenanceService);
        }
        return components;
    }
}
