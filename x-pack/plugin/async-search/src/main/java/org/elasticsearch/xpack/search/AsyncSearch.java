/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ShutdownAwarePlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService.ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING;

public final class AsyncSearch extends Plugin implements ActionPlugin, ShutdownAwarePlugin {

    @Override
    public List<ActionHandler> getActions() {
        return Arrays.asList(
            new ActionHandler(SubmitAsyncSearchAction.INSTANCE, TransportSubmitAsyncSearchAction.class),
            new ActionHandler(GetAsyncSearchAction.INSTANCE, TransportGetAsyncSearchAction.class),
            new ActionHandler(GetAsyncStatusAction.INSTANCE, TransportGetAsyncStatusAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        RestController restController = restHandlersServices.restController();
        return Arrays.asList(
            new RestSubmitAsyncSearchAction(
                restController.getSearchUsageHolder(),
                clusterSupportsFeature,
                restHandlersServices.crossProjectModeDecider()
            ),
            new RestGetAsyncSearchAction(),
            new RestGetAsyncStatusAction(),
            new RestDeleteAsyncSearchAction()
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING);
    }

    @Override
    public boolean safeToShutdown(String nodeId, SingleNodeShutdownMetadata.Type shutdownType) {
        return true;
    }

    @Override
    public void signalShutdown(Collection<String> shutdownNodeIds) {}

    /**
     * Graceful shutdown waits for the async search tasks: they outlive the HTTP request that submitted them and only complete once
     * the result is stored. The task of an async search is registered under the name of the search action it runs as, see
     * {@link TransportSubmitAsyncSearchAction}, so it is this name that is awaited; the synchronous searches registered under it too
     * are bound to their HTTP request, which the HTTP server's shutdown already waits for.
     */
    @Override
    public Collection<String> getActionsToAwaitOnShutdown() {
        return List.of(TransportSearchAction.NAME);
    }
}
