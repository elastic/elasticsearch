/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService.ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING;

/**
 * Plugin for asynchronous search functionality in Elasticsearch.
 * <p>
 * This plugin enables long-running search requests to execute asynchronously,
 * allowing clients to submit a search, disconnect, and retrieve results later.
 * This is particularly useful for searches that may take a long time to complete.
 * </p>
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * POST /my-index/_async_search?wait_for_completion_timeout=2s
 * {
 *   "query": {
 *     "match_all": {}
 *   }
 * }
 *
 * // Returns an ID to retrieve results later
 * GET /_async_search/<id>
 *
 * // Check status without retrieving results
 * GET /_async_search/status/<id>
 *
 * // Delete the async search
 * DELETE /_async_search/<id>
 * }</pre>
 */
public final class AsyncSearch extends Plugin implements ActionPlugin {

    /**
     * Returns the list of action handlers provided by this plugin.
     * <p>
     * Registers transport actions for submitting async searches, retrieving results,
     * and checking search status.
     * </p>
     *
     * @return a list of action handlers for async search operations
     */
    @Override
    public List<ActionHandler> getActions() {
        return Arrays.asList(
            new ActionHandler(SubmitAsyncSearchAction.INSTANCE, TransportSubmitAsyncSearchAction.class),
            new ActionHandler(GetAsyncSearchAction.INSTANCE, TransportGetAsyncSearchAction.class),
            new ActionHandler(GetAsyncStatusAction.INSTANCE, TransportGetAsyncStatusAction.class)
        );
    }

    /**
     * Returns the REST handlers provided by this plugin.
     * <p>
     * Registers REST endpoints for async search operations including submit, get,
     * status, and delete operations.
     * </p>
     *
     * @param settings the node settings
     * @param namedWriteableRegistry the named writeable registry
     * @param restController the REST controller
     * @param clusterSettings the cluster settings
     * @param indexScopedSettings the index-scoped settings
     * @param settingsFilter the settings filter
     * @param indexNameExpressionResolver the index name expression resolver
     * @param nodesInCluster supplier for discovery nodes
     * @param clusterSupportsFeature predicate to check feature support
     * @return a list of REST handlers for async search endpoints
     */
    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return Arrays.asList(
            new RestSubmitAsyncSearchAction(restController.getSearchUsageHolder(), clusterSupportsFeature, settings),
            new RestGetAsyncSearchAction(),
            new RestGetAsyncStatusAction(),
            new RestDeleteAsyncSearchAction()
        );
    }

    /**
     * Returns the list of settings provided by this plugin.
     *
     * @return a list containing the async search cleanup interval setting
     */
    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING);
    }
}
