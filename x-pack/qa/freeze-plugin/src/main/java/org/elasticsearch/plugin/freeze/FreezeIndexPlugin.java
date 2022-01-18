/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.plugin.freeze;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Restores the REST endpoint for freezing indices so that the JDBC tests can still freeze indices
 * for testing purposes until frozen indices are no longer supported.
 */
public class FreezeIndexPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new FreezeIndexRestEndpoint());
    }

    /**
     * Used by the {@link FreezeIndexPlugin} above.
     */
    static class FreezeIndexRestEndpoint extends BaseRestHandler {
        @Override
        public String getName() {
            return "freeze-for-testing-only";
        }

        @Override
        public List<Route> routes() {
            return List.of(new Route(POST, "/{index}/_freeze"));
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            boolean freeze = request.path().endsWith("/_freeze");
            FreezeRequest freezeRequest = new FreezeRequest(Strings.splitStringByCommaToArray(request.param("index")));
            freezeRequest.timeout(request.paramAsTime("timeout", freezeRequest.timeout()));
            freezeRequest.masterNodeTimeout(request.paramAsTime("master_timeout", freezeRequest.masterNodeTimeout()));
            freezeRequest.indicesOptions(IndicesOptions.fromRequest(request, freezeRequest.indicesOptions()));
            String waitForActiveShards = request.param("wait_for_active_shards");
            if (waitForActiveShards != null) {
                freezeRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
            }
            freezeRequest.setFreeze(freeze);
            return channel -> client.execute(FreezeIndexAction.INSTANCE, freezeRequest, new RestToXContentListener<>(channel));
        }
    }

}
