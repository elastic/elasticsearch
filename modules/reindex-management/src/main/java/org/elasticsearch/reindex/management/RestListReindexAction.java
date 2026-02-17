/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.reindex.management.ReindexManagementPlugin.CAPABILITY_REINDEX_MANAGEMENT_API;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.Scope.PUBLIC;

/**
 * REST handler for listing all running reindex tasks.
 */
@ServerlessScope(PUBLIC)
public class RestListReindexAction extends BaseRestHandler {

    private final Predicate<NodeFeature> clusterSupportsFeature;

    RestListReindexAction(final Predicate<NodeFeature> clusterSupportsFeature) {
        this.clusterSupportsFeature = Objects.requireNonNull(clusterSupportsFeature);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_reindex"));
    }

    @Override
    public String getName() {
        return "list_reindex_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (clusterSupportsFeature.test(ReindexManagementFeatures.NEW_ENDPOINTS) == false) {
            throw new IllegalArgumentException("endpoint not supported on all nodes in the cluster");
        }

        boolean detailed = request.paramAsBoolean("detailed", false);
        ListReindexRequest listReindexRequest = new ListReindexRequest().setActions(ReindexAction.NAME).setDetailed(detailed);
        return channel -> client.execute(TransportListReindexAction.TYPE, listReindexRequest, new RestChunkedToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(CAPABILITY_REINDEX_MANAGEMENT_API);
    }
}
