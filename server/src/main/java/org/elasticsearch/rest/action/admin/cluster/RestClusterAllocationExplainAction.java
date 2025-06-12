/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Class handling cluster allocation explanation at the REST level
 */
@ServerlessScope(Scope.INTERNAL)
public class RestClusterAllocationExplainAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cluster/allocation/explain"), new Route(POST, "/_cluster/allocation/explain"));
    }

    @Override
    public String getName() {
        return "cluster_allocation_explain_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    /*
        The Cluster Allocation Explain API supports both path parameters and parameters passed through the request body, but not both.
        The API also supports empty requests, which translates to "explain the first unassigned shard you find"
     */
    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final var clusterAllocationExplainRequest = new ClusterAllocationExplainRequest(RestUtils.getMasterNodeTimeout(request));

        // A request body was passed
        if (request.hasContentOrSourceParam()) {
            // Supplying parameters alongside a request body is unsupported
            if (hasAnyParameterBeenPassed(request)) {
                throw new IllegalArgumentException("Parameters cannot be passed in both the URL and the request body");
            }

            try (XContentParser parser = request.contentOrSourceParamParser()) {
                ClusterAllocationExplainRequest.parse(clusterAllocationExplainRequest, parser);
            }
        }
        // There is no request body. Check for optionally supplied path parameters
        else {
            clusterAllocationExplainRequest.setIndex(
                request.param(
                    ClusterAllocationExplainRequest.INDEX_PARAMETER_NAME,
                    // Defaults to the existing value, which was instantiated as null
                    clusterAllocationExplainRequest.getIndex()
                )
            );

            clusterAllocationExplainRequest.setShard(
                request.paramAsInt(ClusterAllocationExplainRequest.SHARD_PARAMETER_NAME, clusterAllocationExplainRequest.getShard())
            );

            clusterAllocationExplainRequest.setPrimary(
                request.paramAsBoolean(ClusterAllocationExplainRequest.PRIMARY_PARAMETER_NAME, clusterAllocationExplainRequest.isPrimary())
            );

            clusterAllocationExplainRequest.setCurrentNode(
                request.param(ClusterAllocationExplainRequest.CURRENT_NODE_PARAMETER_NAME, clusterAllocationExplainRequest.getCurrentNode())
            );
        }

        // This checks for optionally provided query parameters
        clusterAllocationExplainRequest.includeYesDecisions(
            request.paramAsBoolean(ClusterAllocationExplainRequest.INCLUDE_YES_DECISIONS_PARAMETER_NAME, false)
        );
        clusterAllocationExplainRequest.includeDiskInfo(
            request.paramAsBoolean(ClusterAllocationExplainRequest.INCLUDE_DISK_INFO_PARAMETER_NAME, false)
        );

        return channel -> client.execute(
            TransportClusterAllocationExplainAction.TYPE,
            clusterAllocationExplainRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }

    private boolean hasAnyParameterBeenPassed(RestRequest request) {
        for (String parameter : ClusterAllocationExplainRequest.PATH_PARAMETERS) {
            if (request.hasParam(parameter)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
