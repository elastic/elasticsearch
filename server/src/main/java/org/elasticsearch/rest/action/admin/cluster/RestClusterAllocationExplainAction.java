/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
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

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClusterAllocationExplainRequest req;
        if (request.hasContentOrSourceParam() == false) {
            // Empty request signals "explain the first unassigned shard you find"
            req = new ClusterAllocationExplainRequest();
        } else {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                req = ClusterAllocationExplainRequest.parse(parser);
            }
        }

        req.includeYesDecisions(request.paramAsBoolean("include_yes_decisions", false));
        req.includeDiskInfo(request.paramAsBoolean("include_disk_info", false));
        return channel -> client.admin().cluster().allocationExplain(req, new RestRefCountedChunkedToXContentListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
