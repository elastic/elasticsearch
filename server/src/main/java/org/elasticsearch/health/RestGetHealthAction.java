/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public class RestGetHealthAction extends BaseRestHandler {

    private static final String VERBOSE_PARAM = "verbose";

    private static final String SIZE_PARAM = "size";

    private static final String CAPABILITY_MULTI_PROJECT_SHARDS_AVAILABILITY = "multi_project_shards_availability";

    @Override
    public String getName() {
        // TODO: Existing - "cluster_health_action", "cat_health_action"
        return "health_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_health_report"), new Route(GET, "/_health_report/{indicator}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indicatorName = request.param("indicator");
        boolean verbose = request.paramAsBoolean(VERBOSE_PARAM, true);
        int size = request.paramAsInt(SIZE_PARAM, 1000);
        GetHealthAction.Request getHealthRequest = new GetHealthAction.Request(indicatorName, verbose, size);
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            GetHealthAction.INSTANCE,
            getHealthRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Sets.union(Set.of(CAPABILITY_MULTI_PROJECT_SHARDS_AVAILABILITY), super.supportedCapabilities());
    }
}
