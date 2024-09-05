/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.capabilities.NodesCapabilitiesRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestUtils.getTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestNodesCapabilitiesAction extends BaseRestHandler {

    public static final NodeFeature CAPABILITIES_ACTION = new NodeFeature("rest.capabilities_action");
    public static final NodeFeature LOCAL_ONLY_CAPABILITIES = new NodeFeature("rest.local_only_capabilities");

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_capabilities"));
    }

    @Override
    public Set<String> supportedQueryParameters() {
        return Set.of("timeout", "method", "path", "parameters", "capabilities", "local_only");
    }

    @Override
    public String getName() {
        return "nodes_capabilities_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        NodesCapabilitiesRequest requestNodes = request.paramAsBoolean("local_only", false)
            ? new NodesCapabilitiesRequest(client.getLocalNodeId())
            : new NodesCapabilitiesRequest();

        NodesCapabilitiesRequest r = requestNodes.timeout(getTimeout(request))
            .method(RestRequest.Method.valueOf(request.param("method", "GET")))
            .path(URLDecoder.decode(request.param("path"), StandardCharsets.UTF_8))
            .parameters(request.paramAsStringArray("parameters", Strings.EMPTY_ARRAY))
            .capabilities(request.paramAsStringArray("capabilities", Strings.EMPTY_ARRAY))
            .restApiVersion(request.getRestApiVersion());

        return channel -> client.admin().cluster().nodesCapabilities(r, new NodesResponseRestListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
