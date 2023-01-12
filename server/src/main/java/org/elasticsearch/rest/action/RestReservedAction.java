/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.OPTIONS;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * The {@link RestReservedAction} registers a set of paths that should be
 * considered "reserved" and Elasticsearch should never expose, because they
 * may be used elsewhere or at a later time. See {@link #RESERVED_PATHS} for
 * the list of reserved URIs.
 */
public class RestReservedAction extends BaseRestHandler {

    // This is the set of reserved URIs
    private static final Set<String> RESERVED_PATHS = Set.of("/__elb_health__", "/__elb_health__/zk", "/_health", "/_health/zk");
    private static final Set<RestRequest.Method> ALL_METHODS = Set.of(GET, PUT, POST, DELETE, OPTIONS);

    @Override
    public List<Route> routes() {
        return RESERVED_PATHS.stream().flatMap(path -> ALL_METHODS.stream().map(method -> new Route(method, path))).toList();
    }

    @Override
    public String getName() {
        return "reserved";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        // Mimic that this is a bad request. This reserved handler does not
        // actually serve the request, it just keeps any other ES Engineer from
        // registering these routes/paths by accident.
        return channel -> RestController.handleBadRequest(restRequest.uri(), restRequest.method(), channel);
    }
}
