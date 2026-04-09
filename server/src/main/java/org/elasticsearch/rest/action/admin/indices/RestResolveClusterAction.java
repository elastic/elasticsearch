/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.resolve.ResolveClusterActionRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.resolve.TransportResolveClusterAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestResolveClusterAction extends BaseRestHandler {

    private static Set<String> INDEX_OPTIONS_PARAMS = Set.of(
        "expand_wildcards",
        "ignore_unavailable",
        "allow_no_indices",
        "ignore_throttled"
    );

    @Override
    public String getName() {
        return "resolve_cluster_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_resolve/cluster"), new Route(GET, "/_resolve/cluster/{name}"));
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] indexExpressions;
        boolean clusterInfoOnly;
        if (request.hasParam("name")) {
            indexExpressions = Strings.splitStringByCommaToArray(request.param("name"));
            clusterInfoOnly = false;
        } else {
            indexExpressions = new String[0];
            clusterInfoOnly = true;
            Set<String> indexOptions = requestIndexOptionsParams(request);
            if (indexOptions.isEmpty() == false) {
                // this restriction avoids problems with having to send wildcarded index expressions to older clusters
                // when no index expression is provided by the user
                throw new IllegalArgumentException(
                    "No index options are allowed on _resolve/cluster when no index expression is specified, but received: " + indexOptions
                );
            }
        }
        ResolveClusterActionRequest resolveRequest = new ResolveClusterActionRequest(
            indexExpressions,
            IndicesOptions.fromRequest(request, ResolveIndexAction.Request.DEFAULT_INDICES_OPTIONS),
            clusterInfoOnly,
            true
        );

        String timeout = request.param("timeout");
        if (timeout != null) {
            resolveRequest.setTimeout(TimeValue.parseTimeValue(timeout, "timeout"));
        }

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .execute(TransportResolveClusterAction.TYPE, resolveRequest, new RestToXContentListener<>(channel));
    }

    private static Set<String> requestIndexOptionsParams(RestRequest request) {
        return Sets.intersection(request.params().keySet(), INDEX_OPTIONS_PARAMS);
    }
}
