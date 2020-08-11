/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.metrics.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xpack.repositories.metrics.action.RepositoriesMetricsRequest;
import org.elasticsearch.xpack.repositories.metrics.action.RepositoriesMetricsAction;

import java.util.List;

public final class RestGetRepositoriesMetricsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_repositories_metrics_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_nodes/{nodeId}/_repositories_metrics"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        RepositoriesMetricsRequest repositoriesMetricsRequest = new RepositoriesMetricsRequest(nodesIds);
        return channel -> client.execute(
            RepositoriesMetricsAction.INSTANCE,
            repositoriesMetricsRequest,
            new RestActions.NodesResponseRestListener<>(channel)
        );
    }
}
