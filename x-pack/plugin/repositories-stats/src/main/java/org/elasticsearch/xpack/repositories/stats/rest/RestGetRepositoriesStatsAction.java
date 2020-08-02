/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xpack.repositories.stats.action.RepositoriesStatsRequest;
import org.elasticsearch.xpack.repositories.stats.action.RepositoriesStatsAction;

import java.util.List;

public final class RestGetRepositoriesStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_repositories_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_nodes/{nodeId}/_repositories_stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        RepositoriesStatsRequest repositoriesStatsRequest = new RepositoriesStatsRequest(nodesIds);
        return channel -> client.execute(
            RepositoriesStatsAction.INSTANCE,
            repositoriesStatsRequest,
            new RestActions.NodesResponseRestListener<>(channel)
        );
    }
}
