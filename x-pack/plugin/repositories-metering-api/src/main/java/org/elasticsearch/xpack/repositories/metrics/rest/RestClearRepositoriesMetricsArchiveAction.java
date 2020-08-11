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
import org.elasticsearch.xpack.repositories.metrics.action.ClearRepositoriesMetricsArchiveAction;
import org.elasticsearch.xpack.repositories.metrics.action.ClearRepositoriesMetricsArchiveRequest;

import java.util.List;

public class RestClearRepositoriesMetricsArchiveAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "clear_repositories_metrics_archive_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.DELETE, "/_nodes/{nodeId}/_repositories_metrics/{maxVersionToClear}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        long maxVersionToClear = request.paramAsLong("maxVersionToClear", -1);
        ClearRepositoriesMetricsArchiveRequest clearArchivesRequest = new ClearRepositoriesMetricsArchiveRequest(maxVersionToClear, nodesIds);
        return channel -> client.execute(
            ClearRepositoriesMetricsArchiveAction.INSTANCE,
            clearArchivesRequest,
            new RestActions.NodesResponseRestListener<>(channel)
        );
    }
}
