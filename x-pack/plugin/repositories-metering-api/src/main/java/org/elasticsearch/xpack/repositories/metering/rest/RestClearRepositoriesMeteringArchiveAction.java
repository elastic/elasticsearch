/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.repositories.metering.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xpack.repositories.metering.action.ClearRepositoriesMeteringArchiveAction;
import org.elasticsearch.xpack.repositories.metering.action.ClearRepositoriesMeteringArchiveRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.INTERNAL)
public class RestClearRepositoriesMeteringArchiveAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "clear_repositories_metrics_archive_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_nodes/{nodeId}/_repositories_metering/{maxVersionToClear}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        long maxVersionToClear = request.paramAsLong("maxVersionToClear", -1);
        ClearRepositoriesMeteringArchiveRequest clearArchivesRequest = new ClearRepositoriesMeteringArchiveRequest(
            maxVersionToClear,
            nodesIds
        );
        return channel -> client.execute(
            ClearRepositoriesMeteringArchiveAction.INSTANCE,
            clearArchivesRequest,
            new RestActions.NodesResponseRestListener<>(channel)
        );
    }
}
