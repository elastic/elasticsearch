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
import org.elasticsearch.xpack.repositories.metering.action.RepositoriesMeteringAction;
import org.elasticsearch.xpack.repositories.metering.action.RepositoriesMeteringRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public final class RestGetRepositoriesMeteringAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_repositories_metering_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_nodes/{nodeId}/_repositories_metering"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        RepositoriesMeteringRequest repositoriesMeteringRequest = new RepositoriesMeteringRequest(nodesIds);
        return channel -> client.execute(
            RepositoriesMeteringAction.INSTANCE,
            repositoriesMeteringRequest,
            new RestActions.NodesResponseRestListener<>(channel)
        );
    }
}
