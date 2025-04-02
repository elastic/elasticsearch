/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestEsqlListQueriesAction extends BaseRestHandler {
    private static final Logger LOGGER = LogManager.getLogger(RestEsqlListQueriesAction.class);

    @Override
    public String getName() {
        return "esql_list_queries";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_query/queries/{id}"), new Route(GET, "/_query/queries"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return restChannelConsumer(request, client);
    }

    private static RestChannelConsumer restChannelConsumer(RestRequest request, NodeClient client) {
        LOGGER.debug("Beginning execution of ESQL list queries.");

        String id = request.param("id");
        var action = id != null ? EsqlGetQueryAction.INSTANCE : EsqlListQueriesAction.INSTANCE;
        var actionRequest = id != null ? new EsqlGetQueryRequest(new TaskId(id)) : new EsqlListQueriesRequest();

        return channel -> client.execute(action, actionRequest, new RestToXContentListener<>(channel));
    }
}
