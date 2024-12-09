/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetMigrationReindexStatusAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_migration_reindex_status_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_migration/reindex/{index}/_status"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        GetMigrationReindexStatusAction.Request getTaskRequest = new GetMigrationReindexStatusAction.Request(index);
        return channel -> client.execute(GetMigrationReindexStatusAction.INSTANCE, getTaskRequest, new RestToXContentListener<>(channel));
    }
}
