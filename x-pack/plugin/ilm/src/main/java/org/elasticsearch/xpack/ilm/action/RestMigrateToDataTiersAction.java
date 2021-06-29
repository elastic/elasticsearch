/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersAction;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMigrateToDataTiersAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "migrate_to_data_tiers_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_ilm/migrate_to_data_tiers"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        MigrateToDataTiersRequest migrateRequest = request.hasContent() ?
            MigrateToDataTiersRequest.parse(request.contentParser()) : new MigrateToDataTiersRequest();
        migrateRequest.setDryRun(request.paramAsBoolean("dry_run", false));
        return channel -> client.execute(MigrateToDataTiersAction.INSTANCE, migrateRequest, new RestToXContentListener<>(channel));
    }
}
