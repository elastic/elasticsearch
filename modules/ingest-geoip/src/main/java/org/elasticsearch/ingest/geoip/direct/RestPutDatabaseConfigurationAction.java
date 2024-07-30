/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.ingest.geoip.direct.PutDatabaseConfigurationAction.Request;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestPutDatabaseConfigurationAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_ingest/geoip/database/{id}"));
    }

    @Override
    public String getName() {
        return "geoip_put_database_configuration";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final Request req;
        try (var parser = request.contentParser()) {
            req = PutDatabaseConfigurationAction.Request.parseRequest(
                getMasterNodeTimeout(request),
                getAckTimeout(request),
                request.param("id"),
                parser
            );
        }
        return channel -> client.execute(PutDatabaseConfigurationAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }
}
