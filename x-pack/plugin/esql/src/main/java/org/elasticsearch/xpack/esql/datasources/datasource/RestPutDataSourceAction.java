/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutDataSourceAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_query/data_source/{name}"));
    }

    @Override
    public String getName() {
        return "esql_put_data_source";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String name = request.param("name");
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            PutDataSourceAction.Request req = PutDataSourceAction.Request.fromXContent(
                parser,
                RestUtils.getMasterNodeTimeout(request),
                RestUtils.getAckTimeout(request),
                name
            );
            return channel -> client.execute(PutDataSourceAction.INSTANCE, req, new RestToXContentListener<>(channel));
        }
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(EsqlDataSourcesCapabilities.DATA_SOURCES);
    }
}
