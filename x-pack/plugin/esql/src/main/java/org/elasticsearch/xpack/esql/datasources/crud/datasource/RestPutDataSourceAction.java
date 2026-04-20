/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.crud.datasource;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutDataSourceAction extends BaseRestHandler {
    private static final String DATA_SOURCE_MANAGEMENT = "data_source_management";

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
            final Map<String, Object> body = parser.map();
            final Object typeValue = body.get("type");
            final Object descriptionValue = body.get("description");
            @SuppressWarnings("unchecked")
            final Map<String, Object> rawSettings = (Map<String, Object>) body.get("settings");
            final String type = typeValue == null ? null : typeValue.toString();
            final String description = descriptionValue == null ? null : descriptionValue.toString();
            final PutDataSourceAction.Request req = new PutDataSourceAction.Request(
                RestUtils.getMasterNodeTimeout(request),
                RestUtils.getAckTimeout(request),
                name,
                type,
                description,
                rawSettings
            );
            return channel -> client.execute(PutDataSourceAction.INSTANCE, req, new RestToXContentListener<>(channel));
        }
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(DATA_SOURCE_MANAGEMENT);
    }
}
