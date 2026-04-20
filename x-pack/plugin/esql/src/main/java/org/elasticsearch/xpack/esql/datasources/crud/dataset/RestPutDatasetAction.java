/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.crud.dataset;

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

public class RestPutDatasetAction extends BaseRestHandler {
    private static final String DATASET_MANAGEMENT = "dataset_management";

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_query/dataset/{name}"));
    }

    @Override
    public String getName() {
        return "esql_put_dataset";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String name = request.param("name");
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            final Map<String, Object> body = parser.map();
            final Object dataSourceValue = body.get("data_source");
            final Object resourceValue = body.get("resource");
            final Object descriptionValue = body.get("description");
            @SuppressWarnings("unchecked")
            final Map<String, Object> rawSettings = (Map<String, Object>) body.get("settings");
            final String dataSource = dataSourceValue == null ? null : dataSourceValue.toString();
            final String resource = resourceValue == null ? null : resourceValue.toString();
            final String description = descriptionValue == null ? null : descriptionValue.toString();
            final PutDatasetAction.Request req = new PutDatasetAction.Request(
                RestUtils.getMasterNodeTimeout(request),
                RestUtils.getAckTimeout(request),
                name,
                dataSource,
                resource,
                description,
                rawSettings
            );
            return channel -> client.execute(PutDatasetAction.INSTANCE, req, new RestToXContentListener<>(channel));
        }
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(DATASET_MANAGEMENT);
    }
}
