/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dataset;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteDatasetAction extends BaseRestHandler {
    private static final String DATASET_MANAGEMENT = "dataset_management";

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_query/dataset/{name}"));
    }

    @Override
    public String getName() {
        return "esql_delete_dataset";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final DeleteDatasetAction.Request req = new DeleteDatasetAction.Request(
            RestUtils.getMasterNodeTimeout(request),
            RestUtils.getAckTimeout(request),
            request.param("name")
        );
        return channel -> client.execute(DeleteDatasetAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(DATASET_MANAGEMENT);
    }
}
