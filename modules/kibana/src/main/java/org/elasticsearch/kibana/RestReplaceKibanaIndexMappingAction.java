/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * REST entry point for {@link ReplaceKibanaIndexMappingAction}: {@code PUT /_kibana/<index>/_replace_mappings} with the
 * complete replacement mapping as the request body.
 */
public class RestReplaceKibanaIndexMappingAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "kibana_replace_index_mapping_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_kibana/{index}/_replace_mappings"), new Route(POST, "/_kibana/{index}/_replace_mappings"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final var replaceRequest = new ReplaceKibanaIndexMappingAction.Request(
            RestUtils.getMasterNodeTimeout(request),
            RestUtils.getAckTimeout(request),
            request.param("index"),
            request.requiredContent().utf8ToString()
        );
        return channel -> client.execute(ReplaceKibanaIndexMappingAction.INSTANCE, replaceRequest, new RestToXContentListener<>(channel));
    }
}
