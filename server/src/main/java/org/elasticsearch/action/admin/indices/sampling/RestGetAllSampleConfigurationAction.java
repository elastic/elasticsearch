/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/**
 * REST action for getting all sampling configurations for indices.
 * <p>
 * Handles GET requests to /_sample/config endpoint and delegates
 * to the GetAllSampleConfigurationAction transport action.
 * </p>
 *
 * <p>Example usage:</p>
 * GET /_sample/config
 * returns
 * [
 * {
 *  "index": "logs",
 *  "configuration": {
 *      "rate": ".5",
 *      "if": "ctx?.network?.name == 'Guest'"
 *  }
 * },
 * {
 *  "index": "logsTwo",
 *  "configuration": {
 *      "rate": ".75"
 *  }
 * },
 * ]
 */
@ServerlessScope(Scope.INTERNAL)
public class RestGetAllSampleConfigurationAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_sample/config"));
    }

    @Override
    public String getName() {
        return "get_all_sample_configuration_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GetAllSampleConfigurationAction.Request getRequest = new GetAllSampleConfigurationAction.Request(getMasterNodeTimeout(request));

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            GetAllSampleConfigurationAction.INSTANCE,
            getRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }

}
