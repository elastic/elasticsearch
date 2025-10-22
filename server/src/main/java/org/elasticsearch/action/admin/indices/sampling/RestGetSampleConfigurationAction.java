/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/**
 * REST action for getting sampling configurations for indices.
 * <p>
 * Handles GET requests to /{index}/_sample/config endpoint and delegates
 * to the GetSampleConfigurationAction transport action.
 * </p>
 *
 * <p>Example usage:</p>
 * GET /my-index/_sample/config
 * returns
 *  {
 *      "index": "logs",
 *      "configuration":
 *      {
 *          "rate": "5%",
 *          "if": "ctx?.network?.name == 'Guest'"
 *      }
 *  }
 */
@ServerlessScope(Scope.INTERNAL)
public class RestGetSampleConfigurationAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_sample/config"));
    }

    @Override
    public String getName() {
        return "get_sample_configuration_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indexNames = request.param("index").split(",");
        if (indexNames.length > 1) {
            throw new ActionRequestValidationException().addValidationError(
                "Provided endpoint can only get sampling configuration for a single index at a time, but found "
                    + Arrays.stream(indexNames).collect(Collectors.joining(", ", "[", "]"))
            );
        }
        GetSampleConfigurationAction.Request getRequest = new GetSampleConfigurationAction.Request(getMasterNodeTimeout(request));

        // Set the target index
        getRequest.indices(indexNames);

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            GetSampleConfigurationAction.INSTANCE,
            getRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }

}
