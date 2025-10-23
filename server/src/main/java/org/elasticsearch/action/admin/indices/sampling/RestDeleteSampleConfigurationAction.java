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
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/**
 * REST action for deleting sampling configurations for indices.
 * <p>
 * Handles DELETE requests to /{index}/_sample/config endpoint and delegates
 * to the DeleteSampleConfigurationAction transport action.
 * </p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * DELETE /my-index/_sample/config
 * }</pre>
 */
@ServerlessScope(Scope.INTERNAL)
public class RestDeleteSampleConfigurationAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/{index}/_sample/config"));
    }

    @Override
    public String getName() {
        return "delete_sample_configuration_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indexNames = request.param("index").split(",");
        if (indexNames.length > 1) {
            throw new ActionRequestValidationException().addValidationError(
                "Can only delete sampling configuration for a single index at a time, but found "
                    + Arrays.stream(indexNames).collect(Collectors.joining(", ", "[", "]"))
            );
        }
        DeleteSampleConfigurationAction.Request deleteSampleConfigRequest = new DeleteSampleConfigurationAction.Request(
            getMasterNodeTimeout(request),
            getAckTimeout(request)
        );
        deleteSampleConfigRequest.indices(indexNames[0]);
        return channel -> client.execute(
            DeleteSampleConfigurationAction.INSTANCE,
            deleteSampleConfigRequest,
            new RestToXContentListener<>(channel)
        );
    }

}
