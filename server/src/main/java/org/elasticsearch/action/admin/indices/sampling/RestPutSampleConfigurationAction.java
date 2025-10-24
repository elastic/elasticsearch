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
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/**
 * REST action for updating sampling configurations for indices.
 * <p>
 * Handles PUT requests to /{index}/_sample/config endpoint and delegates
 * to the PutSampleConfigurationAction transport action.
 * </p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * PUT /my-index/_sample/config
 * {
 *   "rate": ".05",
 *   "max_samples": 1000,
 *   "max_size": "10mb",
 *   "time_to_live": "1d",
 *   "if": "ctx?.network?.name == 'Guest'"
 * }
 * }</pre>
 */
@ServerlessScope(Scope.INTERNAL)
public class RestPutSampleConfigurationAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/{index}/_sample/config"));
    }

    @Override
    public String getName() {
        return "put_sample_configuration_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indexNames = request.param("index").split(",");
        if (indexNames.length > 1) {
            throw new ActionRequestValidationException().addValidationError(
                "Can only set sampling configuration for a single index at a time, but found "
                    + Arrays.stream(indexNames).collect(Collectors.joining(", ", "[", "]"))
            );
        }
        PutSampleConfigurationAction.Request putRequest;

        XContentParser parser = request.contentParser();
        SamplingConfiguration samplingConfig = SamplingConfiguration.fromXContentUserData(parser);
        putRequest = new PutSampleConfigurationAction.Request(samplingConfig, getMasterNodeTimeout(request), getAckTimeout(request));

        // Set the target index
        putRequest.indices(indexNames);

        // TODO: Make this cancellable e.g. RestGetSampleStatsAction
        return channel -> client.execute(PutSampleConfigurationAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
    }

}
