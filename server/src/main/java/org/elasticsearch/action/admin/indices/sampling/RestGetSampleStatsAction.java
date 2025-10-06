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
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetSampleStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_sample_stats";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_sample/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] indexNames = request.param("index").split(",");
        if (indexNames.length > 1) {
            throw new ActionRequestValidationException().addValidationError(
                "Can only get samples for a single index at a time, but found "
                    + Arrays.stream(indexNames).collect(Collectors.joining(", ", "[", "]"))
            );
        }
        GetSampleStatsAction.Request getSampleStatsRequest = new GetSampleStatsAction.Request(indexNames[0]);
        return channel -> client.execute(GetSampleStatsAction.INSTANCE, getSampleStatsRequest, new RestToXContentListener<>(channel));
    }
}
