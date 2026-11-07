/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestGetPipelineAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(RestUtils.REST_MASTER_TIMEOUT_PARAM, "summary");
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Sets.addToCopy(SUPPORTED_QUERY_PARAMETERS, "id");

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_ingest/pipeline"), new Route(GET, "/_ingest/pipeline/{id}"));
    }

    @Override
    public String getName() {
        return "ingest_get_pipeline_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final var request = new GetPipelineRequest(
            getMasterNodeTimeout(restRequest),
            restRequest.paramAsBoolean("summary", false),
            Strings.splitStringByCommaToArray(restRequest.param("id"))
        );
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            GetPipelineAction.INSTANCE,
            request,
            // A GET for a specific pipeline id that is not found returns 404, so preserve the response's status rather than always 200.
            new RestChunkedToXContentListener<>(channel) {
                @Override
                protected RestStatus getRestStatus(GetPipelineResponse response) {
                    return response.status();
                }
            }
        );
    }

    @Override
    public Set<String> supportedQueryParameters() {
        return SUPPORTED_QUERY_PARAMETERS;
    }

    @Override
    public Set<String> allSupportedParameters() {
        return ALL_SUPPORTED_PARAMETERS;
    }

}
