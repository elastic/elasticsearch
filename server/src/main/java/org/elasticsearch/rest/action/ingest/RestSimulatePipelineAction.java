/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSimulatePipelineAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_ingest/pipeline/{id}/_simulate"),
            new Route(POST, "/_ingest/pipeline/{id}/_simulate"),
            new Route(GET, "/_ingest/pipeline/_simulate"),
            new Route(POST, "/_ingest/pipeline/_simulate"));
    }

    @Override
    public String getName() {
        return "ingest_simulate_pipeline_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Tuple<XContentType, BytesReference> sourceTuple = restRequest.contentOrSourceParam();
        SimulatePipelineRequest request = new SimulatePipelineRequest(sourceTuple.v2(), sourceTuple.v1());
        request.setId(restRequest.param("id"));
        request.setVerbose(restRequest.paramAsBoolean("verbose", false));
        return channel -> client.admin().cluster().simulatePipeline(request, new RestToXContentListener<>(channel));
    }
}
