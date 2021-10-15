/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.ModifyDataStreamsAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestModifyDataStreamAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "modify_data_stream_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_data_stream/_modify"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        MetadataDataStreamsService.ModifyDataStreamRequest modifyDsRequest = new MetadataDataStreamsService.ModifyDataStreamRequest();
        modifyDsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", modifyDsRequest.masterNodeTimeout()));
        modifyDsRequest.timeout(request.paramAsTime("timeout", modifyDsRequest.timeout()));
        try (XContentParser parser = request.contentParser()) {
            MetadataDataStreamsService.ModifyDataStreamRequest.PARSER.parse(parser, modifyDsRequest, null);
        }
        if (modifyDsRequest.getActions() == null || modifyDsRequest.getActions().isEmpty()) {
            throw new IllegalArgumentException("no data stream actions specified");
        }
        return channel -> client.execute(ModifyDataStreamsAction.INSTANCE, modifyDsRequest, new RestToXContentListener<>(channel));
    }

}
