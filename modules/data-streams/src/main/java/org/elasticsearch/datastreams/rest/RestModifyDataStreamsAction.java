/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestModifyDataStreamsAction extends BaseRestHandler {

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
        ModifyDataStreamsAction.Request modifyDsRequest;
        try (XContentParser parser = request.contentParser()) {
            modifyDsRequest = ModifyDataStreamsAction.Request.PARSER.parse(
                parser,
                actions -> new ModifyDataStreamsAction.Request(
                    RestUtils.getMasterNodeTimeout(request),
                    RestUtils.getAckTimeout(request),
                    actions
                )
            );
        }
        if (modifyDsRequest.getActions() == null || modifyDsRequest.getActions().isEmpty()) {
            throw new IllegalArgumentException("no data stream actions specified, at least one must be specified");
        }
        return channel -> client.execute(ModifyDataStreamsAction.INSTANCE, modifyDsRequest, new RestToXContentListener<>(channel));
    }

}
