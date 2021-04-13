/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termenum.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.termenum.action.TermEnumAction;
import org.elasticsearch.xpack.core.termenum.action.TermEnumRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestTermEnumAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/{index}/_terms"),
            new Route(POST, "/{index}/_terms"));
    }

    @Override
    public String getName() {
        return "term_enum_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            TermEnumRequest termEnumRequest = TermEnumAction.fromXContent(parser, 
                Strings.splitStringByCommaToArray(request.param("index")));
            return channel ->
            client.execute(TermEnumAction.INSTANCE, termEnumRequest, new RestToXContentListener<>(channel));
        }        
    }
    
}
