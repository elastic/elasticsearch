/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.fieldsenum.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.fieldsenum.action.FieldsEnumAction;
import org.elasticsearch.xpack.core.fieldsenum.action.FieldsEnumRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestFieldsEnumAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/{index}/_fields_enum"),
            new Route(POST, "/{index}/_fields_enum"));
    }

    @Override
    public String getName() {
        return "fields_enum_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            FieldsEnumRequest fieldEnumRequest = FieldsEnumAction.fromXContent(parser, 
                Strings.splitStringByCommaToArray(request.param("index")));
            return channel ->
            client.execute(FieldsEnumAction.INSTANCE, fieldEnumRequest, new RestToXContentListener<>(channel));
        }        
    }
    
}
