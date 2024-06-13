/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.textstructure.action.FindMessageStructureAction;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.textstructure.TextStructurePlugin.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestFindMessageStructureAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, BASE_PATH + "find_message_structure"), new Route(POST, BASE_PATH + "find_message_structure"));
    }

    @Override
    public String getName() {
        return "text_structure_find_message_structure_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        FindMessageStructureAction.Request request;
        try (XContentParser parser = restRequest.contentOrSourceParamParser()) {
            request = FindMessageStructureAction.Request.parseRequest(parser);
        }
        RestFindStructureArgumentsParser.parse(restRequest, request);
        return channel -> client.execute(FindMessageStructureAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(TextStructure.EXPLAIN);
    }
}
