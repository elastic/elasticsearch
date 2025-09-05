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
import org.elasticsearch.xpack.core.textstructure.action.FindFieldStructureAction;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.textstructure.TextStructurePlugin.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestFindFieldStructureAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, BASE_PATH + "find_field_structure"));
    }

    @Override
    public String getName() {
        return "text_structure_find_field_structure_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        FindFieldStructureAction.Request request = new FindFieldStructureAction.Request();
        RestFindStructureArgumentsParser.parse(restRequest, request);
        request.setIndex(restRequest.param(FindFieldStructureAction.Request.INDEX.getPreferredName()));
        request.setField(restRequest.param(FindFieldStructureAction.Request.FIELD.getPreferredName()));
        return channel -> client.execute(FindFieldStructureAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(TextStructure.EXPLAIN);
    }
}
