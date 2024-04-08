/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.rest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureAction;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.textstructure.TextStructurePlugin.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestFindStructureAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, BASE_PATH + "find_structure").replaces(POST, "/_ml/find_file_structure", RestApiVersion.V_8).build()
        );
    }

    @Override
    public String getName() {
        return "text_structure_find_structure_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        FindStructureAction.Request request = new FindStructureAction.Request();
        RestFindStructureArgumentsParser.parse(restRequest, request);

        if (restRequest.hasContent()) {
            request.setSample(restRequest.content());
        } else {
            throw new ElasticsearchParseException("request body is required");
        }

        return channel -> client.execute(FindStructureAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(TextStructure.EXPLAIN);
    }
}
