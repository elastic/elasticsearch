/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.UpdateForV9;
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

    @UpdateForV9(owner = UpdateForV9.Owner.MACHINE_LEARNING)
    // one or more routes use ".replaces" with RestApiVersion.V_8 which will require use of REST API compatibility headers to access
    // that route in v9. It is unclear if this was intentional for v9, and the code has been updated to ".deprecateAndKeep" which will
    // continue to emit deprecations warnings but will not require any special headers to access the API in v9.
    // Please review and update the code and tests as needed. The original code remains commented out below for reference.
    @Override
    public List<Route> routes() {
        return List.of(
            // Route.builder(POST, BASE_PATH + "find_structure").replaces(POST, "/_ml/find_file_structure", RestApiVersion.V_8).build()
            new Route(POST, BASE_PATH + "find_structure"),
            Route.builder(POST, "/_ml/find_file_structure").deprecateAndKeep("Use the _text_structure API instead.").build()
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
        var content = restRequest.requiredContent();
        request.setSample(content);

        return channel -> client.execute(
            FindStructureAction.INSTANCE,
            request,
            ActionListener.withRef(new RestToXContentListener<>(channel), content)
        );
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(TextStructure.EXPLAIN);
    }
}
