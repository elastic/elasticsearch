/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteStoredScriptAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_scripts/{id}"));
    }

    @Override
    public String getName() {
        return "delete_stored_script_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String id = request.param("id");
        DeleteStoredScriptRequest deleteStoredScriptRequest = new DeleteStoredScriptRequest(id);
        deleteStoredScriptRequest.timeout(request.paramAsTime("timeout", deleteStoredScriptRequest.timeout()));
        deleteStoredScriptRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteStoredScriptRequest.masterNodeTimeout()));

        return channel -> client.admin().cluster().deleteStoredScript(deleteStoredScriptRequest, new RestToXContentListener<>(channel));
    }
}
