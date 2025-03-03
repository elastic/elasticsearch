/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportPutStoredScriptAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestPutStoredScriptAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_scripts/{id}"),
            new Route(PUT, "/_scripts/{id}"),
            new Route(POST, "/_scripts/{id}/{context}"),
            new Route(PUT, "/_scripts/{id}/{context}")
        );
    }

    @Override
    public String getName() {
        return "put_stored_script_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final var content = request.requiredContent();
        final var xContentType = request.getXContentType();
        final var putRequest = new PutStoredScriptRequest(
            getMasterNodeTimeout(request),
            getAckTimeout(request),
            request.param("id"),
            request.param("context"),
            content.length(),
            StoredScriptSource.parse(content, xContentType)
        );
        return channel -> client.execute(
            TransportPutStoredScriptAction.TYPE,
            putRequest,
            ActionListener.withRef(new RestToXContentListener<>(channel), content)
        );
    }
}
