/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

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
        String id = request.param("id");
        String context = request.param("context");
        BytesReference content = request.requiredContent();
        XContentType xContentType = request.getXContentType();
        StoredScriptSource source = StoredScriptSource.parse(content, xContentType);

        PutStoredScriptRequest putRequest = new PutStoredScriptRequest(id, context, content, request.getXContentType(), source);
        putRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putRequest.masterNodeTimeout()));
        putRequest.timeout(request.paramAsTime("timeout", putRequest.timeout()));
        return channel -> client.admin().cluster().putStoredScript(putRequest, new RestToXContentListener<>(channel));
    }
}
