/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.xpack.security.action.user.DeleteUserRequest;
import org.elasticsearch.xpack.security.action.user.DeleteUserRequestBuilder;
import org.elasticsearch.xpack.security.action.user.DeleteUserResponse;
import org.elasticsearch.xpack.security.client.SecurityClient;

/**
 * Rest action to delete a user from the security index
 */
public class RestDeleteUserAction extends BaseRestHandler {

    @Inject
    public RestDeleteUserAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.DELETE, "/_xpack/security/user/{username}", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestChannel channel, NodeClient client) throws Exception {
        String username = request.param("username");

        DeleteUserRequestBuilder requestBuilder = new SecurityClient(client).prepareDeleteUser(username);
        if (request.hasParam("refresh")) {
            requestBuilder.refresh(request.paramAsBoolean("refresh", true));
        }
        requestBuilder.execute(new RestBuilderListener<DeleteUserResponse>(channel) {
            @Override
            public RestResponse buildResponse(DeleteUserResponse response, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(response.found() ? RestStatus.OK : RestStatus.NOT_FOUND,
                        builder.startObject()
                                .field("found", response.found())
                                .endObject());
            }
        });
    }
}
