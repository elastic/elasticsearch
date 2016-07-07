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
import org.elasticsearch.xpack.security.SecurityContext;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.action.user.ChangePasswordResponse;
import org.elasticsearch.xpack.security.client.SecurityClient;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 */
public class RestChangePasswordAction extends BaseRestHandler {

    private final SecurityContext securityContext;

    @Inject
    public RestChangePasswordAction(Settings settings, RestController controller, SecurityContext securityContext) {
        super(settings);
        this.securityContext = securityContext;
        controller.registerHandler(POST, "/_xpack/security/user/{username}/_password", this);
        controller.registerHandler(PUT, "/_xpack/security/user/{username}/_password", this);
        controller.registerHandler(POST, "/_xpack/security/user/_password", this);
        controller.registerHandler(PUT, "/_xpack/security/user/_password", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        final User user = securityContext.getUser();
        String username = request.param("username");
        if (username == null) {
            username = user.runAs() == null ? user.principal() : user.runAs().principal();;
        }

        new SecurityClient(client).prepareChangePassword(username, request.content())
                .setRefreshPolicy(request.param("refresh"))
                .execute(new RestBuilderListener<ChangePasswordResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(ChangePasswordResponse changePasswordResponse, XContentBuilder builder) throws
                            Exception {
                        return new BytesRestResponse(RestStatus.OK, channel.newBuilder().startObject().endObject());
                    }
                });
    }
}
