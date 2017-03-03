/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import java.io.IOException;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.security.SecurityContext;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.security.client.SecurityClient;
import org.elasticsearch.xpack.security.user.User;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler that tests whether a user has the specified
 * {@link org.elasticsearch.xpack.security.authz.RoleDescriptor.IndicesPrivileges privileges}
 */
public class RestHasPrivilegesAction extends BaseRestHandler {

    private final SecurityContext securityContext;

    public RestHasPrivilegesAction(Settings settings, RestController controller, SecurityContext securityContext) {
        super(settings);
        this.securityContext = securityContext;
        controller.registerHandler(GET, "/_xpack/security/user/{username}/_has_privileges", this);
        controller.registerHandler(POST, "/_xpack/security/user/{username}/_has_privileges", this);
        controller.registerHandler(GET, "/_xpack/security/user/_has_privileges", this);
        controller.registerHandler(POST, "/_xpack/security/user/_has_privileges", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String username = getUsername(request);
        HasPrivilegesRequestBuilder requestBuilder = new SecurityClient(client)
                .prepareHasPrivileges(username, request.content(), request.getXContentType());
        return channel -> requestBuilder.execute(new HasPrivilegesRestResponseBuilder(username, channel));
    }

    private String getUsername(RestRequest request) {
        final String username = request.param("username");
        if (username != null) {
            return username;
        }
        final User user = securityContext.getUser();
        if (user.runAs() != null) {
            return user.runAs().principal();
        } else {
            return user.principal();
        }
    }

    static class HasPrivilegesRestResponseBuilder extends RestBuilderListener<HasPrivilegesResponse> {
        private String username;

        HasPrivilegesRestResponseBuilder(String username, RestChannel channel) {
            super(channel);
            this.username = username;
        }

        @Override
        public RestResponse buildResponse(HasPrivilegesResponse response, XContentBuilder builder) throws Exception {
            builder.startObject()
                    .field("username", username)
                    .field("has_all_requested", response.isCompleteMatch());

            builder.field("cluster");
            builder.map(response.getClusterPrivileges());

            builder.startObject("index");
            for (HasPrivilegesResponse.IndexPrivileges index : response.getIndexPrivileges()) {
                builder.field(index.getIndex());
                builder.map(index.getPrivileges());
            }
            builder.endObject();

            builder.endObject();
            return new BytesRestResponse(RestStatus.OK, builder);
        }

    }
}
