/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest action to delete a user from the security index
 */
@ServerlessScope(Scope.INTERNAL)
public class RestDeleteUserAction extends NativeUserBaseRestHandler {

    public RestDeleteUserAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_security/user/{username}"));
    }

    @Override
    public String getName() {
        return "security_delete_user_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String username = request.param("username");
        final String refresh = request.param("refresh");
        return channel -> new DeleteUserRequestBuilder(client).username(username)
            .setRefreshPolicy(refresh)
            .execute(new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(DeleteUserResponse response, XContentBuilder builder) throws Exception {
                    return new RestResponse(
                        response.found() ? RestStatus.OK : RestStatus.NOT_FOUND,
                        builder.startObject().field("found", response.found()).endObject()
                    );
                }
            });
    }
}
