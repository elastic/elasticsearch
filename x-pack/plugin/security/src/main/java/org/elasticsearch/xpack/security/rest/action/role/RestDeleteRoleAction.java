/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.role;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest endpoint to delete a Role from the security index
 */
@ServerlessScope(Scope.INTERNAL)
public class RestDeleteRoleAction extends SecurityBaseRestHandler {

    public RestDeleteRoleAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(DELETE, "/_security/role/{name}").replaces(DELETE, "/_xpack/security/role/{name}", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "security_delete_role_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String name = request.param("name");
        final String refresh = request.param("refresh");

        return channel -> new DeleteRoleRequestBuilder(client).name(name)
            .setRefreshPolicy(refresh)
            .execute(new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(DeleteRoleResponse response, XContentBuilder builder) throws Exception {
                    return new RestResponse(
                        response.found() ? RestStatus.OK : RestStatus.NOT_FOUND,
                        builder.startObject().field("found", response.found()).endObject()
                    );
                }
            });
    }
}
