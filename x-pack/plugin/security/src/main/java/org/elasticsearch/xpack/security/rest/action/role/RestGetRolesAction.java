/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.role;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.action.role.GetRolesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest endpoint to retrieve a Role from the security index
 */
@ServerlessScope(Scope.PUBLIC)
public class RestGetRolesAction extends NativeRoleBaseRestHandler {

    public RestGetRolesAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/role/"), new Route(GET, "/_security/role/{name}"));
    }

    @Override
    public String getName() {
        return "security_get_roles_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String[] roles = request.paramAsStringArray("name", Strings.EMPTY_ARRAY);
        final boolean restrictToNativeRolesOnly = request.isServerlessRequest() && false == request.isOperatorRequest();
        return channel -> new GetRolesRequestBuilder(client).names(roles)
            .nativeOnly(restrictToNativeRolesOnly)
            .execute(new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(GetRolesResponse response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    for (RoleDescriptor role : response.roles()) {
                        builder.field(role.getName(), role);
                    }
                    builder.endObject();

                    // if the user asked for specific roles, but none of them were found
                    // we'll return an empty result and 404 status code
                    if (roles.length != 0 && response.roles().length == 0) {
                        return new RestResponse(RestStatus.NOT_FOUND, builder);
                    }

                    // either the user asked for all roles, or at least one of the roles
                    // the user asked for was found
                    return new RestResponse(RestStatus.OK, builder);
                }
            });
    }

    @Override
    protected Exception innerCheckFeatureAvailable(RestRequest request) {
        // Note: For non-restricted requests this action handles both reserved roles and native
        // roles, and should still be available even if native role management is disabled.
        // For restricted requests it should only be available if native role management is enabled
        if (false == request.isServerlessRequest() || request.isOperatorRequest()) {
            return null;
        } else {
            return super.innerCheckFeatureAvailable(request);
        }
    }
}
