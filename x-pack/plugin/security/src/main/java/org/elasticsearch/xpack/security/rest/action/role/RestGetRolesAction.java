/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.role;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.role.GetRolesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest endpoint to retrieve a Role from the security index
 */
public class RestGetRolesAction extends SecurityBaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestGetRolesAction.class));

    public RestGetRolesAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return Collections.unmodifiableList(Arrays.asList(
            new ReplacedRoute(GET, "/_security/role/", GET, "/_xpack/security/role/", deprecationLogger),
            new ReplacedRoute(GET, "/_security/role/{name}", GET, "/_xpack/security/role/{name}", deprecationLogger)
        ));
    }

    @Override
    public String getName() {
        return "security_get_roles_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String[] roles = request.paramAsStringArray("name", Strings.EMPTY_ARRAY);
        return channel -> new GetRolesRequestBuilder(client)
            .names(roles)
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
                    return new BytesRestResponse(RestStatus.NOT_FOUND, builder);
                }

                // either the user asked for all roles, or at least one of the roles
                // the user asked for was found
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
