/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.rest.action.role;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
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
import org.elasticsearch.shield.action.role.GetRolesResponse;
import org.elasticsearch.shield.client.SecurityClient;
import org.elasticsearch.shield.authz.RoleDescriptor;

/**
 * Rest endpoint to retrieve a Role from the shield index
 */
public class RestGetRolesAction extends BaseRestHandler {

    @Inject
    public RestGetRolesAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/role/", this);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/role/{name}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        final String[] roles = request.paramAsStringArray("name", Strings.EMPTY_ARRAY);
        new SecurityClient(client).prepareGetRoles(roles).execute(new RestBuilderListener<GetRolesResponse>(channel) {
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
