/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.admin.role;

import org.elasticsearch.client.Client;
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
import org.elasticsearch.shield.authz.RoleDescriptor;
import org.elasticsearch.shield.client.ShieldClient;

/**
 * Rest endpoint to add a Role to the shield index
 */
public class RestAddRoleAction extends BaseRestHandler {

    @Inject
    public RestAddRoleAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.POST, "/_shield/role/{role}", this);
        controller.registerHandler(RestRequest.Method.PUT, "/_shield/role/{role}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, final RestChannel channel, Client client) throws Exception {
        AddRoleRequest addRoleReq = new AddRoleRequest(request.content());
        addRoleReq.name(request.param("role"));

        new ShieldClient(client).addRole(addRoleReq, new RestBuilderListener<AddRoleResponse>(channel) {
            @Override
            public RestResponse buildResponse(AddRoleResponse addRoleResponse, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK,
                        builder.startObject()
                        .field("role", addRoleResponse)
                        .endObject());
            }
        });
    }
}
