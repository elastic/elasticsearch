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
import org.elasticsearch.shield.client.ShieldClient;

/**
 * Rest endpoint to delete a Role from the shield index
 */
public class RestDeleteRoleAction extends BaseRestHandler {

    @Inject
    public RestDeleteRoleAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.DELETE, "/_shield/role/{role}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, final RestChannel channel, Client client) throws Exception {
        String role = request.param("role");
        DeleteRoleRequest delRoleRequest = new DeleteRoleRequest(role);

        new ShieldClient(client).deleteRole(delRoleRequest, new RestBuilderListener<DeleteRoleResponse>(channel) {
            @Override
            public RestResponse buildResponse(DeleteRoleResponse response, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(response.found() ? RestStatus.OK : RestStatus.NOT_FOUND,
                        builder.startObject()
                        .field("found", response.found())
                        .endObject());
            }
        });
    }
}
