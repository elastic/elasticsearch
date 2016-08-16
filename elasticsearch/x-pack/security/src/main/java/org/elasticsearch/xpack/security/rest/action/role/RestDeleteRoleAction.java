/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.role;

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
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.security.action.role.DeleteRoleRequestBuilder;
import org.elasticsearch.xpack.security.action.role.DeleteRoleResponse;
import org.elasticsearch.xpack.security.client.SecurityClient;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest endpoint to delete a Role from the security index
 */
public class RestDeleteRoleAction extends BaseRestHandler {

    @Inject
    public RestDeleteRoleAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(DELETE, "/_xpack/security/role/{name}", this);

        // @deprecated: Remove in 6.0
        controller.registerAsDeprecatedHandler(DELETE, "/_shield/role/{name}", this,
                                               "[DELETE /_shield/role/{name}] is deprecated! Use " +
                                               "[DELETE /_xpack/security/role/{name}] instead.",
                                               deprecationLogger);
    }

    @Override
    public void handleRequest(RestRequest request, final RestChannel channel, NodeClient client) throws Exception {
        DeleteRoleRequestBuilder requestBuilder = new SecurityClient(client).prepareDeleteRole(request.param("name"));
        if (request.hasParam("refresh")) {
            requestBuilder.refresh(request.paramAsBoolean("refresh", true));
        }
        requestBuilder.execute(new RestBuilderListener<DeleteRoleResponse>(channel) {
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
