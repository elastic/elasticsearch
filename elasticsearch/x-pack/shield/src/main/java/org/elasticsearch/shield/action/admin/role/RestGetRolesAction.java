/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.admin.role;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
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
 * Rest endpoint to retrieve a Role from the shield index
 */
public class RestGetRolesAction extends BaseRestHandler {

    @Inject
    public RestGetRolesAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/role/", this);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/role/{roles}", this);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/roles/", this);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/roles/{roles}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, final RestChannel channel, Client client) throws Exception {
        String[] roles = Strings.splitStringByCommaToArray(request.param("roles"));

        new ShieldClient(client).prepareGetRoles().roles(roles).execute(new RestBuilderListener<GetRolesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetRolesResponse getRolesResponse, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("found", getRolesResponse.isExists());
                builder.startArray("roles");
                for (ToXContent role : getRolesResponse.roles()) {
                    role.toXContent(builder, ToXContent.EMPTY_PARAMS);
                }
                builder.endArray();
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
