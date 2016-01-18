/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.admin.user;

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
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.client.ShieldClient;

/**
 * Rest action to retrieve a user from the shield index
 */
public class RestGetUsersAction extends BaseRestHandler {

    @Inject
    public RestGetUsersAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/user/", this);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/user/{user}", this);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/users/", this);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/users/{user}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, final RestChannel channel, Client client) throws Exception {
        String[] users = Strings.splitStringByCommaToArray(request.param("user"));

        new ShieldClient(client).prepareGetUsers().users(users).execute(new RestBuilderListener<GetUsersResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetUsersResponse getUsersResponse, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("found", getUsersResponse.isExists());
                builder.startArray("users");
                for (User user : getUsersResponse.users()) {
                    user.toXContent(builder, ToXContent.EMPTY_PARAMS);
                }
                builder.endArray();
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
