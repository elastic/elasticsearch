/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.rest.action.user;

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
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.action.user.GetUsersResponse;
import org.elasticsearch.shield.client.SecurityClient;

/**
 * Rest action to retrieve a user from the shield index
 */
public class RestGetUsersAction extends BaseRestHandler {

    @Inject
    public RestGetUsersAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/user/", this);
        controller.registerHandler(RestRequest.Method.GET, "/_shield/user/{username}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, final RestChannel channel, Client client) throws Exception {
        String[] usernames = request.paramAsStringArray("username", Strings.EMPTY_ARRAY);

        new SecurityClient(client).prepareGetUsers(usernames).execute(new RestBuilderListener<GetUsersResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetUsersResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                for (User user : response.users()) {
                    builder.field(user.principal(), user);
                }
                builder.endObject();

                // if the user asked for specific users, but none of them were found
                // we'll return an empty result and 404 status code
                if (usernames.length != 0 && response.users().length == 0) {
                    return new BytesRestResponse(RestStatus.NOT_FOUND, builder);
                }

                // either the user asked for all users, or at least one of the users
                // was found
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
