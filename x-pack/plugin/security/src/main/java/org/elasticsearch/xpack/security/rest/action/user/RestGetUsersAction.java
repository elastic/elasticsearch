/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.action.user.GetUsersRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest action to retrieve a user from the security index
 */
public class RestGetUsersAction extends SecurityBaseRestHandler {

    public RestGetUsersAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, "/_security/user/").replaces(GET, "/_xpack/security/user/", RestApiVersion.V_7).build(),
            Route.builder(GET, "/_security/user/{username}").replaces(GET, "/_xpack/security/user/{username}", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "security_get_users_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] usernames = request.paramAsStringArray("username", Strings.EMPTY_ARRAY);
        final boolean withProfileUid = request.paramAsBoolean("with_profile_uid", false);

        return channel -> new GetUsersRequestBuilder(client).usernames(usernames)
            .withProfileUid(withProfileUid)
            .execute(new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(GetUsersResponse response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, ToXContent.EMPTY_PARAMS);

                    // if the user asked for specific users, but none of them were found
                    // we'll return an empty result and 404 status code
                    if (usernames.length != 0 && response.users().length == 0) {
                        return new RestResponse(RestStatus.NOT_FOUND, builder);
                    }

                    // either the user asked for all users, or at least one of the users
                    // was found
                    return new RestResponse(RestStatus.OK, builder);
                }
            });
    }
}
