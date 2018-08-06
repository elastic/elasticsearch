/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.protocol.xpack.security.User;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest action to retrieve a user from the security index
 */
public class RestGetUsersAction extends SecurityBaseRestHandler {

    public RestGetUsersAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(GET, "/_xpack/security/user/", this);
        controller.registerHandler(GET, "/_xpack/security/user/{username}", this);
    }

    @Override
    public String getName() {
        return "xpack_security_get_users_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] usernames = request.paramAsStringArray("username", Strings.EMPTY_ARRAY);

        return channel -> new SecurityClient(client).prepareGetUsers(usernames).execute(new RestBuilderListener<GetUsersResponse>(channel) {
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
