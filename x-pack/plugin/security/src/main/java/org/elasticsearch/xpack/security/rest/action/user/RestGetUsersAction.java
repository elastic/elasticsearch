/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

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
import org.elasticsearch.xpack.core.security.action.user.GetUsersRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest action to retrieve a user from the security index
 */
public class RestGetUsersAction extends SecurityBaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestGetUsersAction.class));

    public RestGetUsersAction(Settings settings, XPackLicenseState licenseState) {
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
            new ReplacedRoute(GET, "/_security/user/", GET, "/_xpack/security/user/", deprecationLogger),
            new ReplacedRoute(GET, "/_security/user/{username}", GET, "/_xpack/security/user/{username}", deprecationLogger)
        ));
    }

    @Override
    public String getName() {
        return "security_get_users_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] usernames = request.paramAsStringArray("username", Strings.EMPTY_ARRAY);

        return channel -> new GetUsersRequestBuilder(client).usernames(usernames).execute(new RestBuilderListener<>(channel) {
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
