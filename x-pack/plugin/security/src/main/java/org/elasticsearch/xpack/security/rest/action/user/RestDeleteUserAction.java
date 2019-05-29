/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserResponse;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest action to delete a user from the security index
 */
public class RestDeleteUserAction extends SecurityBaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestDeleteUserAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public RestDeleteUserAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(DELETE, "/_xpack/security/user/{username}", this);
        controller.registerHandler(DELETE, "/_security/user/{username}", this);

        // @deprecated: Remove in 6.0
        controller.registerAsDeprecatedHandler(DELETE, "/_shield/user/{username}", this,
                                               "[DELETE /_shield/user/{username}] is deprecated! Use " +
                                               "[DELETE /_xpack/security/user/{username}] instead.",
                                               deprecationLogger);
    }

    @Override
    public String getName() {
        return "xpack_security_delete_user_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String username = request.param("username");
        final String refresh = request.param("refresh");
        return channel -> new SecurityClient(client).prepareDeleteUser(username)
                .setRefreshPolicy(refresh)
                .execute(new RestBuilderListener<DeleteUserResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(DeleteUserResponse response, XContentBuilder builder) throws Exception {
                        return new BytesRestResponse(response.found() ? RestStatus.OK : RestStatus.NOT_FOUND,
                                builder.startObject()
                                        .field("found", response.found())
                                        .endObject());
                    }
                });
    }
}
