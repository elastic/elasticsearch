/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAuthenticateAction extends SecurityBaseRestHandler {

    private final SecurityContext securityContext;
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestAuthenticateAction.class));

    public RestAuthenticateAction(Settings settings, RestController controller, SecurityContext securityContext,
                                  XPackLicenseState licenseState) {
        super(settings, licenseState);
        this.securityContext = securityContext;
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            GET, "/_security/_authenticate", this,
            GET, "/_xpack/security/_authenticate", deprecationLogger);
    }

    @Override
    public String getName() {
        return "security_authenticate_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final User user = securityContext.getUser();
        if (user == null) {
            return restChannel -> { throw new IllegalStateException("we should never have a null user and invoke this consumer"); };
        }
        final String username = user.principal();

        return channel -> client.execute(AuthenticateAction.INSTANCE, new AuthenticateRequest(username),
                new RestBuilderListener<AuthenticateResponse>(channel) {
            @Override
            public RestResponse buildResponse(AuthenticateResponse authenticateResponse, XContentBuilder builder) throws Exception {
                authenticateResponse.authentication().toXContent(builder, ToXContent.EMPTY_PARAMS);
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });

    }
}
