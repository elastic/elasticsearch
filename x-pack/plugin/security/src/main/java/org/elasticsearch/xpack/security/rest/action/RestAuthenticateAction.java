/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAuthenticateAction extends SecurityBaseRestHandler {

    private final SecurityContext securityContext;

    public RestAuthenticateAction(Settings settings, SecurityContext securityContext, XPackLicenseState licenseState) {
        super(settings, licenseState);
        this.securityContext = securityContext;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, "/_security/_authenticate").replaces(GET, "/_xpack/security/_authenticate", RestApiVersion.V_7).build()
        );
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

        return channel -> client.execute(
            AuthenticateAction.INSTANCE,
            new AuthenticateRequest(username),
            new RestBuilderListener<AuthenticateResponse>(channel) {
                @Override
                public RestResponse buildResponse(AuthenticateResponse authenticateResponse, XContentBuilder builder) throws Exception {
                    authenticateResponse.authentication().toXContent(builder, ToXContent.EMPTY_PARAMS);
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            }
        );

    }
}
