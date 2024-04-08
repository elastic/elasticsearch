/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.oidc;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest handler that invalidates a security token for the given OpenID Connect realm and if the configuration of
 * the realm supports it, generates a redirect to the `end_session_endpoint` of the OpenID Connect Provider.
 */
public class RestOpenIdConnectLogoutAction extends OpenIdConnectBaseRestHandler {

    static final ObjectParser<OpenIdConnectLogoutRequest, Void> PARSER = new ObjectParser<>("oidc_logout", OpenIdConnectLogoutRequest::new);

    static {
        PARSER.declareString(OpenIdConnectLogoutRequest::setToken, new ParseField("token"));
        PARSER.declareString(OpenIdConnectLogoutRequest::setRefreshToken, new ParseField("refresh_token"));
    }

    public RestOpenIdConnectLogoutAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/oidc/logout"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final OpenIdConnectLogoutRequest logoutRequest = PARSER.parse(parser, null);
            return channel -> client.execute(
                OpenIdConnectLogoutAction.INSTANCE,
                logoutRequest,
                new RestBuilderListener<OpenIdConnectLogoutResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(OpenIdConnectLogoutResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        builder.field("redirect", response.getEndSessionUrl());
                        builder.endObject();
                        return new RestResponse(RestStatus.OK, builder);
                    }
                }
            );
        }
    }

    @Override
    public String getName() {
        return "security_oidc_logout_action";
    }
}
