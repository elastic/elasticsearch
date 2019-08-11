/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.oidc;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutResponse;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest handler that invalidates a security token for the given OpenID Connect realm and if the configuration of
 * the realm supports it, generates a redirect to the `end_session_endpoint` of the OpenID Connect Provider.
 */
public class RestOpenIdConnectLogoutAction extends OpenIdConnectBaseRestHandler {

    static final ObjectParser<OpenIdConnectLogoutRequest, Void> PARSER = new ObjectParser<>("oidc_logout",
        OpenIdConnectLogoutRequest::new);

    static {
        PARSER.declareString(OpenIdConnectLogoutRequest::setToken, new ParseField("token"));
        PARSER.declareString(OpenIdConnectLogoutRequest::setRefreshToken, new ParseField("refresh_token"));
    }

    public RestOpenIdConnectLogoutAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(POST, "/_security/oidc/logout", this);
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final OpenIdConnectLogoutRequest logoutRequest = PARSER.parse(parser, null);
            return channel -> client.execute(OpenIdConnectLogoutAction.INSTANCE, logoutRequest,
                new RestBuilderListener<OpenIdConnectLogoutResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(OpenIdConnectLogoutResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        builder.field("redirect", response.getEndSessionUrl());
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
        }
    }

    @Override
    public String getName() {
        return "security_oidc_logout_action";
    }
}
