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
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateResponse;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest handler that authenticates the user based on the information provided as parameters of the redirect_uri
 */
public class RestOpenIdConnectAuthenticateAction extends OpenIdConnectBaseRestHandler {

    static final ObjectParser<OpenIdConnectAuthenticateRequest, Void> PARSER = new ObjectParser<>("oidc_authn",
        OpenIdConnectAuthenticateRequest::new);

    static {
        PARSER.declareString(OpenIdConnectAuthenticateRequest::setRedirectUri, new ParseField("redirect_uri"));
        PARSER.declareString(OpenIdConnectAuthenticateRequest::setState, new ParseField("state"));
        PARSER.declareString(OpenIdConnectAuthenticateRequest::setNonce, new ParseField("nonce"));
        PARSER.declareStringOrNull(OpenIdConnectAuthenticateRequest::setRealm, new ParseField("realm"));
    }

    public RestOpenIdConnectAuthenticateAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(POST, "/_security/oidc/authenticate", this);
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final OpenIdConnectAuthenticateRequest authenticateRequest = PARSER.parse(parser, null);
            logger.trace("OIDC Authenticate: " + authenticateRequest);
            return channel -> client.execute(OpenIdConnectAuthenticateAction.INSTANCE, authenticateRequest,
                new RestBuilderListener<OpenIdConnectAuthenticateResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(OpenIdConnectAuthenticateResponse response, XContentBuilder builder)
                        throws Exception {
                        builder.startObject()
                            .field("username", response.getPrincipal())
                            .field("access_token", response.getAccessTokenString())
                            .field("refresh_token", response.getRefreshTokenString())
                            .field("expires_in", response.getExpiresIn().seconds())
                            .endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
        }
    }

    @Override
    public String getName() {
        return "security_oidc_authenticate_action";
    }
}
