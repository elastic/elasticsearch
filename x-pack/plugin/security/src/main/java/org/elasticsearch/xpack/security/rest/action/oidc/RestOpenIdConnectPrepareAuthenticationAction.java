/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.oidc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Generates an oAuth 2.0 authentication request as a URL string and returns it to the REST client.
 */
public class RestOpenIdConnectPrepareAuthenticationAction extends OpenIdConnectBaseRestHandler {
    private static final Logger logger = LogManager.getLogger();

    static final ObjectParser<OpenIdConnectPrepareAuthenticationRequest, Void> PARSER = new ObjectParser<>("oidc_prepare_authentication",
        OpenIdConnectPrepareAuthenticationRequest::new);

    static {
        PARSER.declareString(OpenIdConnectPrepareAuthenticationRequest::setRealmName, new ParseField("realm"));
        PARSER.declareString(OpenIdConnectPrepareAuthenticationRequest::setIssuer, new ParseField("iss"));
        PARSER.declareString(OpenIdConnectPrepareAuthenticationRequest::setLoginHint, new ParseField("login_hint"));
        PARSER.declareString(OpenIdConnectPrepareAuthenticationRequest::setState, new ParseField("state"));
        PARSER.declareString(OpenIdConnectPrepareAuthenticationRequest::setNonce, new ParseField("nonce"));
    }

    public RestOpenIdConnectPrepareAuthenticationAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/oidc/prepare"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final OpenIdConnectPrepareAuthenticationRequest prepareAuthenticationRequest = PARSER.parse(parser, null);
            logger.trace("OIDC Prepare Authentication: " + prepareAuthenticationRequest);
            return channel -> client.execute(OpenIdConnectPrepareAuthenticationAction.INSTANCE, prepareAuthenticationRequest,
                new RestBuilderListener<OpenIdConnectPrepareAuthenticationResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(OpenIdConnectPrepareAuthenticationResponse response, XContentBuilder builder)
                        throws Exception {
                        logger.trace("OIDC Prepare Authentication Response: " + response);
                        return new BytesRestResponse(RestStatus.OK, response.toXContent(builder, request));
                    }
                });
        }
    }

    @Override
    public String getName() {
        return "security_oidc_prepare_authentication_action";
    }
}
