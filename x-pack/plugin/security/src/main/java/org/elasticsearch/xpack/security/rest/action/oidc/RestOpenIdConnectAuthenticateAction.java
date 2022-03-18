/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.oidc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateResponse;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest handler that authenticates the user based on the information provided as parameters of the redirect_uri
 */
public class RestOpenIdConnectAuthenticateAction extends OpenIdConnectBaseRestHandler implements RestRequestFilter {
    private static final Logger logger = LogManager.getLogger(RestOpenIdConnectAuthenticateAction.class);

    static final ObjectParser<OpenIdConnectAuthenticateRequest, Void> PARSER = new ObjectParser<>(
        "oidc_authn",
        OpenIdConnectAuthenticateRequest::new
    );

    static {
        PARSER.declareString(OpenIdConnectAuthenticateRequest::setRedirectUri, new ParseField("redirect_uri"));
        PARSER.declareString(OpenIdConnectAuthenticateRequest::setState, new ParseField("state"));
        PARSER.declareString(OpenIdConnectAuthenticateRequest::setNonce, new ParseField("nonce"));
        PARSER.declareStringOrNull(OpenIdConnectAuthenticateRequest::setRealm, new ParseField("realm"));
    }

    public RestOpenIdConnectAuthenticateAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/oidc/authenticate"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final OpenIdConnectAuthenticateRequest authenticateRequest = PARSER.parse(parser, null);
            logger.trace("OIDC Authenticate: " + authenticateRequest);
            return channel -> client.execute(
                OpenIdConnectAuthenticateAction.INSTANCE,
                authenticateRequest,
                new RestBuilderListener<OpenIdConnectAuthenticateResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(OpenIdConnectAuthenticateResponse response, XContentBuilder builder)
                        throws Exception {
                        builder.startObject();
                        builder.field("username", response.getPrincipal());
                        builder.field("access_token", response.getAccessTokenString());
                        builder.field("refresh_token", response.getRefreshTokenString());
                        builder.field("expires_in", response.getExpiresIn().seconds());
                        if (response.getAuthentication() != null) {
                            builder.field("authentication", response.getAuthentication());
                        }
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                }
            );
        }
    }

    @Override
    public String getName() {
        return "security_oidc_authenticate_action";
    }

    private static final Set<String> FILTERED_FIELDS = Set.of("redirect_uri");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}
