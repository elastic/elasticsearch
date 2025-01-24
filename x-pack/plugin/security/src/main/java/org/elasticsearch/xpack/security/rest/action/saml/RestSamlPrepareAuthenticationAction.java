/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.saml;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Generates a SAML authentication request ({@code <AuthnRequest>}) based on the provided
 * parameters.
 * The request is returned in the REST response, and the REST client should make it available
 * to the browser.
 */
@ServerlessScope(Scope.INTERNAL)
public class RestSamlPrepareAuthenticationAction extends SamlBaseRestHandler {

    static final ObjectParser<SamlPrepareAuthenticationRequest, Void> PARSER = new ObjectParser<>(
        "saml_prepare_authn",
        SamlPrepareAuthenticationRequest::new
    );

    static {
        PARSER.declareString(SamlPrepareAuthenticationRequest::setAssertionConsumerServiceURL, new ParseField("acs"));
        PARSER.declareString(SamlPrepareAuthenticationRequest::setRealmName, new ParseField("realm"));
        PARSER.declareString(SamlPrepareAuthenticationRequest::setRelayState, new ParseField("relay_state"));
    }

    public RestSamlPrepareAuthenticationAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/saml/prepare"));
    }

    @Override
    public String getName() {
        return "security_saml_prepare_authentication_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final SamlPrepareAuthenticationRequest authenticationRequest = PARSER.parse(parser, null);
            return channel -> client.execute(
                SamlPrepareAuthenticationAction.INSTANCE,
                authenticationRequest,
                new RestBuilderListener<SamlPrepareAuthenticationResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(SamlPrepareAuthenticationResponse response, XContentBuilder builder)
                        throws Exception {
                        builder.startObject();
                        builder.field("realm", response.getRealmName());
                        builder.field("id", response.getRequestId());
                        builder.field("redirect", response.getRedirectUrl());
                        builder.endObject();
                        return new RestResponse(RestStatus.OK, builder);
                    }
                }
            );
        }
    }
}
