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
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationResponse;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Generates an oAuth 2.0 authentication request as a URL string and returns it to the REST client.
 */
public class RestOpenIdConnectPrepareAuthenticationAction extends OpenIdConnectBaseRestHandler {

    static final ObjectParser<OpenIdConnectPrepareAuthenticationRequest, Void> PARSER = new ObjectParser<>("oidc_prepare_auithentication",
        OpenIdConnectPrepareAuthenticationRequest::new);

    static {
        PARSER.declareString(OpenIdConnectPrepareAuthenticationRequest::setRealmName, new ParseField("realm"));
    }

    public RestOpenIdConnectPrepareAuthenticationAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(POST, "/_xpath/security/oidc/prepare", this);
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final OpenIdConnectPrepareAuthenticationRequest authenticationRequest = PARSER.parse(parser, null);
            return channel -> client.execute(OpenIdConnectPrepareAuthenticationAction.INSTANCE, authenticationRequest,
                new RestBuilderListener<OpenIdConnectPrepareAuthenticationResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(OpenIdConnectPrepareAuthenticationResponse actionResponse, XContentBuilder builder)
                        throws Exception {
                        builder.startObject();
                        builder.field("state");
                        builder.field("redirect");
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
        }
    }

    @Override
    public String getName() {
        return "xpack_security_oidc_prepare_authentication_action";
    }
}
