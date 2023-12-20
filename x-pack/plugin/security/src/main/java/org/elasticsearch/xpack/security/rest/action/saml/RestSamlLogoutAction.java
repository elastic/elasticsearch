/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.saml;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
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
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Invalidates the provided security token, and if the associated SAML realm support logout, generates
 * a SAML logout request ({@code &lt;LogoutRequest&gt;}).
 * This logout request is returned in the REST response as a redirect URI, and the REST client should
 * make it available to the browser.
 */
@ServerlessScope(Scope.INTERNAL)
public class RestSamlLogoutAction extends SamlBaseRestHandler {

    static final ObjectParser<SamlLogoutRequest, Void> PARSER = new ObjectParser<>("saml_logout", SamlLogoutRequest::new);

    static {
        PARSER.declareString(SamlLogoutRequest::setToken, new ParseField("token"));
        PARSER.declareString(SamlLogoutRequest::setRefreshToken, new ParseField("refresh_token"));
    }

    public RestSamlLogoutAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, "/_security/saml/logout").replaces(POST, "/_xpack/security/saml/logout", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "security_saml_logout_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final SamlLogoutRequest logoutRequest = PARSER.parse(parser, null);
            return channel -> client.execute(
                SamlLogoutAction.INSTANCE,
                logoutRequest,
                new RestBuilderListener<SamlLogoutResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(SamlLogoutResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
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
