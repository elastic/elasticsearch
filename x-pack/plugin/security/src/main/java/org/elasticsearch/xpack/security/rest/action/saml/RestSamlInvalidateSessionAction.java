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
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Invalidates any security tokens associated with the provided SAML session.
 * The session identity is provided in a SAML {@code &lt;LogoutRequest&gt;}
 */
@ServerlessScope(Scope.INTERNAL)
public class RestSamlInvalidateSessionAction extends SamlBaseRestHandler {

    static final ObjectParser<SamlInvalidateSessionRequest, RestSamlInvalidateSessionAction> PARSER = new ObjectParser<>(
        "saml_invalidate_session",
        SamlInvalidateSessionRequest::new
    );

    static {
        PARSER.declareString(SamlInvalidateSessionRequest::setQueryString, new ParseField("query_string", "queryString"));
        PARSER.declareString(SamlInvalidateSessionRequest::setAssertionConsumerServiceURL, new ParseField("acs"));
        PARSER.declareString(SamlInvalidateSessionRequest::setRealmName, new ParseField("realm"));
    }

    public RestSamlInvalidateSessionAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/saml/invalidate"));
    }

    @Override
    public String getName() {
        return "security_saml_invalidate_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final SamlInvalidateSessionRequest invalidateRequest = PARSER.parse(parser, this);
            return channel -> client.execute(
                SamlInvalidateSessionAction.INSTANCE,
                invalidateRequest,
                new RestBuilderListener<SamlInvalidateSessionResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(SamlInvalidateSessionResponse resp, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        builder.field("realm", resp.getRealmName());
                        builder.field("invalidated", resp.getCount());
                        builder.field("redirect", resp.getRedirectUrl());
                        builder.endObject();
                        return new RestResponse(RestStatus.OK, builder);
                    }
                }
            );
        }
    }

}
