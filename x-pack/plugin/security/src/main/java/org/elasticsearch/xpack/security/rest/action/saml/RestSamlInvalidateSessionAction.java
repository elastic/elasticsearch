/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.saml;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.DeprecationLogger;
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
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Invalidates any security tokens associated with the provided SAML session.
 * The session identity is provided in a SAML {@code &lt;LogoutRequest&gt;}
 */
public class RestSamlInvalidateSessionAction extends SamlBaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestSamlInvalidateSessionAction.class));
    static final ObjectParser<SamlInvalidateSessionRequest, RestSamlInvalidateSessionAction> PARSER =
            new ObjectParser<>("saml_invalidate_session", SamlInvalidateSessionRequest::new);

    static {
        PARSER.declareString(SamlInvalidateSessionRequest::setQueryString, new ParseField("queryString"));
        PARSER.declareString(SamlInvalidateSessionRequest::setAssertionConsumerServiceURL, new ParseField("acs"));
        PARSER.declareString(SamlInvalidateSessionRequest::setRealmName, new ParseField("realm"));
    }

    public RestSamlInvalidateSessionAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return Collections.singletonList(
            new ReplacedRoute(POST, "/_security/saml/invalidate",
                POST, "/_xpack/security/saml/invalidate", deprecationLogger)
        );
    }

    @Override
    public String getName() {
        return "security_saml_invalidate_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final SamlInvalidateSessionRequest invalidateRequest = PARSER.parse(parser, this);
            return channel -> client.execute(SamlInvalidateSessionAction.INSTANCE, invalidateRequest,
                    new RestBuilderListener<SamlInvalidateSessionResponse>(channel) {
                        @Override
                        public RestResponse buildResponse(SamlInvalidateSessionResponse resp, XContentBuilder builder) throws Exception {
                            builder.startObject();
                            builder.field("realm", resp.getRealmName());
                            builder.field("invalidated", resp.getCount());
                            builder.field("redirect", resp.getRedirectUrl());
                            builder.endObject();
                            return new BytesRestResponse(RestStatus.OK, builder);
                        }
                    });
        }
    }

}
