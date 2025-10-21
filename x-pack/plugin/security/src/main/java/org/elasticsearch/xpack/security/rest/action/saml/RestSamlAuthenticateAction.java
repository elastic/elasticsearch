/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.saml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateRequestBuilder;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateResponse;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * A REST handler that attempts to authenticate a user based on the provided SAML response/assertion.
 */
@ServerlessScope(Scope.INTERNAL)
public class RestSamlAuthenticateAction extends SamlBaseRestHandler implements RestRequestFilter {
    private static final Logger logger = LogManager.getLogger(RestSamlAuthenticateAction.class);

    static class Input {
        String content;
        List<String> ids;
        String realm;

        void setContent(String content) {
            this.content = content;
        }

        void setIds(List<String> ids) {
            this.ids = ids;
        }

        void setRealm(String realm) {
            this.realm = realm;
        }
    }

    static final ObjectParser<Input, Void> PARSER = new ObjectParser<>("saml_authenticate", Input::new);

    static {
        PARSER.declareString(Input::setContent, new ParseField("content"));
        PARSER.declareStringArray(Input::setIds, new ParseField("ids"));
        PARSER.declareStringOrNull(Input::setRealm, new ParseField("realm"));
    }

    public RestSamlAuthenticateAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/saml/authenticate"));
    }

    @Override
    public String getName() {
        return "security_saml_authenticate_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final Input input = PARSER.parse(parser, null);
            logger.trace("SAML Authenticate: [{}...] [{}]", Strings.cleanTruncate(input.content, 128), input.ids);
            return channel -> {
                final byte[] bytes = decodeBase64(input.content);
                final SamlAuthenticateRequestBuilder requestBuilder = new SamlAuthenticateRequestBuilder(client).saml(bytes)
                    .validRequestIds(input.ids)
                    .authenticatingRealm(input.realm);
                requestBuilder.execute(new RestBuilderListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(SamlAuthenticateResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        builder.field("username", response.getPrincipal());
                        builder.field("realm", response.getRealm());
                        builder.field("access_token", response.getTokenString());
                        builder.field("refresh_token", response.getRefreshToken());
                        builder.field("expires_in", response.getExpiresIn().seconds());
                        if (response.getAuthentication() != null) {
                            builder.field("authentication", response.getAuthentication());
                        }
                        builder.endObject();
                        return new RestResponse(RestStatus.OK, builder);
                    }
                });
            };
        }
    }

    private static byte[] decodeBase64(String content) {
        content = content.replaceAll("\\s+", "");
        try {
            return Base64.getDecoder().decode(content);
        } catch (IllegalArgumentException e) {
            logger.info("Failed to decode base64 string [{}] - {}", content, e.toString());
            throw e;
        }
    }

    private static final Set<String> FILTERED_FIELDS = Set.of("content");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}
