/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.oauth2;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.action.token.RefreshTokenAction;
import org.elasticsearch.xpack.security.authc.kerberos.KerberosAuthenticationToken;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * An implementation of a OAuth2-esque API for retrieval of an access token.
 * This API does not conform to the RFC completely as it uses XContent for the request body
 * instead for form encoded data. This is a relatively common modification of the OAuth2
 * specification as this aspect does not make the most sense since the response body is
 * expected to be JSON
 */
public final class RestGetTokenAction extends TokenBaseRestHandler implements RestRequestFilter {

    static final ConstructingObjectParser<CreateTokenRequest, Void> PARSER = new ConstructingObjectParser<>("token_request",
            a -> new CreateTokenRequest((String) a[0], (String) a[1], (SecureString) a[2], (SecureString) a[3], (String) a[4],
                    (String) a[5]));
    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("grant_type"));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("username"));
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), parser -> new SecureString(
                Arrays.copyOfRange(parser.textCharacters(), parser.textOffset(), parser.textOffset() + parser.textLength())),
                new ParseField("password"), ValueType.STRING);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), parser -> new SecureString(
                Arrays.copyOfRange(parser.textCharacters(), parser.textOffset(), parser.textOffset() + parser.textLength())),
                new ParseField("kerberos_ticket"), ValueType.STRING);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("scope"));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("refresh_token"));
    }

    public RestGetTokenAction(Settings settings, XPackLicenseState xPackLicenseState) {
        super(settings, xPackLicenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, "/_security/oauth2/token")
                .replaces(POST, "/_xpack/security/oauth2/token", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "security_get_token_action";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client)throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final CreateTokenRequest tokenRequest = PARSER.parse(parser, null);
            final ActionType<CreateTokenResponse> action =
                    "refresh_token".equals(tokenRequest.getGrantType()) ? RefreshTokenAction.INSTANCE : CreateTokenAction.INSTANCE;
            return channel -> client.execute(action, tokenRequest,
                    // this doesn't use the RestBuilderListener since we need to override the
                    // handling of failures in some cases.
                    new CreateTokenResponseActionListener(channel, request, logger));
        }
    }

    static class CreateTokenResponseActionListener implements ActionListener<CreateTokenResponse> {

        private final RestChannel channel;
        private final RestRequest request;
        private final Logger logger;

        CreateTokenResponseActionListener(RestChannel restChannel, RestRequest restRequest,
                                          Logger logger) {
            this.channel = restChannel;
            this.request = restRequest;
            this.logger = logger;
        }

        @Override
        public void onResponse(CreateTokenResponse createTokenResponse) {
            try (XContentBuilder builder = channel.newBuilder()) {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, createTokenResponse.toXContent(builder, request)));
            } catch (IOException e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.debug("Failed to create token", e);
            if (e instanceof ActionRequestValidationException) {
                ActionRequestValidationException validationException = (ActionRequestValidationException) e;
                final TokenRequestError error;
                if (validationException.validationErrors().stream().anyMatch(s -> s.contains("grant_type"))) {
                    error = TokenRequestError.UNSUPPORTED_GRANT_TYPE;
                } else {
                    error = TokenRequestError.INVALID_REQUEST;
                }

                sendTokenErrorResponse(error, validationException.getMessage(), e);
            } else if (e instanceof ElasticsearchSecurityException && "invalid_grant".equals(e.getMessage()) &&
                    ((ElasticsearchSecurityException) e).getHeader("error_description").size() == 1) {
                sendTokenErrorResponse(TokenRequestError.INVALID_GRANT,
                        ((ElasticsearchSecurityException) e).getHeader("error_description").get(0), e);
            } else if (e instanceof ElasticsearchSecurityException
                    && "failed to authenticate user, gss context negotiation not complete".equals(e.getMessage())) {
                sendTokenErrorResponse(TokenRequestError._UNAUTHORIZED, extractBase64EncodedToken((ElasticsearchSecurityException) e), e);
            } else {
                sendFailure(e);
            }
        }

        private String extractBase64EncodedToken(ElasticsearchSecurityException e) {
            String base64EncodedToken = null;
            List<String> values = e.getHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE);
            if (values != null && values.size() == 1) {
                final String wwwAuthenticateHeaderValue = values.get(0);
                // it may contain base64 encoded token that needs to be sent to client if Spnego GSS context negotiation failed
                if (wwwAuthenticateHeaderValue.startsWith(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX)) {
                    base64EncodedToken = wwwAuthenticateHeaderValue
                            .substring(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX.length()).trim();
                }
            }
            return base64EncodedToken;
        }

        void sendTokenErrorResponse(TokenRequestError error, String description, Exception e) {
            try (XContentBuilder builder = channel.newErrorBuilder()) {
                // defined by https://tools.ietf.org/html/rfc6749#section-5.2
                builder.startObject()
                        .field("error", error.toString().toLowerCase(Locale.ROOT))
                        .field("error_description", description)
                        .endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
            } catch (IOException ioe) {
                ioe.addSuppressed(e);
                sendFailure(e);
            }
        }

        void sendFailure(Exception e) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("failed to send failure response", inner);
            }
        }
    }

    // defined by https://tools.ietf.org/html/rfc6749#section-5.2
    enum TokenRequestError {
        /**
         * The request is missing a required parameter, includes an unsupported
         * parameter value (other than grant type), repeats a parameter,
         * includes multiple credentials, utilizes more than one mechanism for
         * authenticating the client, or is otherwise malformed.
         */
        INVALID_REQUEST,

        /**
         * Client authentication failed (e.g., unknown client, no client
         * authentication included, or unsupported authentication method).  The
         * authorization server MAY return an HTTP 401 (Unauthorized) status
         * code to indicate which HTTP authentication schemes are supported. If
         * the client attempted to authenticate via the "Authorization" request
         * header field, the authorization server MUST respond with an HTTP 401
         * (Unauthorized) status code and include the "WWW-Authenticate"
         * response header field matching the authentication scheme used by the
         * client.
         */
        INVALID_CLIENT,

        /**
         * The provided authorization grant (e.g., authorization code, resource
         * owner credentials) or refresh token is invalid, expired, revoked,
         * does not match the redirection URI used in the authorization request,
         * or was issued to another client.
         */
        INVALID_GRANT,

        /**
         * The authenticated client is not authorized to use this authorization
         * grant type.
         */
        UNAUTHORIZED_CLIENT,

        /**
         * The authorization grant type is not supported by the authorization
         * server.
         */
        UNSUPPORTED_GRANT_TYPE,

        /**
         * The requested scope is invalid, unknown, malformed, or exceeds the
         * scope granted by the resource owner.
         */
        INVALID_SCOPE,

        // Custom error code
        /**
         * When the request for authentication fails using custom grant type for given
         * credentials.
         * If the client attempted to authenticate via the "Authorization" request
         * the authorization server MAY respond with an HTTP 401
         * (Unauthorized) status code and include the "WWW-Authenticate"
         * response header field
         */
        _UNAUTHORIZED,
    }

    private static final Set<String> FILTERED_FIELDS = Set.of("password", "kerberos_ticket", "refresh_token");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}
