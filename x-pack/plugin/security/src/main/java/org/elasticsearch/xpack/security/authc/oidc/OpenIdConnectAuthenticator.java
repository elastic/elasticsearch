/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jose.util.IOUtils;
import com.nimbusds.jose.util.Resource;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ErrorObject;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.openid.connect.sdk.AuthenticationErrorResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponseParser;
import com.nimbusds.openid.connect.sdk.AuthenticationSuccessResponse;
import com.nimbusds.openid.connect.sdk.Nonce;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponseParser;
import com.nimbusds.openid.connect.sdk.UserInfoErrorResponse;
import com.nimbusds.openid.connect.sdk.UserInfoRequest;
import com.nimbusds.openid.connect.sdk.UserInfoResponse;
import com.nimbusds.openid.connect.sdk.UserInfoSuccessResponse;
import com.nimbusds.openid.connect.sdk.claims.AccessTokenHash;
import com.nimbusds.openid.connect.sdk.token.OIDCTokens;
import com.nimbusds.openid.connect.sdk.validators.AccessTokenValidator;
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator;
import net.minidev.json.JSONObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Handles an OpenID Connect Authentication response as received by the facilitator. In the case of an implicit flow, validates
 * the ID Token and extracts the elasticsearch user properties from it. In the case of an authorization code flow, it first
 * exchanges the code in the authentication response for an ID Token at the token endpoint of the OpenID Connect Provider.
 */
public class OpenIdConnectAuthenticator {

    private final RealmConfig realmConfig;
    private final OpenIdConnectProviderConfiguration opConfig;
    private final RelyingPartyConfiguration rpConfig;
    private final SSLService sslService;

    protected final Logger logger = LogManager.getLogger(getClass());

    OpenIdConnectAuthenticator(RealmConfig realmConfig, OpenIdConnectProviderConfiguration opConfig, RelyingPartyConfiguration rpConfig,
                               SSLService sslService) {
        this.realmConfig = realmConfig;
        this.opConfig = opConfig;
        this.rpConfig = rpConfig;
        this.sslService = sslService;
    }

    /**
     * Processes an OpenID Connect Response to an Authentication Request that comes in the form of a URL with the necessary parameters, that
     * is contained in the provided Token. If the response is valid, it returns a set of OpenID Connect claims that identify the
     * authenticated user. If the UserInfo endpoint is specified in the configuration, we attempt to make a UserInfo request and add
     * the returned claims.
     *
     * @param token The OpenIdConnectToken to consume
     * @return a {@link JWTClaimsSet} with the OP claims for the user
     * @throws ElasticsearchSecurityException if the response is invalid in any way
     */
    public JWTClaimsSet authenticate(OpenIdConnectToken token) {
        try {
            AuthenticationResponse authenticationResponse = AuthenticationResponseParser.parse(new URI(token.getRedirectUrl()));
            Nonce expectedNonce = new Nonce(token.getNonce());
            State expectedState = new State(token.getState());
            if (logger.isTraceEnabled()) {
                logger.trace("OpenID Connect Provider redirected user to [{}]. Expected Nonce is [{}] and expected State is [{}]",
                    token.getRedirectUrl(), expectedNonce, expectedState);
            }
            if (authenticationResponse instanceof AuthenticationErrorResponse) {
                ErrorObject error = ((AuthenticationErrorResponse) authenticationResponse).getErrorObject();
                throw new ElasticsearchSecurityException("OpenID Connect Provider response indicates authentication failure." +
                    "Code=[{}], Description=[{}]", error.getCode(), error.getDescription());
            }
            final AuthenticationSuccessResponse response = authenticationResponse.toSuccessResponse();
            validateState(expectedState, response.getState());
            validateResponseType(response);
            JWT idToken;
            AccessToken accessToken;
            if (rpConfig.getResponseType().impliesCodeFlow()) {
                final AuthorizationCode code = response.getAuthorizationCode();
                Tuple<AccessToken, JWT> tokens = exchangeCodeForToken(code);
                accessToken = tokens.v1();
                idToken = tokens.v2();
                validateAccessToken(accessToken, idToken, true);
            } else {
                idToken = response.getIDToken();
                accessToken = response.getAccessToken();
                validateAccessToken(accessToken, idToken, false);
            }
            final JWTClaimsSet idTokenClaims = validateAndParseIdToken(idToken, expectedNonce);
            if (opConfig.getAuthorizationEndpoint() != null) {
                final JWTClaimsSet userInfoClaims = getUserInfo(accessToken);
                final JSONObject combinedClaims = idTokenClaims.toJSONObject();
                combinedClaims.merge(userInfoClaims.toJSONObject());
                return (JWTClaimsSet.parse(combinedClaims));
            } else {
                return idTokenClaims;
            }

        } catch (ElasticsearchSecurityException e) {
            // Don't wrap in a new ElasticsearchSecurityException
            throw e;
        } catch (Exception e) {
            logger.debug("Failed to consume the OpenID connect response", e);
            throw new ElasticsearchSecurityException("Failed to consume the OpenID connect response");
        }
    }

    /**
     * Validates an access token according to the
     * <a href="https://openid.net/specs/openid-connect-core-1_0.html#ImplicitTokenValidation">specification</a>
     *
     * @param accessToken The Access Token to validate
     * @param idToken     The Id Token that was received in the same response
     * @param optional    When using the authorization code flow the OP might not provide the at_hash parameter in the
     *                    Id Token as allowed in the specification. In such a case we can't validate the access token
     *                    but this is considered safe as it was received in a back channel communication that was protected
     *                    by TLS.
     */
    private void validateAccessToken(AccessToken accessToken, JWT idToken, boolean optional) {
        try {
            // only "bearer" is defined in the specification but check just in case
            if (accessToken.getType().equals("bearer") == false) {
                logger.debug("Invalid access token type [{}], while [bearer] was expected", accessToken.getType());
                throw new ElasticsearchSecurityException("Received a response with an invalid state parameter");
            }
            AccessTokenHash atHash = new AccessTokenHash(idToken.getJWTClaimsSet().getStringClaim("at_hash"));
            if (null == atHash && optional == false) {
                logger.debug("Failed to verify access token. at_hash claim is missing from the ID Token");
                throw new ElasticsearchSecurityException("Failed to verify access token");
            }
            JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(idToken.getHeader().getAlgorithm().getName());
            AccessTokenValidator.validate(accessToken, jwsAlgorithm, atHash);
        } catch (Exception e) {
            logger.debug("Failed to verify access token.", e);
            throw new ElasticsearchSecurityException("Failed to verify access token.");
        }
    }

    /**
     * Parses and validates an OpenID Connect Id Token to a set of claims
     *
     * @param idToken       The Id Token to parse and validate
     * @param expectedNonce The nonce that was generated in the beginning of this authentication attempt and was stored at the user's
     *                      session with the facilitator
     * @return a {@link JWTClaimsSet} with the OP claims that were contained in the Id Token
     */
    private JWTClaimsSet validateAndParseIdToken(JWT idToken, Nonce expectedNonce) {
        Secret clientSecret = null;
        try {
            final IDTokenValidator validator;
            final JWSAlgorithm requestedAlgorithm = rpConfig.getSignatureVerificationAlgorithm();
            if (JWSAlgorithm.Family.HMAC_SHA.contains(requestedAlgorithm)) {
                clientSecret = new Secret(rpConfig.getClientSecret().toString());
                validator = new IDTokenValidator(opConfig.getIssuer(), rpConfig.getClientId(), requestedAlgorithm, clientSecret);
            } else {
                validator = new IDTokenValidator(opConfig.getIssuer(), rpConfig.getClientId(), requestedAlgorithm,
                    opConfig.getJwkSetUrl(), getPrivilegedResourceRetriever());
            }
            JWTClaimsSet verifiedIdTokenClaims = validator.validate(idToken, expectedNonce).toJWTClaimsSet();
            if (logger.isTraceEnabled()) {
                logger.trace("Received the Id Token for the user: [{}]", verifiedIdTokenClaims);
            }
            return verifiedIdTokenClaims;

        } catch (Exception e) {
            logger.debug("Failed to parse or validate the ID Token. ", e);
            throw new ElasticsearchSecurityException("Failed to parse or validate the ID Token");
        } finally {
            if (null != clientSecret) {
                clientSecret.erase();
            }
        }
    }

    /**
     * Validate that the response we received corresponds to the response type we requested
     *
     * @param response The {@link AuthenticationSuccessResponse} we received
     * @throws ElasticsearchSecurityException if the response is not the expected one for the configured response type
     */
    private void validateResponseType(AuthenticationSuccessResponse response) {
        if (rpConfig.getResponseType().equals(response.impliedResponseType()) == false) {
            logger.debug("Unexpected response type [{}], while [{}] is configured", response.impliedResponseType(),
                rpConfig.getResponseType());
            throw new ElasticsearchSecurityException("Received a response with an unexpected response type");
        }
    }

    /**
     * Validate that the state parameter the response contained corresponds to the one that we generated in the
     * beginning of this authentication attempt and was stored with the user's session at the facilitator
     *
     * @param expectedState The state that was originally generated
     * @param state         The state that was contained in the response
     */
    private void validateState(State expectedState, State state) {
        if (null == state || null == expectedState) {
            logger.debug("Failed to validate the response, at least one of the stored [{}] or received [{}] values were empty. ", state,
                expectedState);
            throw new ElasticsearchSecurityException("Failed to validate the response, state parameter is missing.");
        } else if (state.equals(expectedState) == false) {
            logger.debug("Invalid state parameter [{}], while [{}] was expected", state, expectedState);
            throw new ElasticsearchSecurityException("Received a response with an invalid state parameter.");
        }
    }

    /**
     * Makes a {@link HTTPRequest} using the appropriate TLS configuration and returns the associated {@link HTTPResponse}
     */
    private HTTPResponse getResponse(HTTPRequest httpRequest) throws PrivilegedActionException {
        final String sslKey = RealmSettings.realmSslPrefix(realmConfig.identifier());
        final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration(sslKey);
        boolean isHostnameVerificationEnabled = sslConfiguration.verificationMode().isHostnameVerificationEnabled();
        final HostnameVerifier verifier = isHostnameVerificationEnabled ? new DefaultHostnameVerifier() : NoopHostnameVerifier.INSTANCE;
        httpRequest.setSSLSocketFactory(sslService.sslSocketFactory(sslConfiguration));
        httpRequest.setHostnameVerifier(verifier);

        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedExceptionAction<HTTPResponse>) httpRequest::send);
    }

    /**
     * Completes the Authorization Code Grant authentication flow of OpenId Connect by exchanging the received
     * authorization code for an Id Token and an access token.
     *
     * @param code the authorization code that was received as a response to the authentication request
     * @return a {@link Tuple} containing the received (yet not validated) {@link AccessToken} and {@link JWT}
     */
    private Tuple<AccessToken, JWT> exchangeCodeForToken(AuthorizationCode code) {
        Secret clientSecret = null;
        try {
            clientSecret = new Secret(rpConfig.getClientSecret().toString());
            final ClientAuthentication clientAuth = new ClientSecretBasic(rpConfig.getClientId(), clientSecret);
            final AuthorizationGrant codeGrant = new AuthorizationCodeGrant(code, rpConfig.getRedirectUri());
            final TokenRequest request = new TokenRequest(opConfig.getTokenEndpoint(), clientAuth, codeGrant);
            final HTTPResponse httpResponse = getResponse(request.toHTTPRequest());
            final TokenResponse tokenResponse = OIDCTokenResponseParser.parse(httpResponse);
            if (tokenResponse.indicatesSuccess() == false) {
                TokenErrorResponse errorResponse = (TokenErrorResponse) tokenResponse;
                logger.debug("Failed to exchange code for Id Token. Code=[{}], Description=[{}]",
                    errorResponse.getErrorObject().getCode(), errorResponse.getErrorObject().getDescription());
                throw new ElasticsearchSecurityException("Failed to exchange code for Id Token.");
            }
            OIDCTokenResponse successResponse = (OIDCTokenResponse) tokenResponse.toSuccessResponse();
            if (logger.isTraceEnabled()) {
                logger.trace("Successfully exchanged code for ID Token: [{}]", successResponse.toJSONObject().toJSONString());
            }
            final OIDCTokens oidcTokens = successResponse.getOIDCTokens();
            final AccessToken accessToken = oidcTokens.getAccessToken();
            final JWT idToken = oidcTokens.getIDToken();
            if (idToken == null) {
                logger.debug("Failed to parse the received Id Token to a JWT");
                throw new ElasticsearchSecurityException("Failed to exchange code for Id Token.");
            }
            return new Tuple<>(accessToken, idToken);
        } catch (Exception e) {
            logger.debug("Failed to exchange code for Id Token using the Token Endpoint. ", e);
            throw new ElasticsearchSecurityException("Failed to exchange code for Id Token.");
        } finally {
            if (null != clientSecret) {
                clientSecret.erase();
            }
        }
    }

    /**
     * Makes a request to the UserInfo Endpoint of the OP
     *
     * @param accessToken the access token to authenticate
     * @return a {@link JWTClaimsSet} with the claims returned
     */
    private JWTClaimsSet getUserInfo(AccessToken accessToken) {
        try {
            final BearerAccessToken bearerToken = new BearerAccessToken(accessToken.getValue());
            final HTTPRequest httpRequest = new UserInfoRequest(opConfig.getUserinfoEndpoint(), bearerToken).toHTTPRequest();
            final HTTPResponse httpResponse = getResponse(httpRequest);
            final UserInfoResponse response = UserInfoResponse.parse(httpResponse);
            if (response.indicatesSuccess() == false) {
                UserInfoErrorResponse errorResponse = response.toErrorResponse();
                logger.debug("Failed to get user information from the UserInfo endpoint. Code=[{}], " +
                    "Description=[{}]", errorResponse.getErrorObject().getCode(), errorResponse.getErrorObject().getDescription());
                throw new ElasticsearchSecurityException("Failed to get user information from the UserInfo endpoint");
            }
            UserInfoSuccessResponse successResponse = response.toSuccessResponse();
            if (logger.isTraceEnabled()) {
                logger.trace("Successfully retrieved user information: [{}]", successResponse.getUserInfo().toJSONObject().toJSONString());
            }
            return successResponse.getUserInfoJWT().getJWTClaimsSet();
        } catch (Exception e) {
            logger.debug("FFailed to get user information from the UserInfo endpoint. ", e);
            throw new ElasticsearchSecurityException("Failed to get user information from the UserInfo endpoint.");
        }
    }

    /**
     * Creates a new {@link PrivilegedResourceRetriever} to be used with the {@link IDTokenValidator} by passing the
     * necessary client SSLContext and hostname verification configuration
     */
    private PrivilegedResourceRetriever getPrivilegedResourceRetriever() {
        final String sslKey = RealmSettings.realmSslPrefix(realmConfig.identifier());
        final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration(sslKey);
        boolean isHostnameVerificationEnabled = sslConfiguration.verificationMode().isHostnameVerificationEnabled();
        final HostnameVerifier verifier = isHostnameVerificationEnabled ? new DefaultHostnameVerifier() : NoopHostnameVerifier.INSTANCE;
        return new PrivilegedResourceRetriever(sslService.sslContext(sslConfiguration), verifier);
    }

    private static final class PrivilegedResourceRetriever extends DefaultResourceRetriever {
        private SSLContext clientContext;
        private HostnameVerifier verifier;

        PrivilegedResourceRetriever(final SSLContext clientContext, final HostnameVerifier verifier) {
            super();
            this.clientContext = clientContext;
            this.verifier = verifier;
        }

        @Override
        public Resource retrieveResource(final URL url) throws IOException {
            SpecialPermission.check();
            try {
                return AccessController.doPrivileged(
                    (PrivilegedExceptionAction<Resource>) () -> {
                        final BasicHttpContext context = new BasicHttpContext();
                        try (CloseableHttpClient client = HttpClients.custom()
                            .setSSLContext(clientContext)
                            .setSSLHostnameVerifier(verifier)
                            .build()) {
                            HttpGet get = new HttpGet(url.toURI());
                            HttpResponse response = client.execute(get, context);
                            return new Resource(IOUtils.readInputStreamToString(response.getEntity().getContent(),
                                StandardCharsets.UTF_8), response.getEntity().getContentType().getValue());
                        }
                    });
            } catch (final PrivilegedActionException e) {
                throw (IOException) e.getCause();
            }

        }

    }
}
