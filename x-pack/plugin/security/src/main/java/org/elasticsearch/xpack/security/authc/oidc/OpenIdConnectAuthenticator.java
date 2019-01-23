/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jose.util.Resource;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ErrorObject;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.openid.connect.sdk.AuthenticationErrorResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponseParser;
import com.nimbusds.openid.connect.sdk.AuthenticationSuccessResponse;
import com.nimbusds.openid.connect.sdk.Nonce;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponseParser;
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import javax.net.ssl.HostnameVerifier;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
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
            if (rpConfig.getResponseType().impliesCodeFlow()) {
                final AuthorizationCode code = response.getAuthorizationCode();
                idToken = exchangeCodeForToken(code);
            } else {
                idToken = response.getIDToken();
            }

            return validateAndParseIdToken(idToken, expectedNonce);

        } catch (URISyntaxException | ParseException e) {
            logger.debug("Failed to parse the response that was sent to the redirect_uri", e);
            throw new ElasticsearchSecurityException("Failed to parse the response that was sent to the redirect_uri");
        }
    }

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
                    opConfig.getJwkSetUrl(), new PrivilegedResourceRetriever());
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

    private void validateResponseType(AuthenticationSuccessResponse response) {
        if (rpConfig.getResponseType().equals(response.impliedResponseType()) == false) {
            logger.debug("Unexpected response type [{}], while [{}] is configured", response.impliedResponseType(),
                rpConfig.getResponseType());
            throw new ElasticsearchSecurityException("Received a response with an unexpected response type");
        }
    }

    private void validateState(State expectedState, State state) {
        if (state.equals(expectedState) == false) {
            logger.debug("Invalid state parameter [{}], while [{}] was expected", state, expectedState);
            throw new ElasticsearchSecurityException("Received a response with an invalid state parameter");
        }
    }

    private JWT exchangeCodeForToken(AuthorizationCode code) {
        Secret clientSecret = null;
        try {
            clientSecret = new Secret(rpConfig.getClientSecret().toString());
            final ClientAuthentication clientAuth = new ClientSecretBasic(rpConfig.getClientId(), clientSecret);
            final AuthorizationGrant codeGrant = new AuthorizationCodeGrant(code, rpConfig.getRedirectUri());
            final TokenRequest request = new TokenRequest(opConfig.getTokenEndpoint(), clientAuth, codeGrant);

            final String sslKey = RealmSettings.realmSslPrefix(realmConfig.identifier());
            final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration(sslKey);
            boolean isHostnameVerificationEnabled = sslConfiguration.verificationMode().isHostnameVerificationEnabled();
            final HostnameVerifier verifier = isHostnameVerificationEnabled ? new DefaultHostnameVerifier() : NoopHostnameVerifier.INSTANCE;
            final HTTPRequest httpRequest = request.toHTTPRequest();
            httpRequest.setSSLSocketFactory(sslService.sslSocketFactory(sslConfiguration));
            httpRequest.setHostnameVerifier(verifier);

            SpecialPermission.check();
            final HTTPResponse httpResponse =
                AccessController.doPrivileged((PrivilegedExceptionAction<HTTPResponse>) httpRequest::send);

            final TokenResponse tokenResponse = OIDCTokenResponseParser.parse(httpResponse);
            if (tokenResponse.indicatesSuccess() == false) {
                TokenErrorResponse errorResponse = (TokenErrorResponse) tokenResponse;
                throw new ElasticsearchSecurityException("Failed to exchange code for Id Token. Code=[{}], Description=[{}]",
                    errorResponse.getErrorObject().getCode(), errorResponse.getErrorObject().getDescription());
            }
            OIDCTokenResponse successResponse = (OIDCTokenResponse) tokenResponse.toSuccessResponse();
            if (logger.isTraceEnabled()) {
                logger.trace("Successfully exchanged code for ID Token: [{}]", successResponse.toJSONObject().toJSONString());
            }

            return successResponse.getOIDCTokens().getIDToken();
        } catch (Exception e) {
            logger.debug("Failed to exchange code for Id Token using the Token Endpoint. ", e);
            throw new ElasticsearchSecurityException("Failed to exchange code for Id Token.");
        } finally {
            if (null != clientSecret) {
                clientSecret.erase();
            }
        }
    }

    private static final class PrivilegedResourceRetriever extends DefaultResourceRetriever {

        PrivilegedResourceRetriever() {
            super();
        }

        @Override
        public Resource retrieveResource(final URL url) throws IOException {
            try {
                return AccessController.doPrivileged(
                    (PrivilegedExceptionAction<Resource>) () -> PrivilegedResourceRetriever.super.retrieveResource(url));
            } catch (final PrivilegedActionException e) {
                throw (IOException) e.getCause();
            }

        }

    }
}
