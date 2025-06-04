/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import net.minidev.json.JSONArray;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSelector;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.IOUtils;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.ErrorObject;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.ClientSecretJWT;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.BearerTokenError;
import com.nimbusds.oauth2.sdk.util.JSONObjectUtils;
import com.nimbusds.openid.connect.sdk.AuthenticationErrorResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponseParser;
import com.nimbusds.openid.connect.sdk.AuthenticationSuccessResponse;
import com.nimbusds.openid.connect.sdk.Nonce;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse;
import com.nimbusds.openid.connect.sdk.claims.AccessTokenHash;
import com.nimbusds.openid.connect.sdk.token.OIDCTokens;
import com.nimbusds.openid.connect.sdk.validators.AccessTokenValidator;
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator;

import org.apache.commons.codec.Charsets;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.PrivilegedFileWatcher;
import org.elasticsearch.xpack.security.authc.jwt.JwtUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.ALLOWED_CLOCK_SKEW;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_CONNECTION_POOL_TTL;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_CONNECTION_READ_TIMEOUT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_CONNECT_TIMEOUT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_MAX_CONNECTIONS;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_PROXY_HOST;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_PROXY_PORT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_PROXY_SCHEME;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_SOCKET_TIMEOUT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_TCP_KEEP_ALIVE;

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
    private AtomicReference<IDTokenValidator> idTokenValidator = new AtomicReference<>();
    private final CloseableHttpAsyncClient httpClient;
    private final ResourceWatcherService watcherService;

    private static final Logger LOGGER = LogManager.getLogger(OpenIdConnectAuthenticator.class);

    public OpenIdConnectAuthenticator(
        RealmConfig realmConfig,
        OpenIdConnectProviderConfiguration opConfig,
        RelyingPartyConfiguration rpConfig,
        SSLService sslService,
        ResourceWatcherService watcherService
    ) {
        this.realmConfig = realmConfig;
        this.opConfig = opConfig;
        this.rpConfig = rpConfig;
        this.sslService = sslService;
        this.httpClient = createHttpClient();
        this.watcherService = watcherService;
        this.idTokenValidator.set(createIdTokenValidator(true));
    }

    // For testing
    OpenIdConnectAuthenticator(
        RealmConfig realmConfig,
        OpenIdConnectProviderConfiguration opConfig,
        RelyingPartyConfiguration rpConfig,
        SSLService sslService,
        IDTokenValidator idTokenValidator,
        ResourceWatcherService watcherService
    ) {
        this.realmConfig = realmConfig;
        this.opConfig = opConfig;
        this.rpConfig = rpConfig;
        this.sslService = sslService;
        this.httpClient = createHttpClient();
        this.idTokenValidator.set(idTokenValidator);
        this.watcherService = watcherService;
    }

    /**
     * Processes an OpenID Connect Response to an Authentication Request that comes in the form of a URL with the necessary parameters,
     * that is contained in the provided Token. If the response is valid, it calls the provided listener with a set of OpenID Connect
     * claims that identify the authenticated user. If the UserInfo endpoint is specified in the configuration, we attempt to make a
     * UserInfo request and add the returned claims to the Id Token claims.
     *
     * @param token    The OpenIdConnectToken to consume
     * @param listener The listener to notify with the resolved {@link JWTClaimsSet}
     */
    public void authenticate(OpenIdConnectToken token, final ActionListener<JWTClaimsSet> listener) {
        try {
            AuthenticationResponse authenticationResponse = AuthenticationResponseParser.parse(new URI(token.getRedirectUrl()));
            final Nonce expectedNonce = token.getNonce();
            State expectedState = token.getState();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "OpenID Connect Provider redirected user to [{}]. Expected Nonce is [{}] and expected State is [{}]",
                    token.getRedirectUrl(),
                    expectedNonce,
                    expectedState
                );
            }
            if (authenticationResponse instanceof AuthenticationErrorResponse) {
                ErrorObject error = ((AuthenticationErrorResponse) authenticationResponse).getErrorObject();
                listener.onFailure(
                    new ElasticsearchSecurityException(
                        "OpenID Connect Provider response indicates authentication failure" + "Code=[{}], Description=[{}]",
                        error.getCode(),
                        error.getDescription()
                    )
                );
                return;
            }
            final AuthenticationSuccessResponse response = authenticationResponse.toSuccessResponse();
            validateState(expectedState, response.getState());
            validateResponseType(response);
            if (rpConfig.getResponseType().impliesCodeFlow()) {
                final AuthorizationCode code = response.getAuthorizationCode();
                exchangeCodeForToken(code, ActionListener.wrap(tokens -> {
                    final AccessToken accessToken = tokens.v1();
                    final JWT idToken = tokens.v2();
                    validateAccessToken(accessToken, idToken);
                    getUserClaims(accessToken, idToken, expectedNonce, true, listener);
                }, listener::onFailure));
            } else {
                final JWT idToken = response.getIDToken();
                final AccessToken accessToken = response.getAccessToken();
                validateAccessToken(accessToken, idToken);
                getUserClaims(accessToken, idToken, expectedNonce, true, listener);
            }
        } catch (ElasticsearchSecurityException e) {
            // Don't wrap in a new ElasticsearchSecurityException
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(new ElasticsearchSecurityException("Failed to consume the OpenID connect response.", e));
        }
    }

    /**
     * Collects all the user claims we can get for the authenticated user. This happens in two steps:
     * <ul>
     * <li>First we attempt to validate the Id Token we have received and get any claims it contains</li>
     * <li>If we have received an Access Token and the UserInfo endpoint is configured, we also attempt to get the user info response
     * from there and parse the returned claims,
     * see {@link OpenIdConnectAuthenticator#getAndCombineUserInfoClaims(AccessToken, JWTClaimsSet, ActionListener)}</li>
     * </ul>
     *
     * @param accessToken    The {@link AccessToken} that the OP has issued for this user
     * @param idToken        The {@link JWT} Id Token that the OP has issued for this user
     * @param expectedNonce  The nonce value we sent in the authentication request and should be contained in the Id Token
     * @param claimsListener The listener to notify with the resolved {@link JWTClaimsSet}
     */
    // package private to testing
    @SuppressWarnings("unchecked")
    void getUserClaims(
        @Nullable AccessToken accessToken,
        JWT idToken,
        Nonce expectedNonce,
        boolean shouldRetry,
        ActionListener<JWTClaimsSet> claimsListener
    ) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("ID Token Header: {}", idToken.getHeader());
            }
            JWTClaimsSet verifiedIdTokenClaims = idTokenValidator.get().validate(idToken, expectedNonce).toJWTClaimsSet();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Received and validated the Id Token for the user: [{}]", verifiedIdTokenClaims);
            }
            // Add the Id Token string as a synthetic claim
            final Map<String, Object> verifiedIdTokenClaimsObject = verifiedIdTokenClaims.toJSONObject();
            final JWTClaimsSet idTokenClaim = new JWTClaimsSet.Builder().claim("id_token_hint", idToken.serialize()).build();
            mergeObjects(verifiedIdTokenClaimsObject, idTokenClaim.toJSONObject());
            final JWTClaimsSet enrichedVerifiedIdTokenClaims = JWTClaimsSet.parse(verifiedIdTokenClaimsObject);
            if (accessToken != null && opConfig.getUserinfoEndpoint() != null) {
                getAndCombineUserInfoClaims(accessToken, enrichedVerifiedIdTokenClaims, claimsListener);
            } else {
                if (accessToken == null && opConfig.getUserinfoEndpoint() != null) {
                    LOGGER.debug("UserInfo endpoint is configured but the OP didn't return an access token so we can't query it");
                } else if (accessToken != null && opConfig.getUserinfoEndpoint() == null) {
                    LOGGER.debug("OP returned an access token but the UserInfo endpoint is not configured.");
                }
                claimsListener.onResponse(enrichedVerifiedIdTokenClaims);
            }
        } catch (BadJOSEException e) {
            // We only try to update the cached JWK set once if a remote source is used and
            // RSA or ECDSA is used for signatures
            if (shouldRetry
                && JWSAlgorithm.Family.HMAC_SHA.contains(rpConfig.getSignatureAlgorithm()) == false
                && opConfig.getJwkSetPath().startsWith("https://")) {
                ((ReloadableJWKSource) ((JWSVerificationKeySelector) idTokenValidator.get().getJWSKeySelector()).getJWKSource())
                    .triggerReload(ActionListener.wrap(v -> {
                        getUserClaims(accessToken, idToken, expectedNonce, false, claimsListener);
                    }, ex -> {
                        LOGGER.debug("Attempted and failed to refresh JWK cache upon token validation failure", e);
                        claimsListener.onFailure(ex);
                    }));
            } else {
                LOGGER.debug("Failed to parse or validate the ID Token", e);
                claimsListener.onFailure(new ElasticsearchSecurityException("Failed to parse or validate the ID Token", e));
            }
        } catch (com.nimbusds.oauth2.sdk.ParseException | ParseException | JOSEException e) {
            LOGGER.debug(
                () -> format("ID Token: [%s], Nonce: [%s]", JwtUtil.toStringRedactSignature(idToken).get(), expectedNonce.toString()),
                e
            );
            claimsListener.onFailure(new ElasticsearchSecurityException("Failed to parse or validate the ID Token", e));
        }
    }

    /**
     * Validates an access token according to the
     * <a href="https://openid.net/specs/openid-connect-core-1_0.html#ImplicitTokenValidation">specification</a>.
     * <p>
     * When using the authorization code flow the OP might not provide the at_hash parameter in the
     * Id Token as allowed in the specification. In such a case we can't validate the access token
     * but this is considered safe as it was received in a back channel communication that was protected
     * by TLS. Also when using the implicit flow with the response type set to "id_token", no Access
     * Token will be returned from the OP
     *
     * @param accessToken The Access Token to validate. Can be null when the configured response type is "id_token"
     * @param idToken     The Id Token that was received in the same response
     */
    private void validateAccessToken(AccessToken accessToken, JWT idToken) {
        try {
            if (rpConfig.getResponseType().equals(ResponseType.parse("id_token token"))
                || rpConfig.getResponseType().equals(ResponseType.parse("code"))) {
                assert (accessToken != null) : "Access Token cannot be null for Response Type " + rpConfig.getResponseType().toString();
                final boolean isValidationOptional = rpConfig.getResponseType().equals(ResponseType.parse("code"));
                // only "Bearer" is defined in the specification but check just in case
                if (accessToken.getType().toString().equals("Bearer") == false) {
                    throw new ElasticsearchSecurityException(
                        "Invalid access token type [{}], while [Bearer] was expected",
                        accessToken.getType()
                    );
                }
                String atHashValue = idToken.getJWTClaimsSet().getStringClaim("at_hash");
                if (Strings.hasText(atHashValue) == false) {
                    if (isValidationOptional == false) {
                        throw new ElasticsearchSecurityException("Failed to verify access token. ID Token doesn't contain at_hash claim ");
                    }
                } else {
                    AccessTokenHash atHash = new AccessTokenHash(atHashValue);
                    JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(idToken.getHeader().getAlgorithm().getName());
                    AccessTokenValidator.validate(accessToken, jwsAlgorithm, atHash);
                }
            } else if (rpConfig.getResponseType().equals(ResponseType.parse("id_token")) && accessToken != null) {
                // This should NOT happen and indicates a misconfigured OP. Warn the user but do not fail
                LOGGER.warn("Access Token incorrectly returned from the OpenId Connect Provider while using \"id_token\" response type.");
            }
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Failed to verify access token.", e);
        }
    }

    /**
     * Reads and parses a JWKSet from a file
     *
     * @param jwkSetPath The path to the file that contains the JWKs as a string.
     * @return the parsed {@link JWKSet}
     * @throws ParseException if the file cannot be parsed
     * @throws IOException    if the file cannot be read
     */
    private JWKSet readJwkSetFromFile(String jwkSetPath) throws IOException, ParseException {
        final Path path = realmConfig.env().configDir().resolve(jwkSetPath);
        // avoid using JWKSet.loadFile() as it does not close FileInputStream internally
        try {
            String jwkSet = AccessController.doPrivileged(
                (PrivilegedExceptionAction<String>) () -> Files.readString(path, StandardCharsets.UTF_8)
            );
            return JWKSet.parse(jwkSet);
        } catch (PrivilegedActionException ex) {
            throw (IOException) ex.getException();
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
            throw new ElasticsearchSecurityException(
                "Unexpected response type [{}], while [{}] is configured",
                response.impliedResponseType(),
                rpConfig.getResponseType()
            );
        }
    }

    /**
     * Validate that the state parameter the response contained corresponds to the one that we generated in the
     * beginning of this authentication attempt and was stored with the user's session at the facilitator
     *
     * @param expectedState The state that was originally generated
     * @param state         The state that was contained in the response
     */
    private static void validateState(State expectedState, State state) {
        if (null == state) {
            throw new ElasticsearchSecurityException("Failed to validate the response, the response did not contain a state parameter");
        } else if (null == expectedState) {
            throw new ElasticsearchSecurityException(
                "Failed to validate the response, the user's session did not contain a state " + "parameter"
            );
        } else if (state.equals(expectedState) == false) {
            throw new ElasticsearchSecurityException("Invalid state parameter [{}], while [{}] was expected", state, expectedState);
        }
    }

    /**
     * Attempts to make a request to the UserInfo Endpoint of the OpenID Connect provider
     */
    private void getAndCombineUserInfoClaims(
        AccessToken accessToken,
        JWTClaimsSet verifiedIdTokenClaims,
        ActionListener<JWTClaimsSet> claimsListener
    ) {
        try {
            final HttpGet httpGet = new HttpGet(opConfig.getUserinfoEndpoint());
            httpGet.setHeader("Authorization", "Bearer " + accessToken.getValue());
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                httpClient.execute(httpGet, new FutureCallback<HttpResponse>() {
                    @Override
                    public void completed(HttpResponse result) {
                        handleUserinfoResponse(result, verifiedIdTokenClaims, claimsListener);
                    }

                    @Override
                    public void failed(Exception ex) {
                        claimsListener.onFailure(
                            new ElasticsearchSecurityException("Failed to get claims from the Userinfo Endpoint.", ex)
                        );
                    }

                    @Override
                    public void cancelled() {
                        claimsListener.onFailure(
                            new ElasticsearchSecurityException("Failed to get claims from the Userinfo Endpoint. Request was cancelled")
                        );
                    }
                });
                return null;
            });
        } catch (Exception e) {
            claimsListener.onFailure(new ElasticsearchSecurityException("Failed to get user information from the UserInfo endpoint.", e));
        }
    }

    /**
     * Handle the UserInfo Response from the OpenID Connect Provider. If successful, merge the returned claims with the claims
     * of the Id Token and call the provided listener.
     * (This method is package-protected for testing purposes)
     */
    static void handleUserinfoResponse(
        HttpResponse httpResponse,
        JWTClaimsSet verifiedIdTokenClaims,
        ActionListener<JWTClaimsSet> claimsListener
    ) {
        try {
            final HttpEntity entity = httpResponse.getEntity();
            final Header encodingHeader = entity.getContentEncoding();
            final Charset encoding = encodingHeader == null ? StandardCharsets.UTF_8 : Charsets.toCharset(encodingHeader.getValue());
            final Header contentHeader = entity.getContentType();
            final String contentAsString = EntityUtils.toString(entity, encoding);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "Received UserInfo Response from OP with status [{}] and content [{}] ",
                    httpResponse.getStatusLine().getStatusCode(),
                    contentAsString
                );
            }
            if (httpResponse.getStatusLine().getStatusCode() == 200) {
                if (ContentType.parse(contentHeader.getValue()).getMimeType().equals("application/json")) {
                    final JWTClaimsSet userInfoClaims = JWTClaimsSet.parse(contentAsString);
                    validateUserInfoResponse(userInfoClaims, verifiedIdTokenClaims.getSubject(), claimsListener);
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Successfully retrieved user information: [{}]", userInfoClaims);
                    }
                    final Map<String, Object> combinedClaims = verifiedIdTokenClaims.toJSONObject();
                    mergeObjects(combinedClaims, userInfoClaims.toJSONObject());
                    claimsListener.onResponse(JWTClaimsSet.parse(combinedClaims));
                } else if (ContentType.parse(contentHeader.getValue()).getMimeType().equals("application/jwt")) {
                    // TODO Handle validating possibly signed responses
                    claimsListener.onFailure(
                        new IllegalStateException(
                            "Unable to parse Userinfo Response. Signed/encrypted JWTs are" + "not currently supported"
                        )
                    );
                } else {
                    claimsListener.onFailure(
                        new IllegalStateException(
                            "Unable to parse Userinfo Response. Content type was expected to "
                                + "be [application/json] or [appliation/jwt] but was ["
                                + contentHeader.getValue()
                                + "]"
                        )
                    );
                }
            } else {
                final Header wwwAuthenticateHeader = httpResponse.getFirstHeader("WWW-Authenticate");
                if (wwwAuthenticateHeader != null && Strings.hasText(wwwAuthenticateHeader.getValue())) {
                    BearerTokenError error = BearerTokenError.parse(wwwAuthenticateHeader.getValue());
                    claimsListener.onFailure(
                        new ElasticsearchSecurityException(
                            "Failed to get user information from the UserInfo endpoint. Code=[{}], Description=[{}]",
                            error.getCode(),
                            error.getDescription()
                        )
                    );
                } else {
                    claimsListener.onFailure(
                        new ElasticsearchSecurityException(
                            "Failed to get user information from the UserInfo endpoint. Code=[{}], Description=[{}]",
                            httpResponse.getStatusLine().getStatusCode(),
                            httpResponse.getStatusLine().getReasonPhrase()
                        )
                    );
                }
            }
        } catch (Exception e) {
            claimsListener.onFailure(new ElasticsearchSecurityException("Failed to get user information from the UserInfo endpoint.", e));
        }
    }

    /**
     * Validates that the userinfo response contains a sub Claim and that this claim value is the same as the one returned in the ID Token
     */
    private static void validateUserInfoResponse(
        JWTClaimsSet userInfoClaims,
        String expectedSub,
        ActionListener<JWTClaimsSet> claimsListener
    ) {
        if (userInfoClaims.getSubject().isEmpty()) {
            claimsListener.onFailure(new ElasticsearchSecurityException("Userinfo Response did not contain a sub Claim"));
        } else if (userInfoClaims.getSubject().equals(expectedSub) == false) {
            claimsListener.onFailure(
                new ElasticsearchSecurityException(
                    "Userinfo Response is not valid as it is for " + "subject [{}] while the ID Token was for subject [{}]",
                    userInfoClaims.getSubject(),
                    expectedSub
                )
            );
        }
    }

    /**
     * Attempts to make a request to the Token Endpoint of the OpenID Connect provider in order to exchange an
     * authorization code for an Id Token (and potentially an Access Token)
     */
    private void exchangeCodeForToken(AuthorizationCode code, ActionListener<Tuple<AccessToken, JWT>> tokensListener) {
        try {
            final AuthorizationCodeGrant codeGrant = new AuthorizationCodeGrant(code, rpConfig.getRedirectUri());
            final HttpPost httpPost = new HttpPost(opConfig.getTokenEndpoint());
            httpPost.setHeader("Content-type", "application/x-www-form-urlencoded");
            final List<NameValuePair> params = new ArrayList<>();
            for (Map.Entry<String, List<String>> entry : codeGrant.toParameters().entrySet()) {
                // All parameters of AuthorizationCodeGrant are singleton lists
                params.add(new BasicNameValuePair(entry.getKey(), entry.getValue().get(0)));
            }
            if (rpConfig.getClientAuthenticationMethod().equals(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)) {
                UsernamePasswordCredentials creds = new UsernamePasswordCredentials(
                    URLEncoder.encode(rpConfig.getClientId().getValue(), StandardCharsets.UTF_8),
                    URLEncoder.encode(rpConfig.getClientSecret().toString(), StandardCharsets.UTF_8)
                );
                httpPost.addHeader(new BasicScheme().authenticate(creds, httpPost, null));
            } else if (rpConfig.getClientAuthenticationMethod().equals(ClientAuthenticationMethod.CLIENT_SECRET_POST)) {
                params.add(new BasicNameValuePair("client_id", rpConfig.getClientId().getValue()));
                params.add(new BasicNameValuePair("client_secret", rpConfig.getClientSecret().toString()));
            } else if (rpConfig.getClientAuthenticationMethod().equals(ClientAuthenticationMethod.CLIENT_SECRET_JWT)) {
                ClientSecretJWT clientSecretJWT = new ClientSecretJWT(
                    rpConfig.getClientId(),
                    opConfig.getTokenEndpoint(),
                    rpConfig.getClientAuthenticationJwtAlgorithm(),
                    new Secret(rpConfig.getClientSecret().toString())
                );
                for (Map.Entry<String, List<String>> entry : clientSecretJWT.toParameters().entrySet()) {
                    // Both client_assertion and client_assertion_type are singleton lists
                    params.add(new BasicNameValuePair(entry.getKey(), entry.getValue().get(0)));
                }
            } else {
                tokensListener.onFailure(
                    new ElasticsearchSecurityException(
                        "Failed to exchange code for Id Token using Token Endpoint."
                            + "Expected client authentication method to be one of "
                            + OpenIdConnectRealmSettings.CLIENT_AUTH_METHODS
                            + " but was ["
                            + rpConfig.getClientAuthenticationMethod()
                            + "]"
                    )
                );
            }
            httpPost.setEntity(new UrlEncodedFormEntity(params, (Charset) null));
            SpecialPermission.check();
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {

                httpClient.execute(httpPost, new FutureCallback<HttpResponse>() {
                    @Override
                    public void completed(HttpResponse result) {
                        handleTokenResponse(result, tokensListener);
                    }

                    @Override
                    public void failed(Exception ex) {
                        tokensListener.onFailure(
                            new ElasticsearchSecurityException("Failed to exchange code for Id Token using the Token Endpoint.", ex)
                        );
                    }

                    @Override
                    public void cancelled() {
                        final String message = "Failed to exchange code for Id Token using the Token Endpoint. Request was cancelled";
                        tokensListener.onFailure(new ElasticsearchSecurityException(message));
                    }
                });
                return null;
            });
        } catch (AuthenticationException | JOSEException e) {
            tokensListener.onFailure(
                new ElasticsearchSecurityException("Failed to exchange code for Id Token using the Token Endpoint.", e)
            );
        }
    }

    /**
     * Handle the Token Response from the OpenID Connect Provider. If successful, extract the (yet not validated) Id Token
     * and access token and call the provided listener.
     * (Package private for testing purposes)
     */
    static void handleTokenResponse(HttpResponse httpResponse, ActionListener<Tuple<AccessToken, JWT>> tokensListener) {
        try {
            final HttpEntity entity = httpResponse.getEntity();
            final Header encodingHeader = entity.getContentEncoding();
            final Header contentHeader = entity.getContentType();
            final String contentHeaderValue = contentHeader == null ? null : ContentType.parse(contentHeader.getValue()).getMimeType();
            if (contentHeaderValue == null || contentHeaderValue.equals("application/json") == false) {
                tokensListener.onFailure(
                    new IllegalStateException(
                        "Unable to parse Token Response. Content type was expected to be "
                            + "[application/json] but was ["
                            + contentHeaderValue
                            + "]"
                    )
                );
                return;
            }
            final Charset encoding = encodingHeader == null ? StandardCharsets.UTF_8 : Charsets.toCharset(encodingHeader.getValue());
            final RestStatus responseStatus = RestStatus.fromCode(httpResponse.getStatusLine().getStatusCode());
            if (RestStatus.OK != responseStatus) {
                final String json = EntityUtils.toString(entity, encoding);
                LOGGER.warn("Received Token Response from OP with status [{}] and content [{}]", responseStatus, json);
                if (RestStatus.BAD_REQUEST == responseStatus) {
                    final TokenErrorResponse tokenErrorResponse = TokenErrorResponse.parse(JSONObjectUtils.parse(json));
                    tokensListener.onFailure(
                        new ElasticsearchSecurityException(
                            "Failed to exchange code for Id Token. Code=[{}], Description=[{}]",
                            tokenErrorResponse.getErrorObject().getCode(),
                            tokenErrorResponse.getErrorObject().getDescription()
                        )
                    );
                } else {
                    tokensListener.onFailure(new ElasticsearchSecurityException("Failed to exchange code for Id Token"));
                }
            } else {
                final OIDCTokenResponse oidcTokenResponse = OIDCTokenResponse.parse(
                    JSONObjectUtils.parse(EntityUtils.toString(entity, encoding))
                );
                final OIDCTokens oidcTokens = oidcTokenResponse.getOIDCTokens();
                final AccessToken accessToken = oidcTokens.getAccessToken();
                final JWT idToken = oidcTokens.getIDToken();
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(
                        "Successfully exchanged code for ID Token [{}] and Access Token [{}]",
                        idToken,
                        truncateToken(accessToken.toString())
                    );
                }
                if (idToken == null) {
                    tokensListener.onFailure(
                        new ElasticsearchSecurityException("Token Response did not contain an ID Token or parsing of the JWT failed.")
                    );
                    return;
                }
                tokensListener.onResponse(new Tuple<>(accessToken, idToken));
            }
        } catch (Exception e) {
            tokensListener.onFailure(
                new ElasticsearchSecurityException(
                    "Failed to exchange code for Id Token using the Token Endpoint. Unable to parse Token Response",
                    e
                )
            );
        }
    }

    private static String truncateToken(String input) {
        if (Strings.hasText(input) == false || input.length() <= 4) {
            return input;
        }
        return input.substring(0, 2) + "***" + input.substring(input.length() - 2);
    }

    /**
     * Creates a {@link CloseableHttpAsyncClient} that uses a {@link PoolingNHttpClientConnectionManager}
     */
    private CloseableHttpAsyncClient createHttpClient() {
        try {
            SpecialPermission.check();
            return AccessController.doPrivileged((PrivilegedExceptionAction<CloseableHttpAsyncClient>) () -> {
                ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(
                    IOReactorConfig.custom().setSoKeepAlive(realmConfig.getSetting(HTTP_TCP_KEEP_ALIVE)).build()
                );
                final String sslKey = RealmSettings.realmSslPrefix(realmConfig.identifier());
                final SslConfiguration sslConfiguration = sslService.getSSLConfiguration(sslKey);
                final SSLContext clientContext = sslService.sslContext(sslConfiguration);
                final HostnameVerifier verifier = SSLService.getHostnameVerifier(sslConfiguration);
                Registry<SchemeIOSessionStrategy> registry = RegistryBuilder.<SchemeIOSessionStrategy>create()
                    .register("http", NoopIOSessionStrategy.INSTANCE)
                    .register("https", new SSLIOSessionStrategy(clientContext, verifier))
                    .build();
                PoolingNHttpClientConnectionManager connectionManager = new PoolingNHttpClientConnectionManager(ioReactor, registry);
                connectionManager.setDefaultMaxPerRoute(realmConfig.getSetting(HTTP_MAX_ENDPOINT_CONNECTIONS));
                connectionManager.setMaxTotal(realmConfig.getSetting(HTTP_MAX_CONNECTIONS));
                final RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(Math.toIntExact(realmConfig.getSetting(HTTP_CONNECT_TIMEOUT).getMillis()))
                    .setConnectionRequestTimeout(Math.toIntExact(realmConfig.getSetting(HTTP_CONNECTION_READ_TIMEOUT).getMillis()))
                    .setSocketTimeout(Math.toIntExact(realmConfig.getSetting(HTTP_SOCKET_TIMEOUT).getMillis()))
                    .build();

                HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClients.custom()
                    .setConnectionManager(connectionManager)
                    .setDefaultRequestConfig(requestConfig)
                    .setKeepAliveStrategy(getKeepAliveStrategy());
                if (realmConfig.hasSetting(HTTP_PROXY_HOST)) {
                    httpAsyncClientBuilder.setProxy(
                        new HttpHost(
                            realmConfig.getSetting(HTTP_PROXY_HOST),
                            realmConfig.getSetting(HTTP_PROXY_PORT),
                            realmConfig.getSetting(HTTP_PROXY_SCHEME)
                        )
                    );
                }
                CloseableHttpAsyncClient httpAsyncClient = httpAsyncClientBuilder.build();
                httpAsyncClient.start();
                return httpAsyncClient;
            });
        } catch (PrivilegedActionException e) {
            throw new IllegalStateException("Unable to create a HttpAsyncClient instance", e);
        }
    }

    // Package private for testing
    CloseableHttpAsyncClient getHttpClient() {
        return httpClient;
    }

    // Package private for testing
    ConnectionKeepAliveStrategy getKeepAliveStrategy() {
        final long userConfiguredKeepAlive = realmConfig.getSetting(HTTP_CONNECTION_POOL_TTL).millis();
        return (response, context) -> {
            var serverKeepAlive = DefaultConnectionKeepAliveStrategy.INSTANCE.getKeepAliveDuration(response, context);
            long actualKeepAlive;
            if (serverKeepAlive <= -1) {
                actualKeepAlive = userConfiguredKeepAlive;
            } else if (userConfiguredKeepAlive <= -1) {
                actualKeepAlive = serverKeepAlive;
            } else {
                actualKeepAlive = Math.min(serverKeepAlive, userConfiguredKeepAlive);
            }
            if (actualKeepAlive < -1) {
                actualKeepAlive = -1;
            }
            LOGGER.debug("effective HTTP connection keep-alive: [{}]ms", actualKeepAlive);
            return actualKeepAlive;
        };
    }

    /*
     * Creates an {@link IDTokenValidator} based on the current Relying Party configuration
     */
    IDTokenValidator createIdTokenValidator(boolean addFileWatcherIfRequired) {
        try {
            final JWSAlgorithm requestedAlgorithm = rpConfig.getSignatureAlgorithm();
            final int allowedClockSkew = Math.toIntExact(realmConfig.getSetting(ALLOWED_CLOCK_SKEW).getMillis());
            final IDTokenValidator idTokenValidator;
            if (JWSAlgorithm.Family.HMAC_SHA.contains(requestedAlgorithm)) {
                final Secret clientSecret = new Secret(rpConfig.getClientSecret().toString());
                idTokenValidator = new IDTokenValidator(opConfig.getIssuer(), rpConfig.getClientId(), requestedAlgorithm, clientSecret);
            } else {
                String jwkSetPath = opConfig.getJwkSetPath();
                if (jwkSetPath.startsWith("http://")) {
                    throw new IllegalArgumentException("The [http] protocol is not supported as it is insecure. Use [https] instead");
                } else if (jwkSetPath.startsWith("https://")) {
                    final JWSVerificationKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(
                        requestedAlgorithm,
                        new ReloadableJWKSource<>(new URL(jwkSetPath))
                    );
                    idTokenValidator = new IDTokenValidator(opConfig.getIssuer(), rpConfig.getClientId(), keySelector, null);
                } else {
                    if (addFileWatcherIfRequired) {
                        setMetadataFileWatcher(jwkSetPath);
                    }
                    final JWKSet jwkSet = readJwkSetFromFile(jwkSetPath);
                    idTokenValidator = new IDTokenValidator(opConfig.getIssuer(), rpConfig.getClientId(), requestedAlgorithm, jwkSet);
                }
            }
            idTokenValidator.setMaxClockSkew(allowedClockSkew);
            return idTokenValidator;
        } catch (IOException | ParseException e) {
            throw new IllegalStateException("Unable to create a IDTokenValidator instance", e);
        }
    }

    private void setMetadataFileWatcher(String jwkSetPath) throws IOException {
        final Path path = realmConfig.env().configDir().resolve(jwkSetPath);
        FileWatcher watcher = new PrivilegedFileWatcher(path);
        watcher.addListener(new FileListener(LOGGER, () -> this.idTokenValidator.set(createIdTokenValidator(false))));
        watcherService.add(watcher, ResourceWatcherService.Frequency.MEDIUM);
    }

    /**
     * Merges the Map with the claims of the ID Token with the Map with the claims of the UserInfo response.
     * The merging is performed based on the following rules:
     * <ul>
     * <li>If the values for a given claim are primitives (of the same type), the value from the ID Token is retained</li>
     * <li>If the values for a given claim are Objects, the values are merged</li>
     * <li>If the values for a given claim are Arrays, the values are merged without removing duplicates</li>
     * <li>If the values for a given claim are of different types, an exception is thrown</li>
     * </ul>
     *
     * @param userInfo The Map with the ID Token claims
     * @param idToken  The Map with the UserInfo Response claims
     * @return the merged Map
     */
    // pkg protected for testing
    @SuppressWarnings("unchecked")
    static Map<String, Object> mergeObjects(Map<String, Object> idToken, Map<String, Object> userInfo) {
        for (Map.Entry<String, Object> entry : idToken.entrySet()) {
            Object value1 = entry.getValue();
            Object value2 = userInfo.get(entry.getKey());
            if (value2 == null) {
                continue;
            }
            if (value1 instanceof JSONArray) {
                idToken.put(entry.getKey(), mergeArrays((JSONArray) value1, value2));
            } else if (value1 instanceof Map) {
                idToken.put(entry.getKey(), mergeObjects((Map<String, Object>) value1, value2));
            } else if (value1.getClass().equals(value2.getClass()) == false) {
                // A special handling for certain OPs that mix the usage of true and "true"
                if (value1 instanceof Boolean && value2 instanceof String && String.valueOf(value1).equals(value2)) {
                    idToken.put(entry.getKey(), value1);
                } else if (value2 instanceof Boolean && value1 instanceof String && String.valueOf(value2).equals(value1)) {
                    idToken.put(entry.getKey(), value2);
                } else {
                    throw new IllegalStateException(
                        "Error merging ID token and userinfo claim value for claim ["
                            + entry.getKey()
                            + "]. "
                            + "Cannot merge ["
                            + value1.getClass().getName()
                            + "] with ["
                            + value2.getClass().getName()
                            + "]"
                    );
                }
            }
        }
        for (Map.Entry<String, Object> entry : userInfo.entrySet()) {
            if (idToken.containsKey(entry.getKey()) == false) {
                idToken.put(entry.getKey(), entry.getValue());
            }
        }
        return idToken;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mergeObjects(Map<String, Object> jsonObject1, Object jsonObject2) {
        if (jsonObject2 == null) {
            return jsonObject1;
        }
        if (jsonObject2 instanceof Map) {
            return mergeObjects(jsonObject1, (Map<String, Object>) jsonObject2);
        }
        throw new IllegalStateException(
            "Error while merging ID token and userinfo claims. " + "Cannot merge a Map with a [" + jsonObject2.getClass().getName() + "]"
        );
    }

    private static JSONArray mergeArrays(JSONArray jsonArray1, Object jsonArray2) {
        if (jsonArray2 == null) {
            return jsonArray1;
        }
        if (jsonArray2 instanceof JSONArray) {
            return mergeArrays(jsonArray1, (JSONArray) jsonArray2);
        }
        if (jsonArray2 instanceof String) {
            jsonArray1.add(jsonArray2);
        }
        return jsonArray1;
    }

    private static JSONArray mergeArrays(JSONArray jsonArray1, JSONArray jsonArray2) {
        jsonArray1.addAll(jsonArray2);
        return jsonArray1;
    }

    protected void close() {
        try {
            this.httpClient.close();
        } catch (IOException e) {
            LOGGER.debug("Unable to close the HttpAsyncClient", e);
        }
    }

    private static class FileListener implements FileChangesListener {

        private final Logger logger;
        private final CheckedRunnable<Exception> onChange;

        private FileListener(Logger logger, CheckedRunnable<Exception> onChange) {
            this.logger = logger;
            this.onChange = onChange;
        }

        @Override
        public void onFileCreated(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileChanged(Path file) {
            try {
                onChange.run();
            } catch (Exception e) {
                logger.warn(() -> format("An error occurred while reloading file %s", file), e);
            }
        }
    }

    /**
     * Remote JSON Web Key source specified by a JWKSet URL. The retrieved JWK set is cached to
     * avoid unnecessary http requests. A single attempt to update the cached set is made
     * (with {@link ReloadableJWKSource#triggerReload}) when the {@link IDTokenValidator} fails
     * to validate an ID Token (because of an unknown key) as this might mean that the OpenID
     * Connect Provider has rotated the signing keys.
     */
    class ReloadableJWKSource<C extends SecurityContext> implements JWKSource<C> {

        private volatile JWKSet cachedJwkSet = new JWKSet();
        private final AtomicReference<ListenableFuture<Void>> reloadFutureRef = new AtomicReference<>();
        private final URL jwkSetPath;

        private ReloadableJWKSource(URL jwkSetPath) {
            this.jwkSetPath = jwkSetPath;
            triggerReload(
                ActionListener.wrap(
                    success -> LOGGER.trace("Successfully loaded and cached remote JWKSet on startup"),
                    failure -> LOGGER.trace("Failed to load and cache remote JWKSet on startup", failure)
                )
            );
        }

        @Override
        public List<JWK> get(JWKSelector jwkSelector, C context) {
            return jwkSelector.select(cachedJwkSet);
        }

        void triggerReload(ActionListener<Void> toNotify) {
            ListenableFuture<Void> future = reloadFutureRef.get();
            while (future == null) {
                future = new ListenableFuture<>();
                if (reloadFutureRef.compareAndSet(null, future)) {
                    reloadAsync(future);
                } else {
                    future = reloadFutureRef.get();
                }
            }
            future.addListener(toNotify);
        }

        void reloadAsync(final ListenableFuture<Void> future) {
            try {
                final HttpGet httpGet = new HttpGet(jwkSetPath.toURI());
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    httpClient.execute(httpGet, new FutureCallback<HttpResponse>() {
                        @Override
                        public void completed(HttpResponse result) {
                            try {
                                cachedJwkSet = JWKSet.parse(
                                    IOUtils.readInputStreamToString(result.getEntity().getContent(), StandardCharsets.UTF_8)
                                );
                                reloadFutureRef.set(null);
                                LOGGER.trace("Successfully refreshed and cached remote JWKSet");
                                future.onResponse(null);
                            } catch (Exception e) {
                                failed(e);
                            }
                        }

                        @Override
                        public void failed(Exception ex) {
                            future.onFailure(new ElasticsearchSecurityException("Failed to retrieve remote JWK set.", ex));
                            reloadFutureRef.set(null);
                        }

                        @Override
                        public void cancelled() {
                            future.onFailure(
                                new ElasticsearchSecurityException("Failed to retrieve remote JWK set. Request was cancelled.")
                            );
                            reloadFutureRef.set(null);
                        }
                    });
                    return null;
                });
            } catch (Exception e) {
                future.onFailure(e);
                reloadFutureRef.set(null);
            }
        }
    }
}
