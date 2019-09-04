/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.BadJWSException;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.BadJWTException;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.openid.connect.sdk.AuthenticationSuccessResponse;
import com.nimbusds.openid.connect.sdk.Nonce;
import com.nimbusds.openid.connect.sdk.claims.AccessTokenHash;
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator;
import com.nimbusds.openid.connect.sdk.validators.InvalidHashException;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;

import static java.time.Instant.now;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenIdConnectAuthenticatorTests extends OpenIdConnectTestCase {

    private OpenIdConnectAuthenticator authenticator;
    private Settings globalSettings;
    private Environment env;
    private ThreadContext threadContext;
    private int callsToReloadJwk;

    @Before
    public void setup() {
        globalSettings = Settings.builder().put("path.home", createTempDir())
            .put("xpack.security.authc.realms.oidc.oidc-realm.ssl.verification_mode", "certificate").build();
        env = TestEnvironment.newEnvironment(globalSettings);
        threadContext = new ThreadContext(globalSettings);
        callsToReloadJwk = 0;
    }

    @After
    public void cleanup() {
        if (authenticator != null) {
            authenticator.close();
        }
    }

    private OpenIdConnectAuthenticator buildAuthenticator() throws URISyntaxException {
        final RealmConfig config = buildConfig(getBasicRealmSettings().build(), threadContext);
        return new OpenIdConnectAuthenticator(config, getOpConfig(), getDefaultRpConfig(), new SSLService(globalSettings, env), null);
    }

    private OpenIdConnectAuthenticator buildAuthenticator(OpenIdConnectProviderConfiguration opConfig, RelyingPartyConfiguration rpConfig,
                                                          OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource) {
        final RealmConfig config = buildConfig(getBasicRealmSettings().build(), threadContext);
        final JWSVerificationKeySelector keySelector = new JWSVerificationKeySelector(rpConfig.getSignatureAlgorithm(), jwkSource);
        final IDTokenValidator validator = new IDTokenValidator(opConfig.getIssuer(), rpConfig.getClientId(), keySelector, null);
        return new OpenIdConnectAuthenticator(config, opConfig, rpConfig, new SSLService(globalSettings, env), validator,
            null);
    }

    private OpenIdConnectAuthenticator buildAuthenticator(OpenIdConnectProviderConfiguration opConfig,
                                                          RelyingPartyConfiguration rpConfig) {
        final RealmConfig config = buildConfig(getBasicRealmSettings().build(), threadContext);
        final IDTokenValidator validator = new IDTokenValidator(opConfig.getIssuer(), rpConfig.getClientId(),
            rpConfig.getSignatureAlgorithm(), new Secret(rpConfig.getClientSecret().toString()));
        return new OpenIdConnectAuthenticator(config, opConfig, rpConfig, new SSLService(globalSettings, env), validator,
            null);
    }

    public void testEmptyRedirectUrlIsRejected() throws Exception {
        authenticator = buildAuthenticator();
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        OpenIdConnectToken token = new OpenIdConnectToken(null, new State(), new Nonce(), authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to consume the OpenID connect response"));
    }

    public void testInvalidStateIsRejected() throws URISyntaxException {
        authenticator = buildAuthenticator();
        final String code = randomAlphaOfLengthBetween(8, 12);
        final String state = randomAlphaOfLengthBetween(8, 12);
        final String invalidState = state.concat(randomAlphaOfLength(2));
        final String redirectUrl = "https://rp.elastic.co/cb?code=" + code + "&state=" + state;
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        OpenIdConnectToken token = new OpenIdConnectToken(redirectUrl, new State(invalidState), new Nonce(),authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Invalid state parameter"));
    }

    public void testInvalidNonceIsRejected() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType(randomFrom("HS", "ES", "RS"));
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            authenticator = buildAuthenticator(opConfig, rpConfig);
        } else {
            OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
            authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);
        }

        final State state = new State();
        final Nonce nonce = new Nonce();
        final Nonce invalidNonce = new Nonce();
        final String subject = "janedoe";
        final String keyId = (jwk.getAlgorithm().getName().startsWith("HS")) ? null : jwk.getKeyID();
        final Tuple<AccessToken, JWT> tokens = buildTokens(invalidNonce, key, jwk.getAlgorithm().getName(), keyId, subject, true, false);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to parse or validate the ID Token"));
        assertThat(e.getCause(), instanceOf(BadJWTException.class));
        assertThat(e.getCause().getMessage(), containsString("Unexpected JWT nonce"));
    }

    public void testAuthenticateImplicitFlowWithRsa() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType("RS");
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
        authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);

        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final Tuple<AccessToken, JWT> tokens = buildTokens(nonce, key, jwk.getAlgorithm().getName(), jwk.getKeyID(), subject, true, false);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        JWTClaimsSet claimsSet = future.actionGet();
        assertThat(claimsSet.getSubject(), equalTo(subject));
    }

    public void testAuthenticateImplicitFlowWithEcdsa() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType("RS");
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
        authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);

        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final Tuple<AccessToken, JWT> tokens = buildTokens(nonce, key, jwk.getAlgorithm().getName(), jwk.getKeyID(), subject, true, false);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        JWTClaimsSet claimsSet = future.actionGet();
        assertThat(claimsSet.getSubject(), equalTo(subject));
    }

    public void testAuthenticateImplicitFlowWithHmac() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType("HS");
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();

        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        authenticator = buildAuthenticator(opConfig, rpConfig);

        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final Tuple<AccessToken, JWT> tokens = buildTokens(nonce, key, jwk.getAlgorithm().getName(), null, subject, true, false);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        JWTClaimsSet claimsSet = future.actionGet();
        assertThat(claimsSet.getSubject(), equalTo(subject));
    }

    public void testClockSkewIsHonored() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType(randomFrom("HS", "ES", "RS"));
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            authenticator = buildAuthenticator(opConfig, rpConfig);
        } else {
            OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
            authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);
        }
        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final String keyId = (jwk.getAlgorithm().getName().startsWith("HS")) ? null : jwk.getKeyID();
        JWTClaimsSet.Builder idTokenBuilder = new JWTClaimsSet.Builder()
            .jwtID(randomAlphaOfLength(8))
            .audience(rpConfig.getClientId().getValue())
            // Expired 55 seconds ago with an allowed clock skew of 60 seconds
            .expirationTime(Date.from(now().minusSeconds(55)))
            .issuer(opConfig.getIssuer().getValue())
            .issueTime(Date.from(now().minusSeconds(200)))
            .notBeforeTime(Date.from(now().minusSeconds(200)))
            .claim("nonce", nonce)
            .subject(subject);
        final Tuple<AccessToken, JWT> tokens = buildTokens(idTokenBuilder.build(), key, jwk.getAlgorithm().getName(), keyId, subject,
            true, false);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        JWTClaimsSet claimsSet = future.actionGet();
        assertThat(claimsSet.getSubject(), equalTo(subject));
        assertThat(callsToReloadJwk, equalTo(0));
    }

    public void testImplicitFlowFailsWithExpiredToken() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType(randomFrom("HS", "ES", "RS"));
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            authenticator = buildAuthenticator(opConfig, rpConfig);
        } else {
            OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
            authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);
        }
        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final String keyId = (jwk.getAlgorithm().getName().startsWith("HS")) ? null : jwk.getKeyID();
        JWTClaimsSet.Builder idTokenBuilder = new JWTClaimsSet.Builder()
            .jwtID(randomAlphaOfLength(8))
            .audience(rpConfig.getClientId().getValue())
            // Expired 65 seconds ago with an allowed clock skew of 60 seconds
            .expirationTime(Date.from(now().minusSeconds(65)))
            .issuer(opConfig.getIssuer().getValue())
            .issueTime(Date.from(now().minusSeconds(200)))
            .notBeforeTime(Date.from(now().minusSeconds(200)))
            .claim("nonce", nonce)
            .subject(subject);
        final Tuple<AccessToken, JWT> tokens = buildTokens(idTokenBuilder.build(), key, jwk.getAlgorithm().getName(), keyId,
            subject, true, false);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to parse or validate the ID Token"));
        assertThat(e.getCause(), instanceOf(BadJWTException.class));
        assertThat(e.getCause().getMessage(), containsString("Expired JWT"));
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            assertThat(callsToReloadJwk, equalTo(0));
        } else {
            assertThat(callsToReloadJwk, equalTo(1));
        }
    }

    public void testImplicitFlowFailsNotYetIssuedToken() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType(randomFrom("HS", "ES", "RS"));
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            authenticator = buildAuthenticator(opConfig, rpConfig);
        } else {
            OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
            authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);
        }
        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final String keyId = (jwk.getAlgorithm().getName().startsWith("HS")) ? null : jwk.getKeyID();
        JWTClaimsSet.Builder idTokenBuilder = new JWTClaimsSet.Builder()
            .jwtID(randomAlphaOfLength(8))
            .audience(rpConfig.getClientId().getValue())
            .expirationTime(Date.from(now().plusSeconds(3600)))
            .issuer(opConfig.getIssuer().getValue())
            // Issued 80 seconds in the future with max allowed clock skew of 60
            .issueTime(Date.from(now().plusSeconds(80)))
            .notBeforeTime(Date.from(now().minusSeconds(80)))
            .claim("nonce", nonce)
            .subject(subject);
        final Tuple<AccessToken, JWT> tokens = buildTokens(idTokenBuilder.build(), key, jwk.getAlgorithm().getName(), keyId,
            subject, true, false);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to parse or validate the ID Token"));
        assertThat(e.getCause(), instanceOf(BadJWTException.class));
        assertThat(e.getCause().getMessage(), containsString("JWT issue time ahead of current time"));
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            assertThat(callsToReloadJwk, equalTo(0));
        } else {
            assertThat(callsToReloadJwk, equalTo(1));
        }
    }

    public void testImplicitFlowFailsInvalidIssuer() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType(randomFrom("HS", "ES", "RS"));
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            authenticator = buildAuthenticator(opConfig, rpConfig);
        } else {
            OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
            authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);
        }
        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final String keyId = (jwk.getAlgorithm().getName().startsWith("HS")) ? null : jwk.getKeyID();
        JWTClaimsSet.Builder idTokenBuilder = new JWTClaimsSet.Builder()
            .jwtID(randomAlphaOfLength(8))
            .audience(rpConfig.getClientId().getValue())
            .expirationTime(Date.from(now().plusSeconds(3600)))
            .issuer("https://another.op.org")
            .issueTime(Date.from(now().minusSeconds(200)))
            .notBeforeTime(Date.from(now().minusSeconds(200)))
            .claim("nonce", nonce)
            .subject(subject);
        final Tuple<AccessToken, JWT> tokens = buildTokens(idTokenBuilder.build(), key, jwk.getAlgorithm().getName(), keyId,
            subject, true, false);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to parse or validate the ID Token"));
        assertThat(e.getCause(), instanceOf(BadJWTException.class));
        assertThat(e.getCause().getMessage(), containsString("Unexpected JWT issuer"));
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            assertThat(callsToReloadJwk, equalTo(0));
        } else {
            assertThat(callsToReloadJwk, equalTo(1));
        }
    }

    public void testImplicitFlowFailsInvalidAudience() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType(randomFrom("HS", "ES", "RS"));
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            authenticator = buildAuthenticator(opConfig, rpConfig);
        } else {
            OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
            authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);
        }
        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final String keyId = (jwk.getAlgorithm().getName().startsWith("HS")) ? null : jwk.getKeyID();
        JWTClaimsSet.Builder idTokenBuilder = new JWTClaimsSet.Builder()
            .jwtID(randomAlphaOfLength(8))
            .audience("some-other-RP")
            .expirationTime(Date.from(now().plusSeconds(3600)))
            .issuer(opConfig.getIssuer().getValue())
            .issueTime(Date.from(now().minusSeconds(200)))
            .notBeforeTime(Date.from(now().minusSeconds(80)))
            .claim("nonce", nonce)
            .subject(subject);
        final Tuple<AccessToken, JWT> tokens = buildTokens(idTokenBuilder.build(), key, jwk.getAlgorithm().getName(), keyId,
            subject, true, false);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to parse or validate the ID Token"));
        assertThat(e.getCause(), instanceOf(BadJWTException.class));
        assertThat(e.getCause().getMessage(), containsString("Unexpected JWT audience"));
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            assertThat(callsToReloadJwk, equalTo(0));
        } else {
            assertThat(callsToReloadJwk, equalTo(1));
        }
    }

    public void testAuthenticateImplicitFlowFailsWithForgedRsaIdToken() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType("RS");
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
        authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);

        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final Tuple<AccessToken, JWT> tokens = buildTokens(nonce, key, jwk.getAlgorithm().getName(), jwk.getKeyID(), subject, true, true);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to parse or validate the ID Token"));
        assertThat(e.getCause(), instanceOf(BadJWSException.class));
        assertThat(e.getCause().getMessage(), containsString("Signed JWT rejected: Invalid signature"));
        assertThat(callsToReloadJwk, equalTo(1));
    }

    public void testAuthenticateImplicitFlowFailsWithForgedEcsdsaIdToken() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType("ES");
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
        authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);

        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final Tuple<AccessToken, JWT> tokens = buildTokens(nonce, key, jwk.getAlgorithm().getName(), jwk.getKeyID(), subject, true, true);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to parse or validate the ID Token"));
        assertThat(e.getCause(), instanceOf(BadJWSException.class));
        assertThat(e.getCause().getMessage(), containsString("Signed JWT rejected: Invalid signature"));
        assertThat(callsToReloadJwk, equalTo(1));
    }

    public void testAuthenticateImplicitFlowFailsWithForgedHmacIdToken() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType("HS");
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        authenticator = buildAuthenticator(opConfig, rpConfig);

        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final Tuple<AccessToken, JWT> tokens = buildTokens(nonce, key, jwk.getAlgorithm().getName(), null, subject, true, true);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to parse or validate the ID Token"));
        assertThat(e.getCause(), instanceOf(BadJWSException.class));
        assertThat(e.getCause().getMessage(), containsString("Signed JWT rejected: Invalid signature"));
        assertThat(callsToReloadJwk, equalTo(0));
    }

    public void testAuthenticateImplicitFlowFailsWithForgedAccessToken() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType(randomFrom("HS", "ES", "RS"));
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            authenticator = buildAuthenticator(opConfig, rpConfig);
        } else {
            OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
            authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);
        }
        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final String keyId = (jwk.getAlgorithm().getName().startsWith("HS")) ? null : jwk.getKeyID();
        final Tuple<AccessToken, JWT> tokens = buildTokens(nonce, key, jwk.getAlgorithm().getName(), keyId, subject, true, false);
        final String responseUrl = buildAuthResponse(tokens.v2(), new BearerAccessToken("someforgedAccessToken"), state,
            rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to verify access token"));
        assertThat(e.getCause(), instanceOf(InvalidHashException.class));
        assertThat(e.getCause().getMessage(), containsString("Access token hash (at_hash) mismatch"));
        assertThat(callsToReloadJwk, equalTo(0));
    }

    public void testImplicitFlowFailsWithNoneAlgorithm() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType(randomFrom("HS"));
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        final Key key = keyMaterial.v1();
        RelyingPartyConfiguration rpConfig = getRpConfigNoAccessToken(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            authenticator = buildAuthenticator(opConfig, rpConfig);
        } else {
            OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
            authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);
        }
        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final String keyId = (jwk.getAlgorithm().getName().startsWith("HS")) ? null : jwk.getKeyID();
        final Tuple<AccessToken, JWT> tokens = buildTokens(nonce, key, jwk.getAlgorithm().getName(), keyId, subject, false, false);
        JWT idToken = tokens.v2();
        // Change the algorithm of the signed JWT to NONE
        String[] serializedParts = idToken.serialize().split("\\.");
        String legitimateHeader = new String(Base64.getUrlDecoder().decode(serializedParts[0]), StandardCharsets.UTF_8);
        String forgedHeader = legitimateHeader.replace(jwk.getAlgorithm().getName(), "NONE");
        String encodedForgedHeader =
            Base64.getUrlEncoder().withoutPadding().encodeToString(forgedHeader.getBytes(StandardCharsets.UTF_8));
        String fordedTokenString = encodedForgedHeader + "." + serializedParts[1] + "." + serializedParts[2];
        idToken = SignedJWT.parse(fordedTokenString);
        final String responseUrl = buildAuthResponse(idToken, tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to parse or validate the ID Token"));
        assertThat(e.getCause(), instanceOf(BadJOSEException.class));
        assertThat(e.getCause().getMessage(), containsString("Another algorithm expected, or no matching key(s) found"));
        assertThat(callsToReloadJwk, equalTo(0));
    }

    /**
     * The premise of this attack is that an RP that expects a JWT signed with an asymmetric algorithm (RSA, ECDSA)
     * receives a JWT signed with an HMAC. Trusting the received JWT's alg claim more than it's own configuration,
     * it attempts to validate the HMAC with the provider's {RSA,EC} public key as a secret key
     */
    public void testImplicitFlowFailsWithAlgorithmMixupAttack() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType(randomFrom("ES", "RS"));
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
        authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);
        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        SecretKeySpec hmacKey = new SecretKeySpec("thisismysupersupersupersupersupersuperlongsecret".getBytes(StandardCharsets.UTF_8),
            "HmacSha384");
        final Tuple<AccessToken, JWT> tokens = buildTokens(nonce, hmacKey, "HS384", null, subject,
            true, false);
        final String responseUrl = buildAuthResponse(tokens.v2(), tokens.v1(), state, rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to parse or validate the ID Token"));
        assertThat(e.getCause(), instanceOf(BadJOSEException.class));
        assertThat(e.getCause().getMessage(), containsString("Another algorithm expected, or no matching key(s) found"));
        assertThat(callsToReloadJwk, equalTo(1));
    }

    public void testImplicitFlowFailsWithUnsignedJwt() throws Exception {
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType(randomFrom("HS", "ES", "RS"));
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        RelyingPartyConfiguration rpConfig = getRpConfigNoAccessToken(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            authenticator = buildAuthenticator(opConfig, rpConfig);
        } else {
            OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource = mockSource(jwk);
            authenticator = buildAuthenticator(opConfig, rpConfig, jwkSource);
        }
        final State state = new State();
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        JWTClaimsSet.Builder idTokenBuilder = new JWTClaimsSet.Builder()
            .jwtID(randomAlphaOfLength(8))
            .audience(rpConfig.getClientId().getValue())
            .expirationTime(Date.from(now().plusSeconds(3600)))
            .issuer(opConfig.getIssuer().getValue())
            .issueTime(Date.from(now().minusSeconds(200)))
            .notBeforeTime(Date.from(now().minusSeconds(200)))
            .claim("nonce", nonce)
            .subject(subject);

        final String responseUrl = buildAuthResponse(new PlainJWT(idTokenBuilder.build()), null, state,
            rpConfig.getRedirectUri());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken(responseUrl, state, nonce, authenticatingRealm);
        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        authenticator.authenticate(token, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            future::actionGet);
        assertThat(e.getMessage(), containsString("Failed to parse or validate the ID Token"));
        assertThat(e.getCause(), instanceOf(BadJWTException.class));
        assertThat(e.getCause().getMessage(), containsString("Signed ID token expected"));
        if (jwk.getAlgorithm().getName().startsWith("HS")) {
            assertThat(callsToReloadJwk, equalTo(0));
        } else {
            assertThat(callsToReloadJwk, equalTo(1));
        }
    }

    public void testJsonObjectMerging() throws Exception {
        final Nonce nonce = new Nonce();
        final String subject = "janedoe";
        final Tuple<Key, JWKSet> keyMaterial = getRandomJwkForType(randomFrom("ES", "RS"));
        final JWK jwk = keyMaterial.v2().getKeys().get(0);
        RelyingPartyConfiguration rpConfig = getRpConfig(jwk.getAlgorithm().getName());
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        JSONObject address = new JWTClaimsSet.Builder()
            .claim("street_name", "12, Test St.")
            .claim("locality", "New York")
            .claim("region", "NY")
            .claim("country", "USA")
            .build()
            .toJSONObject();
        JSONObject idTokenObject = new JWTClaimsSet.Builder()
            .jwtID(randomAlphaOfLength(8))
            .audience(rpConfig.getClientId().getValue())
            .expirationTime(Date.from(now().plusSeconds(3600)))
            .issuer(opConfig.getIssuer().getValue())
            .issueTime(Date.from(now().minusSeconds(200)))
            .notBeforeTime(Date.from(now().minusSeconds(200)))
            .claim("nonce", nonce)
            .claim("given_name", "Jane Doe")
            .claim("family_name", "Doe")
            .claim("profile", "https://test-profiles.com/jane.doe")
            .claim("name", "Jane")
            .claim("email", "jane.doe@example.com")
            .claim("roles", new JSONArray().appendElement("role1").appendElement("role2").appendElement("role3"))
            .claim("address", address)
            .subject(subject)
            .build()
            .toJSONObject();

        JSONObject userinfoObject = new JWTClaimsSet.Builder()
            .claim("given_name", "Jane Doe")
            .claim("family_name", "Doe")
            .claim("profile", "https://test-profiles.com/jane.doe")
            .claim("name", "Jane")
            .claim("email", "jane.doe@example.com")
            .subject(subject)
            .build()
            .toJSONObject();

        OpenIdConnectAuthenticator.mergeObjects(idTokenObject, userinfoObject);
        assertTrue(idTokenObject.containsKey("given_name"));
        assertTrue(idTokenObject.containsKey("family_name"));
        assertTrue(idTokenObject.containsKey("profile"));
        assertTrue(idTokenObject.containsKey("name"));
        assertTrue(idTokenObject.containsKey("email"));
        assertTrue(idTokenObject.containsKey("address"));
        assertTrue(idTokenObject.containsKey("roles"));
        assertTrue(idTokenObject.containsKey("nonce"));
        assertTrue(idTokenObject.containsKey("sub"));
        assertTrue(idTokenObject.containsKey("jti"));
        assertTrue(idTokenObject.containsKey("aud"));
        assertTrue(idTokenObject.containsKey("exp"));
        assertTrue(idTokenObject.containsKey("iss"));
        assertTrue(idTokenObject.containsKey("iat"));
        assertTrue(idTokenObject.containsKey("email"));

        // Claims with different types throw an error
        JSONObject wrongTypeInfo = new JWTClaimsSet.Builder()
            .claim("given_name", "Jane Doe")
            .claim("family_name", 123334434)
            .claim("profile", "https://test-profiles.com/jane.doe")
            .claim("name", "Jane")
            .claim("email", "jane.doe@example.com")
            .subject(subject)
            .build()
            .toJSONObject();

        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            OpenIdConnectAuthenticator.mergeObjects(idTokenObject, wrongTypeInfo);
        });

        // Userinfo Claims overwrite ID Token claims
        JSONObject overwriteUserInfo = new JWTClaimsSet.Builder()
            .claim("given_name", "Jane Doe")
            .claim("family_name", "Doe")
            .claim("profile", "https://test-profiles.com/jane.doe2")
            .claim("name", "Jane")
            .claim("email", "jane.doe@mail.com")
            .subject(subject)
            .build()
            .toJSONObject();

        OpenIdConnectAuthenticator.mergeObjects(idTokenObject, overwriteUserInfo);
        assertThat(idTokenObject.getAsString("email"), equalTo("jane.doe@example.com"));
        assertThat(idTokenObject.getAsString("profile"), equalTo("https://test-profiles.com/jane.doe"));

        // Merging Arrays
        JSONObject userInfoWithRoles = new JWTClaimsSet.Builder()
            .claim("given_name", "Jane Doe")
            .claim("family_name", "Doe")
            .claim("profile", "https://test-profiles.com/jane.doe")
            .claim("name", "Jane")
            .claim("email", "jane.doe@example.com")
            .claim("roles", new JSONArray().appendElement("role4").appendElement("role5"))
            .subject(subject)
            .build()
            .toJSONObject();

        OpenIdConnectAuthenticator.mergeObjects(idTokenObject, userInfoWithRoles);
        assertThat((JSONArray) idTokenObject.get("roles"), containsInAnyOrder("role1", "role2", "role3", "role4", "role5"));

        // Merging nested objects
        JSONObject addressUserInfo = new JWTClaimsSet.Builder()
            .claim("street_name", "12, Test St.")
            .claim("locality", "New York")
            .claim("postal_code", "10024")
            .build()
            .toJSONObject();
        JSONObject userInfoWithAddress = new JWTClaimsSet.Builder()
            .claim("given_name", "Jane Doe")
            .claim("family_name", "Doe")
            .claim("profile", "https://test-profiles.com/jane.doe")
            .claim("name", "Jane")
            .claim("email", "jane.doe@example.com")
            .claim("roles", new JSONArray().appendElement("role4").appendElement("role5"))
            .claim("address", addressUserInfo)
            .subject(subject)
            .build()
            .toJSONObject();
        OpenIdConnectAuthenticator.mergeObjects(idTokenObject, userInfoWithAddress);
        assertTrue(idTokenObject.containsKey("address"));
        JSONObject combinedAddress = (JSONObject) idTokenObject.get("address");
        assertTrue(combinedAddress.containsKey("street_name"));
        assertTrue(combinedAddress.containsKey("locality"));
        assertTrue(combinedAddress.containsKey("street_name"));
        assertTrue(combinedAddress.containsKey("postal_code"));
        assertTrue(combinedAddress.containsKey("region"));
        assertTrue(combinedAddress.containsKey("country"));
    }

    private OpenIdConnectProviderConfiguration getOpConfig() throws URISyntaxException {
        return new OpenIdConnectProviderConfiguration(
            new Issuer("https://op.example.com"),
            "https://op.example.org/jwks.json",
            new URI("https://op.example.org/login"),
            new URI("https://op.example.org/token"),
            null,
            new URI("https://op.example.org/logout"));
    }

    private RelyingPartyConfiguration getDefaultRpConfig() throws URISyntaxException {
        return new RelyingPartyConfiguration(
            new ClientID("rp-my"),
            new SecureString("thisismysupersupersupersupersupersuperlongsecret".toCharArray()),
            new URI("https://rp.elastic.co/cb"),
            new ResponseType("id_token", "token"),
            new Scope("openid"),
            JWSAlgorithm.RS384,
            new URI("https://rp.elastic.co/successfull_logout"));
    }
    private RelyingPartyConfiguration getRpConfig(String alg) throws URISyntaxException {
        return new RelyingPartyConfiguration(
            new ClientID("rp-my"),
            new SecureString("thisismysupersupersupersupersupersuperlongsecret".toCharArray()),
            new URI("https://rp.elastic.co/cb"),
            new ResponseType("id_token", "token"),
            new Scope("openid"),
            JWSAlgorithm.parse(alg),
            new URI("https://rp.elastic.co/successfull_logout"));
    }

    private RelyingPartyConfiguration getRpConfigNoAccessToken(String alg) throws URISyntaxException {
        return new RelyingPartyConfiguration(
            new ClientID("rp-my"),
            new SecureString("thisismysupersupersupersupersupersuperlongsecret".toCharArray()),
            new URI("https://rp.elastic.co/cb"),
            new ResponseType("id_token"),
            new Scope("openid"),
            JWSAlgorithm.parse(alg),
            new URI("https://rp.elastic.co/successfull_logout"));
    }

    private String buildAuthResponse(JWT idToken, @Nullable AccessToken accessToken, State state, URI redirectUri) {
        AuthenticationSuccessResponse response = new AuthenticationSuccessResponse(
            redirectUri,
            null,
            idToken,
            accessToken,
            state,
            null,
            null);
        return response.toURI().toString();
    }

    private OpenIdConnectAuthenticator.ReloadableJWKSource mockSource(JWK jwk) {
        OpenIdConnectAuthenticator.ReloadableJWKSource jwkSource =
            mock(OpenIdConnectAuthenticator.ReloadableJWKSource.class);
        when(jwkSource.get(any(), any())).thenReturn(Collections.singletonList(jwk));
        Mockito.doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Void> listener = (ActionListener<Void>) invocation.getArguments()[0];
            callsToReloadJwk += 1;
            listener.onResponse(null);
            return null;
        }).when(jwkSource).triggerReload(any(ActionListener.class));
        return jwkSource;
    }

    private Tuple<AccessToken, JWT> buildTokens(JWTClaimsSet idToken, Key key, String alg, String keyId,
                                                String subject, boolean withAccessToken, boolean forged) throws Exception {
        AccessToken accessToken = null;
        if (withAccessToken) {
            accessToken = new BearerAccessToken(Base64.getUrlEncoder().encodeToString(randomByteArrayOfLength(32)));
            AccessTokenHash expectedHash = AccessTokenHash.compute(accessToken, JWSAlgorithm.parse(alg));
            idToken = JWTClaimsSet.parse(idToken.toJSONObject().appendField("at_hash", expectedHash.getValue()));
        }
        SignedJWT jwt = new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.parse(alg)).keyID(keyId).build(),
            idToken);

        if (key instanceof RSAPrivateKey) {
            jwt.sign(new RSASSASigner((PrivateKey) key));
        } else if (key instanceof ECPrivateKey) {
            jwt.sign(new ECDSASigner((ECPrivateKey) key));
        } else if (key instanceof SecretKey) {
            jwt.sign(new MACSigner((SecretKey) key));
        }
        if (forged) {
            // Change the sub claim to "attacker"
            String[] serializedParts = jwt.serialize().split("\\.");
            String legitimatePayload = new String(Base64.getUrlDecoder().decode(serializedParts[1]), StandardCharsets.UTF_8);
            String forgedPayload = legitimatePayload.replace(subject, "attacker");
            String encodedForgedPayload =
                Base64.getUrlEncoder().withoutPadding().encodeToString(forgedPayload.getBytes(StandardCharsets.UTF_8));
            String fordedTokenString = serializedParts[0] + "." + encodedForgedPayload + "." + serializedParts[2];
            jwt = SignedJWT.parse(fordedTokenString);
        }
        return new Tuple<>(accessToken, jwt);
    }

    private Tuple<AccessToken, JWT> buildTokens(Nonce nonce, Key key, String alg, String keyId, String subject, boolean withAccessToken,
                                                boolean forged) throws Exception {
        RelyingPartyConfiguration rpConfig = getRpConfig(alg);
        OpenIdConnectProviderConfiguration opConfig = getOpConfig();
        JWTClaimsSet.Builder idTokenBuilder = new JWTClaimsSet.Builder()
            .jwtID(randomAlphaOfLength(8))
            .audience(rpConfig.getClientId().getValue())
            .expirationTime(Date.from(now().plusSeconds(3600)))
            .issuer(opConfig.getIssuer().getValue())
            .issueTime(Date.from(now().minusSeconds(4)))
            .notBeforeTime(Date.from(now().minusSeconds(4)))
            .claim("nonce", nonce)
            .subject(subject);

        return buildTokens(idTokenBuilder.build(), key, alg, keyId, subject, withAccessToken, forged);
    }

    private Tuple<Key, JWKSet> getRandomJwkForType(String type) throws Exception {
        JWK jwk;
        Key key;
        int hashSize;
        if (type.equals("RS")) {
            hashSize = randomFrom(256, 384, 512);
            int keySize = randomFrom(2048, 4096);
            KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
            gen.initialize(keySize);
            KeyPair keyPair = gen.generateKeyPair();
            key = keyPair.getPrivate();
            jwk = new RSAKey.Builder((RSAPublicKey) keyPair.getPublic())
                .privateKey((RSAPrivateKey) keyPair.getPrivate())
                .keyUse(KeyUse.SIGNATURE)
                .keyID(UUID.randomUUID().toString())
                .algorithm(JWSAlgorithm.parse(type + hashSize))
                .build();

        } else if (type.equals("HS")) {
            hashSize = randomFrom(256, 384);
            SecretKeySpec hmacKey = new SecretKeySpec("thisismysupersupersupersupersupersuperlongsecret".getBytes(StandardCharsets.UTF_8),
                "HmacSha" + hashSize);
            //SecretKey hmacKey = KeyGenerator.getInstance("HmacSha" + hashSize).generateKey();
            key = hmacKey;
            jwk = new OctetSequenceKey.Builder(hmacKey)
                .keyID(UUID.randomUUID().toString())
                .algorithm(JWSAlgorithm.parse(type + hashSize))
                .build();

        } else if (type.equals("ES")) {
            hashSize = randomFrom(256, 384, 512);
            ECKey.Curve curve = curveFromHashSize(hashSize);
            KeyPairGenerator gen = KeyPairGenerator.getInstance("EC");
            gen.initialize(curve.toECParameterSpec());
            KeyPair keyPair = gen.generateKeyPair();
            key = keyPair.getPrivate();
            jwk = new ECKey.Builder(curve, (ECPublicKey) keyPair.getPublic())
                .privateKey((ECPrivateKey) keyPair.getPrivate())
                .algorithm(JWSAlgorithm.parse(type + hashSize))
                .build();
        } else {
            throw new IllegalArgumentException("Invalid key type :" + type);
        }
        return new Tuple(key, new JWKSet(jwk));
    }

    private ECKey.Curve curveFromHashSize(int size) {
        if (size == 256) {
            return ECKey.Curve.P_256;
        } else if (size == 384) {
            return ECKey.Curve.P_384;
        } else if (size == 512) {
            return ECKey.Curve.P_521;
        } else {
            throw new IllegalArgumentException("Invalid hash size:" + size);
        }
    }
}
