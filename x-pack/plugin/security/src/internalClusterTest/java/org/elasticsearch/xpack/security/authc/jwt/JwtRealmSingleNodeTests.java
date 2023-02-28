/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.security.authc.Realms;

import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class JwtRealmSingleNodeTests extends SecuritySingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        final Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings())
            // 1st JWT realm
            .put("xpack.security.authc.realms.jwt.jwt0.order", 10)
            .put(
                randomBoolean()
                    ? Settings.builder().put("xpack.security.authc.realms.jwt.jwt0.token_type", "id_token").build()
                    : Settings.EMPTY
            )
            .put("xpack.security.authc.realms.jwt.jwt0.allowed_issuer", "my-issuer-01")
            .put("xpack.security.authc.realms.jwt.jwt0.allowed_audiences", "es-01")
            .put("xpack.security.authc.realms.jwt.jwt0.claims.principal", "sub")
            .put("xpack.security.authc.realms.jwt.jwt0.claims.groups", "groups")
            .put("xpack.security.authc.realms.jwt.jwt0.client_authentication.type", "shared_secret")
            .putList("xpack.security.authc.realms.jwt.jwt0.allowed_signature_algorithms", "HS256", "HS384")
            // 2nd JWT realm
            .put("xpack.security.authc.realms.jwt.jwt1.order", 20)
            .put("xpack.security.authc.realms.jwt.jwt1.token_type", "access_token")
            .put("xpack.security.authc.realms.jwt.jwt1.allowed_issuer", "my-issuer-02")
            .put("xpack.security.authc.realms.jwt.jwt1.allowed_subjects", "user-02")
            .put("xpack.security.authc.realms.jwt.jwt1.allowed_audiences", "es-02")
            .put("xpack.security.authc.realms.jwt.jwt1.fallback_claims.sub", "client_id")
            .put("xpack.security.authc.realms.jwt.jwt1.claims.principal", "appid")
            .put("xpack.security.authc.realms.jwt.jwt1.claims.groups", "groups")
            .put("xpack.security.authc.realms.jwt.jwt1.client_authentication.type", "shared_secret")
            .putList("xpack.security.authc.realms.jwt.jwt1.allowed_signature_algorithms", "HS256", "HS384")
            // 3rd JWT realm
            .put("xpack.security.authc.realms.jwt.jwt2.order", 30)
            .put("xpack.security.authc.realms.jwt.jwt2.token_type", "access_token")
            .put("xpack.security.authc.realms.jwt.jwt2.allowed_issuer", "my-issuer-03")
            .put("xpack.security.authc.realms.jwt.jwt2.allowed_subjects", "user-03")
            .put("xpack.security.authc.realms.jwt.jwt2.allowed_audiences", "es-03")
            .put("xpack.security.authc.realms.jwt.jwt2.fallback_claims.sub", "oid")
            .put("xpack.security.authc.realms.jwt.jwt2.claims.principal", "email")
            .put("xpack.security.authc.realms.jwt.jwt2.claims.groups", "groups")
            .put("xpack.security.authc.realms.jwt.jwt2.client_authentication.type", "shared_secret")
            .putList("xpack.security.authc.realms.jwt.jwt2.allowed_signature_algorithms", "HS256", "HS384");

        SecuritySettingsSource.addSecureSettings(builder, secureSettings -> {
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt0.hmac_key", "jwt0_hmac_key");
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt0.client_authentication.shared_secret", "jwt0_shared_secret");
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt1.hmac_key", "jwt1_hmac_key");
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt1.client_authentication.shared_secret", "jwt1_shared_secret");
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt2.hmac_key", "jwt2_hmac_key");
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt2.client_authentication.shared_secret", "jwt2_shared_secret");
        });

        return builder.build();
    }

    public void testAnyJwtRealmWillExtractTheToken() throws ParseException {
        final List<JwtRealm> jwtRealms = getJwtRealms();
        final JwtRealm jwtRealm = randomFrom(jwtRealms);

        final String sharedSecret = randomBoolean() ? randomAlphaOfLengthBetween(10, 20) : null;
        final String iss = randomAlphaOfLengthBetween(5, 18);
        final String aud = randomAlphaOfLengthBetween(5, 18);
        final String sub = randomAlphaOfLengthBetween(5, 18);

        // Realm 1 will extract the token because the JWT has all iss, sub, aud, principal claims.
        // Their values do not match what realm 1 expects but that does not matter when extracting the token
        final SignedJWT signedJWT1 = getSignedJWT(Map.of("iss", iss, "aud", aud, "sub", sub));
        final ThreadContext threadContext1 = prepareThreadContext(signedJWT1, sharedSecret);
        final var token1 = (JwtAuthenticationToken) jwtRealm.token(threadContext1);
        final String principal1 = Strings.format("%s/%s/%s/%s", iss, aud, sub, sub);
        assertJwtToken(token1, principal1, sharedSecret, signedJWT1);

        // Realm 2 for extracting the token from the following JWT
        // Because it does not have the sub claim but client_id, which is configured as fallback by realm 2
        final String appId = randomAlphaOfLengthBetween(5, 18);
        final SignedJWT signedJWT2 = getSignedJWT(Map.of("iss", iss, "aud", aud, "client_id", sub, "appid", appId));
        final ThreadContext threadContext2 = prepareThreadContext(signedJWT2, sharedSecret);
        final var token2 = (JwtAuthenticationToken) jwtRealm.token(threadContext2);
        final String principal2 = Strings.format("%s/%s/%s/%s", iss, aud, sub, appId);
        assertJwtToken(token2, principal2, sharedSecret, signedJWT2);

        // Realm 3 will extract the token from the following JWT
        // Because it has the oid claim which is configured as a fallback by realm 3
        final String email = randomAlphaOfLengthBetween(5, 18) + "@example.com";
        final SignedJWT signedJWT3 = getSignedJWT(Map.of("iss", iss, "aud", aud, "oid", sub, "email", email));
        final ThreadContext threadContext3 = prepareThreadContext(signedJWT3, sharedSecret);
        final var token3 = (JwtAuthenticationToken) jwtRealm.token(threadContext3);
        final String principal3 = Strings.format("%s/%s/%s/%s", iss, aud, sub, email);
        assertJwtToken(token3, principal3, sharedSecret, signedJWT3);

        // The JWT does not match any realm's configuration, a token with generic token principal will be extracted
        final SignedJWT signedJWT4 = getSignedJWT(Map.of("iss", iss, "aud", aud, "azp", sub, "email", email));
        final ThreadContext threadContext4 = prepareThreadContext(signedJWT4, sharedSecret);
        final var token4 = (JwtAuthenticationToken) jwtRealm.token(threadContext4);
        final String principal4 = Strings.format("<unrecognized-jwt> by %s", iss);
        assertJwtToken(token4, principal4, sharedSecret, signedJWT4);

        // The JWT does not have an issuer, a token with generic token principal will be extracted
        final SignedJWT signedJWT5 = getSignedJWT(Map.of("aud", aud, "sub", sub));
        final ThreadContext threadContext5 = prepareThreadContext(signedJWT5, sharedSecret);
        final var token5 = (JwtAuthenticationToken) jwtRealm.token(threadContext5);
        final String principal5 = "<unrecognized-jwt>";
        assertJwtToken(token5, principal5, sharedSecret, signedJWT5);
    }

    public void testJwtRealmReturnsNullTokenWhenJwtCredentialIsAbsent() {
        final List<JwtRealm> jwtRealms = getJwtRealms();
        final JwtRealm jwtRealm = randomFrom(jwtRealms);
        final String sharedSecret = randomBoolean() ? randomAlphaOfLengthBetween(10, 20) : null;

        // Authorization header is absent
        final ThreadContext threadContext1 = prepareThreadContext(null, sharedSecret);
        assertThat(jwtRealm.token(threadContext1), nullValue());

        // Scheme is not Bearer
        final ThreadContext threadContext2 = prepareThreadContext(null, sharedSecret);
        threadContext2.putHeader("Authorization", "Basic foobar");
        assertThat(jwtRealm.token(threadContext2), nullValue());
    }

    public void testJwtRealmThrowsErrorOnJwtParsingFailure() throws ParseException {
        final List<JwtRealm> jwtRealms = getJwtRealms();
        final JwtRealm jwtRealm = randomFrom(jwtRealms);
        final String sharedSecret = randomBoolean() ? randomAlphaOfLengthBetween(10, 20) : null;

        // Not a JWT
        final ThreadContext threadContext1 = prepareThreadContext(null, sharedSecret);
        threadContext1.putHeader("Authorization", "Bearer " + randomAlphaOfLengthBetween(40, 60));
        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> jwtRealm.token(threadContext1));
        assertThat(e1.getMessage(), containsString("Failed to parse JWT bearer token"));

        // Payload is not JSON
        final SignedJWT signedJWT2 = new SignedJWT(
            JWSHeader.parse(Map.of("alg", randomAlphaOfLengthBetween(5, 10))).toBase64URL(),
            Base64URL.encode("payload"),
            Base64URL.encode("signature")
        );
        final ThreadContext threadContext2 = prepareThreadContext(null, sharedSecret);
        threadContext2.putHeader("Authorization", "Bearer " + signedJWT2.serialize());
        final IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> jwtRealm.token(threadContext2));
        assertThat(e2.getMessage(), containsString("Failed to parse JWT claims set"));
    }

    private void assertJwtToken(JwtAuthenticationToken token, String tokenPrincipal, String sharedSecret, SignedJWT signedJWT)
        throws ParseException {
        assertThat(token.principal(), equalTo(tokenPrincipal));
        assertThat(token.getClientAuthenticationSharedSecret(), equalTo(sharedSecret));
        assertThat(token.getJWTClaimsSet(), equalTo(signedJWT.getJWTClaimsSet()));
        assertThat(token.getSignedJWT().getHeader().toJSONObject(), equalTo(signedJWT.getHeader().toJSONObject()));
        assertThat(token.getSignedJWT().getSignature(), equalTo(signedJWT.getSignature()));
        assertThat(token.getSignedJWT().getJWTClaimsSet(), equalTo(token.getJWTClaimsSet()));
    }

    private List<JwtRealm> getJwtRealms() {
        final Realms realms = getInstanceFromNode(Realms.class);
        final List<JwtRealm> jwtRealms = realms.getActiveRealms()
            .stream()
            .filter(realm -> realm instanceof JwtRealm)
            .map(JwtRealm.class::cast)
            .toList();
        return jwtRealms;
    }

    private SignedJWT getSignedJWT(Map<String, Object> m) throws ParseException {
        final HashMap<String, Object> claimsMap = new HashMap<>(m);
        final Instant now = Instant.now();
        // timestamp does not matter for tokenExtraction
        claimsMap.put("iat", now.minus(randomIntBetween(-1, 1), ChronoUnit.DAYS).getEpochSecond());
        claimsMap.put("exp", now.plus(randomIntBetween(-1, 1), ChronoUnit.DAYS).getEpochSecond());

        final JWTClaimsSet claimsSet = JWTClaimsSet.parse(claimsMap);
        final SignedJWT signedJWT = new SignedJWT(
            JWSHeader.parse(Map.of("alg", randomAlphaOfLengthBetween(5, 10))).toBase64URL(),
            claimsSet.toPayload().toBase64URL(),
            Base64URL.encode("signature")
        );
        return signedJWT;
    }

    private ThreadContext prepareThreadContext(SignedJWT signedJWT, String clientSecret) {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        if (signedJWT != null) {
            threadContext.putHeader("Authorization", "Bearer " + signedJWT.serialize());
        }
        if (clientSecret != null) {
            threadContext.putHeader(
                JwtRealm.HEADER_CLIENT_AUTHENTICATION,
                JwtRealm.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME + " " + clientSecret
            );
        }
        return threadContext;
    }
}
