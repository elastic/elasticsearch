/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.openid.connect.sdk.Nonce;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JwtRealmAuthenticateAccessTokenTypeTests extends JwtRealmTestCase {

    private String fallbackSub;
    private String fallbackAud;
    private SignedJWT unsignedJwt;

    public void testAccessTokenTypeWorksWithNoFallback() throws Exception {
        noFallback();

        jwtIssuerAndRealms = generateJwtIssuerRealmPairs(
            randomIntBetween(1, 1), // realms
            randomIntBetween(0, 1), // authz
            randomIntBetween(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.size()), // algorithms
            randomIntBetween(1, 3), // audiences
            randomIntBetween(1, 3), // users
            randomIntBetween(0, 3), // roles
            randomIntBetween(0, 1), // jwtCacheSize
            randomBoolean() // createHttpsServer
        );
        final JwtIssuerAndRealm jwtIssuerAndRealm = randomJwtIssuerRealmPair();
        final User user = randomUser(jwtIssuerAndRealm.issuer());

        final SecureString jwt = randomJwt(jwtIssuerAndRealm, user);
        final SecureString clientSecret = JwtRealmInspector.getClientAuthenticationSharedSecret(jwtIssuerAndRealm.realm());
        doMultipleAuthcAuthzAndVerifySuccess(jwtIssuerAndRealm.realm(), user, jwt, clientSecret, randomIntBetween(1, 3));
    }

    public void testAccessTokenTypeWorksWithFallbacks() throws Exception {
        randomFallbacks();

        jwtIssuerAndRealms = generateJwtIssuerRealmPairs(
            randomIntBetween(1, 1), // realms
            randomIntBetween(0, 1), // authz
            randomIntBetween(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.size()), // algorithms
            randomIntBetween(1, 3), // audiences
            randomIntBetween(1, 3), // users
            randomIntBetween(0, 3), // roles
            randomIntBetween(0, 1), // jwtCacheSize
            randomBoolean() // createHttpsServer
        );
        final JwtIssuerAndRealm jwtIssuerAndRealm = randomJwtIssuerRealmPair();
        final User user = randomUser(jwtIssuerAndRealm.issuer());

        final SecureString jwt2 = randomJwt(jwtIssuerAndRealm, user);
        final SecureString clientSecret = JwtRealmInspector.getClientAuthenticationSharedSecret(jwtIssuerAndRealm.realm());
        doMultipleAuthcAuthzAndVerifySuccess(jwtIssuerAndRealm.realm(), user, jwt2, clientSecret, randomIntBetween(1, 3));
    }

    @Override
    protected JwtRealmSettingsBuilder createJwtRealmSettingsBuilder(JwtIssuer jwtIssuer, int authzCount, int jwtCacheSize)
        throws Exception {
        final JwtRealmSettingsBuilder jwtRealmSettingsBuilder = super.createJwtRealmSettingsBuilder(jwtIssuer, authzCount, jwtCacheSize);
        final String realmName = jwtRealmSettingsBuilder.name();
        final Settings.Builder settingsBuilder = jwtRealmSettingsBuilder.settingsBuilder();
        settingsBuilder.put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.TOKEN_TYPE), "access_token")
            .putList(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS),
                jwtIssuer.principals.keySet().stream().toList()
            );

        if (fallbackSub != null) {
            settingsBuilder.put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_SUB_CLAIM), fallbackSub);
        }
        if (fallbackAud != null) {
            settingsBuilder.put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_AUD_CLAIM), fallbackAud);
        }

        return jwtRealmSettingsBuilder;
    }

    @Override
    protected SecureString randomJwt(JwtIssuerAndRealm jwtIssuerAndRealm, User user) throws Exception {
        final JwtIssuer.AlgJwkPair algJwkPair = randomFrom(jwtIssuerAndRealm.issuer().algAndJwksAll);
        final JWK jwk = algJwkPair.jwk();

        final HashMap<String, Object> otherClaims = new HashMap<>();
        if (randomBoolean()) {
            otherClaims.putAll(Map.of("other1", randomAlphaOfLength(10), "other2", randomAlphaOfLength(10)));
        }

        // Randomly set the fallback claims, it can co-exist with the original one in which case it is ignored
        String subClaimValue = user.principal();
        if (fallbackSub != null) {
            if (randomBoolean()) {
                // original claim does not exist, so it's the effective fallback
                otherClaims.put(fallbackSub, subClaimValue);
                subClaimValue = null;
            } else {
                // original claim still exist, in this case, the fallback can be anything and it does not matter
                otherClaims.put(fallbackSub, randomValueOtherThan(subClaimValue, () -> randomAlphaOfLength(15)));
            }
        }
        List<String> audClaimValue = JwtRealmInspector.getAllowedAudiences(jwtIssuerAndRealm.realm());
        if (fallbackAud != null) {
            if (randomBoolean()) {
                otherClaims.put(fallbackAud, audClaimValue);
                audClaimValue = null;
            } else {
                otherClaims.put(fallbackAud, randomValueOtherThanMany(audClaimValue::contains, () -> randomAlphaOfLength(15)));
            }
        }

        // A bogus auth_time but access_token type does not check it
        if (randomBoolean()) {
            otherClaims.put("auth_time", randomAlphaOfLengthBetween(6, 18));
        }

        final Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        unsignedJwt = JwtTestCase.buildUnsignedJwt(
            randomBoolean() ? null : JOSEObjectType.JWT.toString(), // kty
            randomBoolean() ? null : jwk.getKeyID(), // kid
            algJwkPair.alg(), // alg
            randomAlphaOfLengthBetween(10, 20), // jwtID
            JwtRealmInspector.getAllowedIssuer(jwtIssuerAndRealm.realm()), // iss
            audClaimValue,
            subClaimValue,
            JwtRealmInspector.getPrincipalClaimName(jwtIssuerAndRealm.realm()), // principal claim name
            user.principal(), // principal claim value
            JwtRealmInspector.getGroupsClaimName(jwtIssuerAndRealm.realm()), // group claim name
            List.of(user.roles()), // group claim value
            null,
            Date.from(now.minusSeconds(randomBoolean() ? 0 : 60 * randomLongBetween(5, 10))), // iat
            Date.from(now), // nbf
            Date.from(now.plusSeconds(60 * randomLongBetween(3600, 7200))), // exp
            randomBoolean() ? null : new Nonce(32).toString(),
            otherClaims
        );
        final SecureString signedJWT = signJwt(jwk, unsignedJwt);
        return signedJWT;
    }

    private void noFallback() {
        fallbackSub = null;
        fallbackAud = null;
    }

    private void randomFallbacks() {
        fallbackSub = randomBoolean() ? "_" + randomAlphaOfLength(5) : null;
        fallbackAud = randomBoolean() || fallbackSub == null ? "_" + randomAlphaOfLength(8) : null;
    }
}
