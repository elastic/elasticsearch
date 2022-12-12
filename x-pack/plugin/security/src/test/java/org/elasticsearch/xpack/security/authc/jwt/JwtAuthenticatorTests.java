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

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.text.ParseException;
import java.util.Map;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class JwtAuthenticatorTests extends ESTestCase {

    protected String realmName;
    protected String allowedAlgorithm;
    protected String allowedIssuer;
    @Nullable
    protected String allowedSubject;
    protected String allowedAudience;

    protected abstract JwtRealmSettings.TokenType getTokenType();

    protected void doBeforeTest() {
        realmName = randomAlphaOfLengthBetween(3, 8);
        allowedIssuer = randomAlphaOfLength(6);
        allowedAlgorithm = randomFrom(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC);
        if (getTokenType() == JwtRealmSettings.TokenType.ID_TOKEN) {
            allowedSubject = randomBoolean() ? randomAlphaOfLength(8) : null;
        } else {
            allowedSubject = randomAlphaOfLength(8);
        }
        allowedAudience = randomAlphaOfLength(10);
    }

    protected IllegalArgumentException doTestSubjectIsRequired(JwtAuthenticator jwtAuthenticator) throws ParseException {
        final SignedJWT signedJWT = new SignedJWT(
            JWSHeader.parse(Map.of("alg", allowedAlgorithm)).toBase64URL(),
            JWTClaimsSet.parse(Map.of("iss", allowedIssuer)).toPayload().toBase64URL(),
            Base64URL.encode("signature")
        );
        final JwtAuthenticationToken jwtAuthenticationToken = mock(JwtAuthenticationToken.class);
        when(jwtAuthenticationToken.getEndUserSignedJwt()).thenReturn(new SecureString(signedJWT.serialize().toCharArray()));

        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        jwtAuthenticator.authenticate(jwtAuthenticationToken, future);
        return expectThrows(IllegalArgumentException.class, future::actionGet);
    }

    protected void doTestInvalidIssuerIsCheckedBeforeAlgorithm(JwtAuthenticator jwtAuthenticator) throws ParseException {
        // A JWT token that has mismatch for both algorithm and issuer
        final String invalidAlgorithm = randomValueOtherThan(allowedAlgorithm, () -> randomAlphaOfLengthBetween(3, 8));
        final String invalidIssuer = randomValueOtherThan(allowedIssuer, () -> randomAlphaOfLengthBetween(3, 8));
        final SignedJWT signedJWT = new SignedJWT(
            JWSHeader.parse(Map.of("alg", invalidAlgorithm)).toBase64URL(),
            JWTClaimsSet.parse(Map.of("iss", invalidIssuer, "sub", randomAlphaOfLengthBetween(3, 8))).toPayload().toBase64URL(),
            Base64URL.encode("signature")
        );
        final JwtAuthenticationToken jwtAuthenticationToken = mock(JwtAuthenticationToken.class);
        when(jwtAuthenticationToken.getEndUserSignedJwt()).thenReturn(new SecureString(signedJWT.serialize().toCharArray()));

        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        jwtAuthenticator.authenticate(jwtAuthenticationToken, future);

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(
            e,
            throwableWithMessage(
                "string claim [iss] has value [" + invalidIssuer + "] which does not match allowed claim values [" + allowedIssuer + "]"
            )
        );
    }

    protected JwtAuthenticator buildJwtAuthenticator(String fallbackSub, String fallbackAud) {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(JwtRealmSettings.TYPE, realmName);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.HMAC_KEY), randomAlphaOfLength(40));
        final Settings.Builder builder = Settings.builder()
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS), allowedAlgorithm)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_ISSUER), allowedIssuer)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_AUDIENCES), randomAlphaOfLength(7))
            .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), randomIntBetween(0, 99))
            .put("path.home", randomAlphaOfLength(10))
            .setSecureSettings(secureSettings);

        if (allowedSubject != null) {
            builder.put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS), allowedSubject);
        }

        if (getTokenType() == JwtRealmSettings.TokenType.ID_TOKEN) {
            if (randomBoolean()) {
                builder.put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.TOKEN_TYPE), "id_token");
            }
        } else {
            builder.put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.TOKEN_TYPE), "access_token");
        }

        if (fallbackSub != null) {
            builder.put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_SUB_CLAIM), fallbackSub);
        }
        if (fallbackAud != null) {
            builder.put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_AUD_CLAIM), fallbackAud);
        }

        final Settings settings = builder.build();

        final RealmConfig realmConfig = new RealmConfig(
            realmIdentifier,
            settings,
            TestEnvironment.newEnvironment(settings),
            new ThreadContext(settings)
        );

        return new JwtAuthenticator(realmConfig, null, () -> {});
    }
}
