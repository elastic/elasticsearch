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

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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

public class JwtAuthenticatorTests extends ESTestCase {

    public void testInvalidIssuerIsCheckedBeforeAlgorithm() throws ParseException {
        final String realmName = randomAlphaOfLengthBetween(3, 8);
        final String allowedIssuer = randomAlphaOfLength(6);
        final String allowedAlgorithm = randomFrom(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC);
        final JwtAuthenticator jwtAuthenticator = buildJwtAuthenticator(realmName, allowedAlgorithm, allowedIssuer);

        // A JWT token that has mismatch for both algorithm and issuer
        final String invalidAlgorithm = randomValueOtherThan(allowedAlgorithm, () -> randomAlphaOfLengthBetween(3, 8));
        final String invalidIssuer = randomValueOtherThan(allowedIssuer, () -> randomAlphaOfLengthBetween(3, 8));
        final SignedJWT signedJWT = new SignedJWT(
            JWSHeader.parse(Map.of("alg", invalidAlgorithm)).toBase64URL(),
            JWTClaimsSet.parse(Map.of("iss", invalidIssuer)).toPayload().toBase64URL(),
            Base64URL.encode("signature")
        );
        final JwtAuthenticationToken jwtAuthenticationToken = mock(JwtAuthenticationToken.class);
        when(jwtAuthenticationToken.getEndUserSignedJwt()).thenReturn(new SecureString(signedJWT.serialize().toCharArray()));

        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        jwtAuthenticator.authenticate(jwtAuthenticationToken, future);

        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);

        assertThat(
            e,
            throwableWithMessage(
                "string claim [iss] has value [" + invalidIssuer + "] which does not match allowed claim values [" + allowedIssuer + "]"
            )
        );
    }

    private JwtAuthenticator buildJwtAuthenticator(String realmName, String allowedAlgorithm, String allowedIssuer) {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(JwtRealmSettings.TYPE, realmName);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.HMAC_KEY), randomAlphaOfLength(40));
        final Settings settings = Settings.builder()
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS), allowedAlgorithm)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_ISSUER), allowedIssuer)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_AUDIENCES), randomAlphaOfLength(7))
            .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), randomIntBetween(0, 99))
            .put("path.home", randomAlphaOfLength(10))
            .setSecureSettings(secureSettings)
            .build();

        final RealmConfig realmConfig = new RealmConfig(
            realmIdentifier,
            settings,
            TestEnvironment.newEnvironment(settings),
            new ThreadContext(settings)
        );

        return new JwtAuthenticator(realmConfig, null, () -> {});
    }
}
