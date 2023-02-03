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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.junit.Before;

import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public abstract class JwtAuthenticatorTests extends ESTestCase {

    protected String realmName;
    protected String allowedAlgorithm;
    protected String allowedIssuer;
    @Nullable
    protected String allowedSubject;
    protected String allowedAudience;
    protected String fallbackSub;
    protected String fallbackAud;
    protected Tuple<String, List<String>> requiredClaim;

    protected abstract JwtRealmSettings.TokenType getTokenType();

    @Before
    public void beforeTest() {
        realmName = randomAlphaOfLengthBetween(3, 8);
        allowedIssuer = randomAlphaOfLength(6);
        allowedAlgorithm = randomFrom(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC);
        if (getTokenType() == JwtRealmSettings.TokenType.ID_TOKEN) {
            allowedSubject = randomBoolean() ? randomAlphaOfLength(8) : null;
            fallbackSub = null;
            fallbackAud = null;
        } else {
            allowedSubject = randomAlphaOfLength(8);
            fallbackSub = randomBoolean() ? "_" + randomAlphaOfLength(5) : null;
            fallbackAud = randomBoolean() ? "_" + randomAlphaOfLength(8) : null;
        }
        allowedAudience = randomAlphaOfLength(10);
        requiredClaim = Tuple.tuple(randomAlphaOfLength(8), randomList(1, 3, () -> randomAlphaOfLengthBetween(8, 18)));
    }

    public void testRequiredClaims() throws ParseException {
        final Instant now = Instant.now();
        final String requireClaimValue = randomFrom(requiredClaim.v2());
        final JWTClaimsSet claimsSet = JWTClaimsSet.parse(
            Map.of(
                "iss",
                allowedIssuer,
                "sub",
                allowedSubject == null ? randomAlphaOfLengthBetween(10, 18) : allowedSubject,
                "aud",
                allowedAudience,
                requiredClaim.v1(),
                randomBoolean() ? requireClaimValue : List.of(requireClaimValue, "some-other-value"),
                "iat",
                now.minus(1, ChronoUnit.DAYS).getEpochSecond(),
                "exp",
                now.plus(1, ChronoUnit.DAYS).getEpochSecond()
            )
        );
        final SignedJWT signedJWT = new SignedJWT(
            JWSHeader.parse(Map.of("alg", allowedAlgorithm)).toBase64URL(),
            claimsSet.toPayload().toBase64URL(),
            Base64URL.encode("signature")
        );

        final JwtAuthenticationToken jwtAuthenticationToken = mock(JwtAuthenticationToken.class);
        when(jwtAuthenticationToken.getSignedJWT()).thenReturn(signedJWT);
        when(jwtAuthenticationToken.getJWTClaimsSet()).thenReturn(signedJWT.getJWTClaimsSet());

        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        final JwtAuthenticator jwtAuthenticator = buildJwtAuthenticator();
        jwtAuthenticator.authenticate(jwtAuthenticationToken, future);
        assertThat(future.actionGet(), equalTo(claimsSet));
    }

    public void testMismatchedRequiredClaims() throws ParseException {
        final Instant now = Instant.now();
        final String mismatchRequiredClaimValue = randomValueOtherThanMany(
            requiredClaim.v2()::contains,
            () -> randomAlphaOfLengthBetween(3, 18)
        );
        final JWTClaimsSet claimsSet = JWTClaimsSet.parse(
            Map.of(
                "iss",
                allowedIssuer,
                "sub",
                allowedSubject == null ? randomAlphaOfLengthBetween(10, 18) : allowedSubject,
                "aud",
                allowedAudience,
                requiredClaim.v1(),
                mismatchRequiredClaimValue,
                "iat",
                now.minus(1, ChronoUnit.DAYS).getEpochSecond(),
                "exp",
                now.plus(1, ChronoUnit.DAYS).getEpochSecond()
            )
        );
        final SignedJWT signedJWT = new SignedJWT(
            JWSHeader.parse(Map.of("alg", allowedAlgorithm)).toBase64URL(),
            claimsSet.toPayload().toBase64URL(),
            Base64URL.encode("signature")
        );

        final JwtAuthenticationToken jwtAuthenticationToken = mock(JwtAuthenticationToken.class);
        when(jwtAuthenticationToken.getSignedJWT()).thenReturn(signedJWT);
        when(jwtAuthenticationToken.getJWTClaimsSet()).thenReturn(signedJWT.getJWTClaimsSet());

        final PlainActionFuture<JWTClaimsSet> future = new PlainActionFuture<>();
        final JwtAuthenticator jwtAuthenticator = buildJwtAuthenticator();
        jwtAuthenticator.authenticate(jwtAuthenticationToken, future);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(
            e.getMessage(),
            containsString(
                "string claim ["
                    + requiredClaim.v1()
                    + "] has value ["
                    + mismatchRequiredClaimValue
                    + "] which does not match allowed claim values ["
                    + requiredClaim.v2().stream().collect(Collectors.joining(","))
                    + "]"
            )
        );
    }

    public void testMissingRequiredClaims() throws ParseException {
        final Instant now = Instant.now();
        final JWTClaimsSet claimsSet = JWTClaimsSet.parse(
            Map.of(
                "iss",
                allowedIssuer,
                "sub",
                allowedSubject == null ? randomAlphaOfLengthBetween(10, 18) : allowedSubject,
                "aud",
                allowedAudience,
                "iat",
                now.minus(1, ChronoUnit.DAYS).getEpochSecond(),
                "exp",
                now.plus(1, ChronoUnit.DAYS).getEpochSecond()
            )
        );
        final SignedJWT signedJWT = new SignedJWT(
            JWSHeader.parse(Map.of("alg", allowedAlgorithm)).toBase64URL(),
            claimsSet.toPayload().toBase64URL(),
            Base64URL.encode("signature")
        );

        final JwtAuthenticationToken jwtAuthenticationToken = mock(JwtAuthenticationToken.class);
        when(jwtAuthenticationToken.getSignedJWT()).thenReturn(signedJWT);
        when(jwtAuthenticationToken.getJWTClaimsSet()).thenReturn(signedJWT.getJWTClaimsSet());

        // Required claim is mandatory when configured
        final PlainActionFuture<JWTClaimsSet> future1 = new PlainActionFuture<>();
        buildJwtAuthenticator().authenticate(jwtAuthenticationToken, future1);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future1::actionGet);
        assertThat(e.getMessage(), containsString("missing required string claim [" + requiredClaim.v1() + "]"));

        // Remove required claim from settings, the JWT now works
        requiredClaim = null;
        final PlainActionFuture<JWTClaimsSet> future2 = new PlainActionFuture<>();
        buildJwtAuthenticator().authenticate(jwtAuthenticationToken, future2);
        assertThat(future2.actionGet(), equalTo(claimsSet));
    }

    protected IllegalArgumentException doTestSubjectIsRequired(JwtAuthenticator jwtAuthenticator) throws ParseException {
        final SignedJWT signedJWT = new SignedJWT(
            JWSHeader.parse(Map.of("alg", allowedAlgorithm)).toBase64URL(),
            JWTClaimsSet.parse(Map.of("iss", allowedIssuer)).toPayload().toBase64URL(),
            Base64URL.encode("signature")
        );
        final JwtAuthenticationToken jwtAuthenticationToken = mock(JwtAuthenticationToken.class);
        when(jwtAuthenticationToken.getSignedJWT()).thenReturn(signedJWT);
        when(jwtAuthenticationToken.getJWTClaimsSet()).thenReturn(signedJWT.getJWTClaimsSet());

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
        when(jwtAuthenticationToken.getSignedJWT()).thenReturn(signedJWT);
        when(jwtAuthenticationToken.getJWTClaimsSet()).thenReturn(signedJWT.getJWTClaimsSet());

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

    protected JwtAuthenticator buildJwtAuthenticator() {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(JwtRealmSettings.TYPE, realmName);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.HMAC_KEY), randomAlphaOfLength(40));
        final Settings.Builder builder = Settings.builder()
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS), allowedAlgorithm)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_ISSUER), allowedIssuer)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_AUDIENCES), allowedAudience)
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

        if (requiredClaim != null) {
            final String requiredClaimsKey = RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.REQUIRED_CLAIMS) + requiredClaim
                .v1();
            if (requiredClaim.v2().size() == 1 && randomBoolean()) {
                builder.put(requiredClaimsKey, requiredClaim.v2().get(0));
            } else {
                builder.putList(requiredClaimsKey, requiredClaim.v2());
            }
        }

        final Settings settings = builder.build();

        final RealmConfig realmConfig = new RealmConfig(
            realmIdentifier,
            settings,
            TestEnvironment.newEnvironment(settings),
            new ThreadContext(settings)
        );

        final JwtAuthenticator jwtAuthenticator = spy(new JwtAuthenticator(realmConfig, null, () -> {}));
        // Short circuit signature validation to be always successful since this test class does not test it
        doAnswer(invocation -> {
            final ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(jwtAuthenticator).validateSignature(any(), any(), anyActionListener());

        return jwtAuthenticator;
    }
}
