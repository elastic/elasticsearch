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
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.junit.Before;

import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
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
    protected String allowedSubjectPattern;
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
            // allowedSubject and allowedSubjectPattern can both be null for
            allowedSubject = randomBoolean() ? randomAlphaOfLength(8) : null;
            allowedSubjectPattern = randomBoolean() ? randomAlphaOfLength(8) : null;
            fallbackSub = null;
            fallbackAud = null;
        } else {
            if (randomBoolean()) {
                allowedSubject = randomAlphaOfLength(8);
                allowedSubjectPattern = randomBoolean() ? randomAlphaOfLength(8) : null;
            } else {
                allowedSubject = randomBoolean() ? randomAlphaOfLength(8) : null;
                allowedSubjectPattern = randomAlphaOfLength(8);
            }
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
                getValidSubClaimValue(),
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
                getValidSubClaimValue(),
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
            )
        );
        requiredClaim.v2().stream().forEach(requiredClaim -> { assertThat(e.getMessage(), containsString(requiredClaim)); });
    }

    public void testMissingRequiredClaims() throws ParseException {
        final Instant now = Instant.now();
        final JWTClaimsSet claimsSet = JWTClaimsSet.parse(
            Map.of(
                "iss",
                allowedIssuer,
                "sub",
                getValidSubClaimValue(),
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
            e.getMessage(),
            containsString(
                "string claim [iss] has value ["
                    + invalidIssuer
                    + "] which does not match allowed claim "
                    + "values ["
                    + allowedIssuer
                    + "]"
            )
        );
    }

    public void testInvalidAllowedSubjectClaimPattern() {
        allowedSubjectPattern = "/invalid pattern";
        final SettingsException e = expectThrows(SettingsException.class, () -> buildJwtAuthenticator());
        assertThat(e.getMessage(), containsString("Invalid patterns for allowed claim values for [sub]."));
    }

    public void testEmptyAllowedSubjectIsInvalid() {
        allowedSubject = null;
        allowedSubjectPattern = null;
        RealmConfig someJWTRealmConfig = buildJWTRealmConfig();
        final Settings.Builder builder = Settings.builder();
        builder.put(someJWTRealmConfig.settings());
        boolean emptySubjects = randomBoolean();
        if (emptySubjects) {
            builder.putList(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS), List.of(""));
        } else {
            builder.putList(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECT_PATTERNS), List.of(""));
        }
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new JwtAuthenticator(
                new RealmConfig(
                    someJWTRealmConfig.identifier(),
                    builder.build(),
                    someJWTRealmConfig.env(),
                    someJWTRealmConfig.threadContext()
                ),
                mock(SSLService.class),
                () -> {}
            )
        );
        if (emptySubjects) {
            assertThat(
                e.getMessage(),
                containsString(
                    "Invalid empty value for [" + RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS) + "]."
                )
            );
        } else {
            assertThat(
                e.getMessage(),
                containsString(
                    "Invalid empty value for ["
                        + RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECT_PATTERNS)
                        + "]."
                )
            );
        }
    }

    public void testNoAllowedSubjectInvalidSettings() {
        allowedSubject = null;
        allowedSubjectPattern = null;
        RealmConfig someJWTRealmConfig = buildJWTRealmConfig();
        {
            final Settings.Builder builder = Settings.builder();
            builder.put(someJWTRealmConfig.settings());
            if (randomBoolean()) {
                builder.putList(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS), List.of());
            } else {
                builder.putNull(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS));
            }
            if (randomBoolean()) {
                builder.putList(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECT_PATTERNS), List.of());
            } else {
                builder.putNull(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECT_PATTERNS));
            }
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new JwtAuthenticator(
                    new RealmConfig(
                        someJWTRealmConfig.identifier(),
                        builder.build(),
                        someJWTRealmConfig.env(),
                        someJWTRealmConfig.threadContext()
                    ),
                    mock(SSLService.class),
                    () -> {}
                )
            );
            assertThat(
                e.getCause().getMessage(),
                containsString(
                    "One of either ["
                        + RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS)
                        + "] or ["
                        + RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECT_PATTERNS)
                        + "] must be specified and not be empty."
                )
            );
        }
        {
            final Settings.Builder builder = Settings.builder();
            builder.put(someJWTRealmConfig.settings());
            if (randomBoolean()) {
                builder.putNull(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS));
            } else {
                builder.putList(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS), List.of());
            }
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new JwtAuthenticator(
                    new RealmConfig(
                        someJWTRealmConfig.identifier(),
                        builder.build(),
                        someJWTRealmConfig.env(),
                        someJWTRealmConfig.threadContext()
                    ),
                    mock(SSLService.class),
                    () -> {}
                )
            );
            assertThat(
                e.getCause().getMessage(),
                containsString(
                    "One of either ["
                        + RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS)
                        + "] or ["
                        + RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECT_PATTERNS)
                        + "] must be specified and not be empty."
                )
            );
        }
        {
            final Settings.Builder builder = Settings.builder();
            builder.put(someJWTRealmConfig.settings());
            if (randomBoolean()) {
                builder.putNull(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECT_PATTERNS));
            } else {
                builder.putList(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECT_PATTERNS), List.of());
            }
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new JwtAuthenticator(
                    new RealmConfig(
                        someJWTRealmConfig.identifier(),
                        builder.build(),
                        someJWTRealmConfig.env(),
                        someJWTRealmConfig.threadContext()
                    ),
                    mock(SSLService.class),
                    () -> {}
                )
            );
            assertThat(
                e.getCause().getMessage(),
                containsString(
                    "One of either ["
                        + RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS)
                        + "] or ["
                        + RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECT_PATTERNS)
                        + "] must be specified and not be empty."
                )
            );
        }
    }

    protected JwtAuthenticator buildJwtAuthenticator() {
        final RealmConfig realmConfig = buildJWTRealmConfig();
        final JwtAuthenticator jwtAuthenticator = spy(new JwtAuthenticator(realmConfig, null, () -> {}));
        // Short circuit signature validation to be always successful since this test class does not test it
        doAnswer(invocation -> {
            final ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(jwtAuthenticator).validateSignature(any(), any(), anyActionListener());
        return jwtAuthenticator;
    }

    protected RealmConfig buildJWTRealmConfig() {
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
        if (allowedSubjectPattern != null) {
            builder.put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECT_PATTERNS), allowedSubjectPattern);
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
        return new RealmConfig(realmIdentifier, settings, TestEnvironment.newEnvironment(settings), new ThreadContext(settings));
    }

    private String getValidSubClaimValue() {
        if (allowedSubject == null && allowedSubjectPattern == null) {
            // any subject is valid
            return randomAlphaOfLengthBetween(10, 18);
        } else if (allowedSubject == null) {
            return allowedSubjectPattern;
        } else if (allowedSubjectPattern == null) {
            return allowedSubject;
        } else {
            return randomFrom(allowedSubject, allowedSubjectPattern);
        }
    }
}
