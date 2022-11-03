/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.text.ParseException;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JwtDateClaimValidatorTests extends ESTestCase {
    private Clock clock;

    @Before
    public void init() {
        clock = mock(Clock.class);
    }

    public void testClaimIsNotDate() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final JwtDateClaimValidator validator = new JwtDateClaimValidator(
            clock,
            claimName,
            TimeValue.ZERO,
            randomFrom(JwtDateClaimValidator.Relationship.values()),
            randomBoolean()
        );

        final SignedJWT jwt = prepareJwt(Map.of(claimName, randomAlphaOfLengthBetween(3, 8)));
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> validator.validate(jwt));
        assertThat(e.getMessage(), containsString("cannot parse date claim"));
        assertThat(e.getCause(), instanceOf(ParseException.class));
    }

    public void testClaimDoesNotExist() throws ParseException {
        final String claimName = randomFrom(randomAlphaOfLengthBetween(3, 8), "iat", "nbf", "auth_time");
        final boolean allowNull = randomBoolean();

        final JwtDateClaimValidator validator = new JwtDateClaimValidator(
            clock,
            claimName,
            TimeValue.ZERO,
            randomFrom(JwtDateClaimValidator.Relationship.values()),
            allowNull
        );

        final SignedJWT jwt = prepareJwt(Map.of());
        if (allowNull) {
            validator.validate(jwt);
        } else {
            final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> validator.validate(jwt));
            assertThat(e.getMessage(), containsString("missing date claim"));
        }
    }

    public void testBeforeNow() throws ParseException {
        final String claimName = randomFrom(randomAlphaOfLengthBetween(10, 18), "iat", "nbf", "auth_time");
        final long allowedSkewInSeconds = randomLongBetween(0, 300);
        final JwtDateClaimValidator validator = new JwtDateClaimValidator(
            clock,
            claimName,
            TimeValue.timeValueSeconds(allowedSkewInSeconds),
            JwtDateClaimValidator.Relationship.BEFORE_NOW,
            false
        );

        final Instant now = Instant.now();
        when(clock.instant()).thenReturn(now);

        final Instant before = now.minusSeconds(randomLongBetween(1 - allowedSkewInSeconds, 600));
        validator.validate(prepareJwt(Map.of(claimName, before.getEpochSecond())));

        final Instant after = now.plusSeconds(randomLongBetween(1 + allowedSkewInSeconds, 600));
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> validator.validate(prepareJwt(Map.of(claimName, after.getEpochSecond())))
        );
        assertThat(e.getMessage(), containsString("date claim [" + claimName + "] must be before now"));
    }

    public void testAfterNow() throws ParseException {
        final String claimName = randomFrom(randomAlphaOfLengthBetween(10, 18), "exp");
        final long allowedSkewInSeconds = randomLongBetween(0, 300);
        final JwtDateClaimValidator validator = new JwtDateClaimValidator(
            clock,
            claimName,
            TimeValue.timeValueSeconds(allowedSkewInSeconds),
            JwtDateClaimValidator.Relationship.AFTER_NOW,
            false
        );

        final Instant now = Instant.now();
        when(clock.instant()).thenReturn(now);

        final Instant before = now.minusSeconds(randomLongBetween(1 + allowedSkewInSeconds, 600));
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> validator.validate(prepareJwt(Map.of(claimName, before.getEpochSecond())))
        );
        assertThat(e.getMessage(), containsString("date claim [" + claimName + "] must be after now"));

        final Instant after = now.plusSeconds(randomLongBetween(1 - allowedSkewInSeconds, 600));
        validator.validate(prepareJwt(Map.of(claimName, after.getEpochSecond())));
    }

    private SignedJWT prepareJwt(Map<String, Object> m) throws ParseException {
        final SignedJWT jwt = mock(SignedJWT.class);
        final JWTClaimsSet jwtClaimsSet = JWTClaimsSet.parse(m);
        when(jwt.getJWTClaimsSet()).thenReturn(jwtClaimsSet);
        return jwt;
    }
}
