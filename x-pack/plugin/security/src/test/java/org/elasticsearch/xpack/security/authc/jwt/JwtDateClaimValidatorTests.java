/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;

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

        final JWTClaimsSet jwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, randomAlphaOfLengthBetween(3, 8)));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), jwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("cannot parse date claim"));
        assertThat(e.getCause(), instanceOf(ParseException.class));
    }

    public void testClaimDoesNotExist() throws ParseException {
        final String claimName = randomFrom(randomAlphaOfLengthBetween(3, 8), "iat", "nbf", "auth_time");

        final JwtDateClaimValidator validator = new JwtDateClaimValidator(
            clock,
            claimName,
            TimeValue.ZERO,
            randomFrom(JwtDateClaimValidator.Relationship.values()),
            false
        );

        final JWTClaimsSet jwtClaimsSet = JWTClaimsSet.parse(Map.of());
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), jwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("missing required date claim"));
    }

    public void testClaimDoesNotExistIsOKWhenAllowNullIsTrue() throws ParseException {
        final String claimName = randomFrom(randomAlphaOfLengthBetween(3, 8), "iat", "nbf", "auth_time");

        final JwtDateClaimValidator validator = new JwtDateClaimValidator(
            clock,
            claimName,
            TimeValue.ZERO,
            randomFrom(JwtDateClaimValidator.Relationship.values()),
            true
        );

        final JWTClaimsSet jwtClaimsSet = JWTClaimsSet.parse(Map.of());
        try {
            validator.validate(getJwsHeader(), jwtClaimsSet);
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
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
        try {
            validator.validate(getJwsHeader(), JWTClaimsSet.parse(Map.of(claimName, before.getEpochSecond())));
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }

        final Instant after = now.plusSeconds(randomLongBetween(1 + allowedSkewInSeconds, 600));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), JWTClaimsSet.parse(Map.of(claimName, after.getEpochSecond())))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "date claim ["
                    + claimName
                    + "] value ["
                    + after.getEpochSecond() * 1000
                    + "] must be before now ["
                    + now.toEpochMilli()
                    + "]"
            )
        );
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
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), JWTClaimsSet.parse(Map.of(claimName, before.getEpochSecond())))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "date claim ["
                    + claimName
                    + "] value ["
                    + before.getEpochSecond() * 1000
                    + "] must be after now ["
                    + now.toEpochMilli()
                    + "]"
            )
        );

        final Instant after = now.plusSeconds(randomLongBetween(1 - allowedSkewInSeconds, 600));
        try {
            validator.validate(getJwsHeader(), JWTClaimsSet.parse(Map.of(claimName, after.getEpochSecond())));
        } catch (Exception exception) {
            throw new AssertionError("validation should have passed without exception", e);
        }
    }

    private JWSHeader getJwsHeader() throws ParseException {
        return JWSHeader.parse(Map.of("alg", randomAlphaOfLengthBetween(3, 8)));
    }
}
