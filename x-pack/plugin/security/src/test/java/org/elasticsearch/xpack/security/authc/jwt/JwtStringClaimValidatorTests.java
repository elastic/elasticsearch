/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;

import org.elasticsearch.test.ESTestCase;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class JwtStringClaimValidatorTests extends ESTestCase {

    public void testClaimIsNotString() throws ParseException {
        final String claimName = randomAlphaOfLength(10);
        final String fallbackClaimName = randomAlphaOfLength(12);

        final JwtStringClaimValidator validator;
        final JWTClaimsSet jwtClaimsSet;
        if (randomBoolean()) {
            validator = new JwtStringClaimValidator(claimName, List.of(), randomBoolean());
            // fallback claim is ignored
            jwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, List.of(42), fallbackClaimName, randomAlphaOfLength(8)));
        } else {
            validator = new JwtStringClaimValidator(claimName, Map.of(claimName, fallbackClaimName), List.of(), randomBoolean());
            jwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, List.of(42)));
        }

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), jwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("cannot parse string claim"));
        assertThat(e.getCause(), instanceOf(ParseException.class));
    }

    public void testClaimIsNotSingleValued() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(12);

        final JwtStringClaimValidator validator;
        final JWTClaimsSet jwtClaimsSet;
        if (randomBoolean()) {
            validator = new JwtStringClaimValidator(claimName, List.of(), true);
            // fallback claim is ignored
            jwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, List.of("foo", "bar"), fallbackClaimName, randomAlphaOfLength(8)));
        } else {
            validator = new JwtStringClaimValidator(claimName, Map.of(claimName, fallbackClaimName), List.of(), true);
            jwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, List.of("foo", "bar")));
        }

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), jwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("cannot parse string claim"));
        assertThat(e.getCause(), instanceOf(ParseException.class));
    }

    public void testClaimDoesNotExist() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(12);

        final JwtStringClaimValidator validator;
        final JWTClaimsSet jwtClaimsSet;
        if (randomBoolean()) {
            validator = new JwtStringClaimValidator(claimName, List.of(), randomBoolean());
        } else {
            validator = new JwtStringClaimValidator(claimName, Map.of(claimName, fallbackClaimName), List.of(), randomBoolean());
        }
        jwtClaimsSet = JWTClaimsSet.parse(Map.of());

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), jwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("missing required string claim"));
    }

    public void testMatchingClaimValues() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(12);
        final String claimValue = randomAlphaOfLength(10);
        final boolean singleValuedClaim = randomBoolean();
        final List<String> allowedClaimValues = List.of(claimValue, randomAlphaOfLengthBetween(11, 20));
        final Object incomingClaimValue = singleValuedClaim ? claimValue : randomFrom(claimValue, List.of(claimValue, "other-stuff"));

        final JwtStringClaimValidator validator;
        final JWTClaimsSet validJwtClaimsSet;
        final boolean noFallback = randomBoolean();
        if (noFallback) {
            validator = new JwtStringClaimValidator(claimName, allowedClaimValues, singleValuedClaim);
            // fallback claim is ignored
            validJwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, incomingClaimValue, fallbackClaimName, List.of(42)));
        } else {
            validator = new JwtStringClaimValidator(claimName, Map.of(claimName, fallbackClaimName), allowedClaimValues, singleValuedClaim);
            validJwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, incomingClaimValue));
        }

        try {
            validator.validate(getJwsHeader(), validJwtClaimsSet);
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }

        final JWTClaimsSet invalidJwtClaimsSet;
        if (noFallback) {
            // fallback is ignored (even when it has a valid value) since the main claim exists
            invalidJwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, "not-" + claimValue, fallbackClaimName, claimValue));
        } else {
            invalidJwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, "not-" + claimValue));
        }

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), invalidJwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("does not match allowed claim values"));
    }

    public void testDoesNotSupportWildcardOrRegex() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(12);
        final String claimValue = randomFrom("*", "/.*/");

        final JwtStringClaimValidator validator;
        final JWTClaimsSet invalidJwtClaimsSet;
        final boolean noFallback = randomBoolean();
        if (noFallback) {
            validator = new JwtStringClaimValidator(claimName, List.of(claimValue), randomBoolean());
            // fallback is ignored (even when it has a valid value) since the main claim exists
            invalidJwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, randomAlphaOfLengthBetween(1, 10), fallbackClaimName, claimValue));
        } else {
            validator = new JwtStringClaimValidator(claimName, Map.of(claimName, fallbackClaimName), List.of(claimValue), randomBoolean());
            invalidJwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, randomAlphaOfLengthBetween(1, 10)));
        }

        // It should not match arbitrary claim value because wildcard or regex is not supported
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), invalidJwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("does not match allowed claim values"));

        // It should support literal matching
        final JWTClaimsSet validJwtClaimsSet;
        if (noFallback) {
            // fallback claim is ignored
            validJwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, claimValue, fallbackClaimName, randomAlphaOfLength(10)));
        } else {
            validJwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, claimValue));
        }
        try {
            validator.validate(getJwsHeader(), validJwtClaimsSet);
        } catch (Exception e2) {
            throw new AssertionError("validation should have passed without exception", e2);
        }
    }

    public void testAllowAllSubjects() {
        try {
            JwtStringClaimValidator.ALLOW_ALL_SUBJECTS.validate(
                getJwsHeader(),
                JWTClaimsSet.parse(Map.of("sub", randomAlphaOfLengthBetween(1, 10)))
            );
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> JwtStringClaimValidator.ALLOW_ALL_SUBJECTS.validate(getJwsHeader(), JWTClaimsSet.parse(Map.of()))
        );
        assertThat(e.getMessage(), containsString("missing required string claim"));
    }

    private JWSHeader getJwsHeader() throws ParseException {
        return JWSHeader.parse(Map.of("alg", randomAlphaOfLengthBetween(3, 8)));
    }
}
