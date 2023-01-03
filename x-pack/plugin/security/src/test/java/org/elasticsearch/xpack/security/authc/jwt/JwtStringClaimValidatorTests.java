/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.test.ESTestCase;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class JwtStringClaimValidatorTests extends ESTestCase {

    public void testClaimIsNotString() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final JwtStringClaimValidator validator = new JwtStringClaimValidator(claimName, List.of(), randomBoolean());

        final JWTClaimsSet jwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, List.of(42)));
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> validator.validate(getJwsHeader(), jwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("cannot parse string claim"));
        assertThat(e.getCause(), instanceOf(ParseException.class));
    }

    public void testClaimIsNotSingleValued() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final JwtStringClaimValidator validator = new JwtStringClaimValidator(claimName, List.of(), true);

        final JWTClaimsSet jwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, List.of("foo", "bar")));
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> validator.validate(getJwsHeader(), jwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("cannot parse string claim"));
        assertThat(e.getCause(), instanceOf(ParseException.class));
    }

    public void testClaimDoesNotExist() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final JwtStringClaimValidator validator = new JwtStringClaimValidator(claimName, List.of(), randomBoolean());

        final JWTClaimsSet jwtClaimsSet = JWTClaimsSet.parse(Map.of());
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> validator.validate(getJwsHeader(), jwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("missing required string claim"));
    }

    public void testMatchingClaimValues() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String claimValue = randomAlphaOfLength(10);
        final boolean singleValuedClaim = randomBoolean();
        final JwtStringClaimValidator validator = new JwtStringClaimValidator(
            claimName,
            List.of(claimValue, randomAlphaOfLengthBetween(11, 20)),
            singleValuedClaim
        );

        final JWTClaimsSet validJwtClaimsSet = JWTClaimsSet.parse(
            Map.of(claimName, singleValuedClaim ? claimValue : randomFrom(claimValue, List.of(claimValue, "other-stuff")))
        );
        try {
            validator.validate(getJwsHeader(), validJwtClaimsSet);
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }

        final JWTClaimsSet invalidJwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, "not-" + claimValue));
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> validator.validate(getJwsHeader(), invalidJwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("does not match allowed claim values"));
    }

    public void testDoesNotSupportWildcardOrRegex() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String claimValue = randomFrom("*", "/.*/");
        final JwtStringClaimValidator validator = new JwtStringClaimValidator(claimName, List.of(claimValue), randomBoolean());

        // It should not match arbitrary claim value because wildcard or regex is not supported
        final JWTClaimsSet invalidJwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, randomAlphaOfLengthBetween(1, 10)));
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> validator.validate(getJwsHeader(), invalidJwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("does not match allowed claim values"));

        // It should support literal matching
        final JWTClaimsSet validJwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, claimValue));
        try {
            validator.validate(getJwsHeader(), validJwtClaimsSet);
        } catch (Exception e2) {
            throw new AssertionError("validation should have passed without exception", e2);
        }
    }

    private JWSHeader getJwsHeader() throws ParseException {
        return JWSHeader.parse(Map.of("alg", randomAlphaOfLengthBetween(3, 8)));
    }
}
