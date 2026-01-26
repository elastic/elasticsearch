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
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class JwtTypeValidatorTests extends ESTestCase {

    public void testValidIdTokenType() throws ParseException {
        final String algorithm = randomAlphaOfLengthBetween(3, 8);

        final JWSHeader jwsHeader = JWSHeader.parse(
            randomFrom(
                // typ is allowed to be missing
                Map.of("alg", algorithm),
                Map.of("typ", "JWT", "alg", algorithm)
            )
        );

        try {
            JwtTypeValidator.ID_TOKEN_INSTANCE.validate(jwsHeader, JWTClaimsSet.parse(Map.of()));
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }
    }

    public void testValidAccessTokenType() throws ParseException {
        final String algorithm = randomAlphaOfLengthBetween(3, 8);

        final JWSHeader jwsHeader = JWSHeader.parse(
            randomFrom(
                // typ is allowed to be missing
                Map.of("alg", algorithm),
                Map.of("typ", "JWT", "alg", algorithm),
                Map.of("typ", "at+jwt", "alg", algorithm),
                Map.of("typ", "AT+JWT", "alg", algorithm)
            )
        );

        try {
            JwtTypeValidator.ACCESS_TOKEN_INSTANCE.validate(jwsHeader, JWTClaimsSet.parse(Map.of()));
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }
    }

    public void testInvalidType() throws ParseException {
        final JwtTypeValidator validator = randomFrom(JwtTypeValidator.ID_TOKEN_INSTANCE, JwtTypeValidator.ACCESS_TOKEN_INSTANCE);

        final JWSHeader jwsHeader = JWSHeader.parse(
            Map.of("typ", randomAlphaOfLengthBetween(4, 8), "alg", randomAlphaOfLengthBetween(3, 8))
        );

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(jwsHeader, JWTClaimsSet.parse(Map.of()))
        );
        assertThat(e.getMessage(), containsString("invalid jwt typ header"));
    }
}
