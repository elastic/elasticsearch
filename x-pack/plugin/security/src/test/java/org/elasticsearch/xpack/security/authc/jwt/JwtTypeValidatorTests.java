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

    public void testValidType() throws ParseException {
        final String algorithm = randomAlphaOfLengthBetween(3, 8);

        // typ is allowed to be missing
        final JWSHeader jwsHeader = JWSHeader.parse(
            randomFrom(Map.of("alg", randomAlphaOfLengthBetween(3, 8)), Map.of("typ", "JWT", "alg", randomAlphaOfLengthBetween(3, 8)))
        );

        try {
            JwtTypeValidator.INSTANCE.validate(jwsHeader, JWTClaimsSet.parse(Map.of()));
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }
    }

    public void testInvalidType() throws ParseException {

        final JWSHeader jwsHeader = JWSHeader.parse(
            Map.of("typ", randomAlphaOfLengthBetween(4, 8), "alg", randomAlphaOfLengthBetween(3, 8))
        );

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> JwtTypeValidator.INSTANCE.validate(jwsHeader, JWTClaimsSet.parse(Map.of()))
        );
        assertThat(e.getMessage(), containsString("invalid jwt typ header"));
    }
}
