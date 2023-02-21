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

import static org.hamcrest.Matchers.containsString;

public class JwtAlgorithmValidatorTests extends ESTestCase {

    public void testValidHeader() throws ParseException {
        final String algorithm = randomAlphaOfLengthBetween(3, 8);
        final JwtAlgorithmValidator validator = new JwtAlgorithmValidator(List.of(algorithm, randomAlphaOfLengthBetween(3, 8)));

        final JWSHeader jwsHeader = JWSHeader.parse(Map.of("alg", algorithm));

        try {
            validator.validate(jwsHeader, JWTClaimsSet.parse(Map.of()));
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }
    }

    public void testMismatchingAlgorithm() throws ParseException {
        final String algorithm = randomAlphaOfLength(5);
        final JwtAlgorithmValidator validator = new JwtAlgorithmValidator(randomList(1, 5, () -> randomAlphaOfLength(8)));

        final JWSHeader jwsHeader = JWSHeader.parse(Map.of("alg", algorithm));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(jwsHeader, JWTClaimsSet.parse(Map.of()))
        );
        assertThat(e.getMessage(), containsString("invalid JWT algorithm"));
    }
}
