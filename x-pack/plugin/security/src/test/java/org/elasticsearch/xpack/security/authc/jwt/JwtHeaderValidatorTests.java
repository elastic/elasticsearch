/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.test.ESTestCase;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class JwtHeaderValidatorTests extends ESTestCase {

    public void testValidHeader() throws ParseException {
        final String algorithm = randomAlphaOfLengthBetween(3, 8);
        final JwtHeaderValidator validator = new JwtHeaderValidator(List.of(algorithm, randomAlphaOfLengthBetween(3, 8)));

        // typ is allowed to be missing
        final JWSHeader jwsHeader = JWSHeader.parse(randomFrom(Map.of("alg", algorithm), Map.of("typ", "JWT", "alg", algorithm)));

        try {
            validator.validate(jwsHeader);
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }
    }

    public void testInvalidTyp() throws ParseException {
        final String algorithm = randomAlphaOfLengthBetween(3, 8);
        final JwtHeaderValidator validator = new JwtHeaderValidator(List.of(algorithm, randomAlphaOfLengthBetween(3, 8)));

        final JWSHeader jwsHeader = JWSHeader.parse(Map.of("typ", randomAlphaOfLengthBetween(4, 8), "alg", algorithm));

        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> validator.validate(jwsHeader));
        assertThat(e.getMessage(), containsString("invalid jwt typ header"));
    }

    public void testMismatchingAlgorithm() throws ParseException {
        final String algorithm = randomAlphaOfLength(5);
        final JwtHeaderValidator validator = new JwtHeaderValidator(randomList(1, 5, () -> randomAlphaOfLength(8)));

        final JWSHeader jwsHeader = JWSHeader.parse(Map.of("alg", algorithm));
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> validator.validate(jwsHeader));
        assertThat(e.getMessage(), containsString("invalid JWT algorithm"));
    }
}
