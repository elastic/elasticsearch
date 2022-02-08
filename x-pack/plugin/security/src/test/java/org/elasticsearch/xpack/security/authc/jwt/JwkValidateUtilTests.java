/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.jwk.JWK;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("checkstyle:MissingJavadocMethod")
public class JwkValidateUtilTests extends JwtTestCase {

    public void testComputeBitLengthRsa() throws Exception {
        for (final String signatureAlgorithmRsa : JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_RSA) {
            final JWK jwk = JwtTestCase.randomJwk(signatureAlgorithmRsa);
            final int minLength = JwkValidateUtil.computeBitLengthRsa(jwk.toRSAKey().toPublicKey());
            assertThat(minLength, is(anyOf(equalTo(2048), equalTo(3072))));
        }
    }

    public void testValidateAlgsJwksHmac() throws Exception {
        final List<String> algs = randomOf(randomIntBetween(1, 3), JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC);
        final Map<String, List<JWK>> algsToJwks = JwtTestCase.randomJwks(algs);
        final List<JWK> jwks = algsToJwks.values().stream().flatMap(List::stream).toList();
        // If HMAC JWKSet and algorithms are present, verify they are accepted
        final Tuple<List<String>, List<JWK>> filtered = JwkValidateUtil.filterJwksAndAlgorithms(jwks, algs);
        assertThat(algs.size(), equalTo(filtered.v1().size()));
        assertThat(jwks.size(), equalTo(filtered.v2().size()));
    }

    public void testValidateAlgsJwksPkc() throws Exception {
        final List<String> algs = randomOf(randomIntBetween(1, 3), JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC);
        final Map<String, List<JWK>> algsToJwks = JwtTestCase.randomJwks(algs);
        final List<JWK> jwks = algsToJwks.values().stream().flatMap(List::stream).toList();
        // If RSA/EC JWKSet and algorithms are present, verify they are accepted
        final Tuple<List<String>, List<JWK>> filtered = JwkValidateUtil.filterJwksAndAlgorithms(jwks, algs);
        assertThat(algs.size(), equalTo(filtered.v1().size()));
        assertThat(jwks.size(), equalTo(filtered.v2().size()));
    }
}
