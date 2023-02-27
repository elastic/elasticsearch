/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;

import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class JwkValidateUtilTests extends JwtTestCase {

    // Test decode bytes as UTF8 to String, encode back to UTF8, and compare to original bytes. If same, it is safe for OIDC JWK encode.
    static boolean isJwkHmacOidcSafe(final JWK jwk) {
        if (jwk instanceof OctetSequenceKey jwkHmac) {
            final byte[] rawKeyBytes = jwkHmac.getKeyValue().decode();
            return Arrays.equals(rawKeyBytes, new String(rawKeyBytes, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8));
        }
        return true;
    }

    static boolean areJwkHmacOidcSafe(final Collection<JWK> jwks) {
        for (final JWK jwk : jwks) {
            if (JwkValidateUtilTests.isJwkHmacOidcSafe(jwk) == false) {
                return false;
            }
        }
        return true;
    }

    public void testComputeBitLengthRsa() throws Exception {
        for (final String signatureAlgorithmRsa : JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_RSA) {
            final JWK jwk = JwtTestCase.randomJwkRsa(JWSAlgorithm.parse(signatureAlgorithmRsa));
            final int minLength = JwkValidateUtil.computeBitLengthRsa(jwk.toRSAKey().toPublicKey());
            assertThat(minLength, anyOf(equalTo(2048), equalTo(3072)));
        }
    }

    public void testAlgsJwksAllNotFiltered() throws Exception {
        filterJwksAndAlgorithmsTestHelper(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
    }

    public void testAlgsJwksAllHmacNotFiltered() throws Exception {
        filterJwksAndAlgorithmsTestHelper(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC);
    }

    public void testAlgsJwksAllPkcNotFiltered() throws Exception {
        filterJwksAndAlgorithmsTestHelper(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC);
    }

    private void filterJwksAndAlgorithmsTestHelper(final List<String> candidateAlgs) throws JOSEException {
        final List<String> algsRandom = randomOfMinUnique(2, candidateAlgs); // duplicates allowed
        final List<JwtIssuer.AlgJwkPair> algJwkPairsAll = JwtTestCase.randomJwks(algsRandom, randomBoolean());
        final List<JWK> jwks = algJwkPairsAll.stream().map(JwtIssuer.AlgJwkPair::jwk).toList();

        // verify no filtering
        final JwkSetLoader.JwksAlgs nonFiltered = JwkValidateUtil.filterJwksAndAlgorithms(jwks, algsRandom);
        assertThat(jwks.size(), equalTo(nonFiltered.jwks().size()));
        assertThat(algsRandom.size(), equalTo(nonFiltered.algs().size()));
    }
}
