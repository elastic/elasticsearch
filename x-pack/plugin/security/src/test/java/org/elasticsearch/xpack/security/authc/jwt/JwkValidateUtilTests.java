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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class JwkValidateUtilTests extends JwtTestCase {

    private static final Logger LOGGER = LogManager.getLogger(JwkValidateUtilTests.class);

    public void testComputeBitLengthRsa() throws Exception {
        for (final String signatureAlgorithmRsa : JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_RSA) {
            final JWK jwk = JwtTestCase.randomJwkRsa(JWSAlgorithm.parse(signatureAlgorithmRsa));
            final int minLength = JwkValidateUtil.computeBitLengthRsa(jwk.toRSAKey().toPublicKey());
            assertThat(minLength, is(anyOf(equalTo(2048), equalTo(3072))));
        }
    }

    public void testAlgsJwksAllNotFiltered() throws Exception {
        this.filterJwksAndAlgorithmsTestHelper(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
    }

    public void testAlgsJwksAllHmacNotFiltered() throws Exception {
        this.filterJwksAndAlgorithmsTestHelper(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC);
    }

    public void testAlgsJwksAllPkcNotFiltered() throws Exception {
        this.filterJwksAndAlgorithmsTestHelper(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC);
    }

    private void filterJwksAndAlgorithmsTestHelper(final List<String> candidateAlgs) throws JOSEException {
        final List<String> algsRandom = randomOfMinUnique(2, candidateAlgs); // duplicates allowed
        final List<JwtIssuer.AlgJwkPair> algJwkPairsAll = JwtTestCase.randomJwks(algsRandom, randomBoolean());
        final List<JWK> jwks = algJwkPairsAll.stream().map(JwtIssuer.AlgJwkPair::jwk).toList();
        final List<String> algsAll = algJwkPairsAll.stream().map(JwtIssuer.AlgJwkPair::alg).toList();
        final List<JWK> jwksAll = algJwkPairsAll.stream().map(JwtIssuer.AlgJwkPair::jwk).toList();

        // verify no filtering
        final JwtRealm.FilteredJwksAlgs nonFiltered = JwkValidateUtil.filterJwksAndAlgorithms(jwks, algsRandom);
        assertThat(jwks.size(), equalTo(nonFiltered.jwks().size()));
        assertThat(algsRandom.size(), equalTo(nonFiltered.algs().size()));
    }
}
