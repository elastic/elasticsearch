/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.util.Base64URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class JwkValidateUtilTests extends JwtTestCase {

    private static final Logger LOGGER = LogManager.getLogger(JwkValidateUtilTests.class);

    // HMAC JWKSet setting can use keys from randomJwkHmac()
    // HMAC key setting cannot use randomJwkHmac(), it must use randomJwkHmacString()
    public void testConvertHmacJwkToStringToJwk() throws Exception {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(randomFrom(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC));

        // Use HMAC random bytes for OIDC JWKSet setting only. Demonstrate encode/decode fails if used in OIDC HMAC key setting.
        final OctetSequenceKey hmacKeyRandomBytes = JwtTestCase.randomJwkHmac(jwsAlgorithm);
        assertThat(this.hmacEncodeDecodeAsPasswordTestHelper(hmacKeyRandomBytes), is(false));

        // Convert HMAC random bytes to UTF8 bytes. This makes it usable as an OIDC HMAC key setting.
        final OctetSequenceKey hmacKeyString1 = JwtTestCase.toOidcJwkHmac(hmacKeyRandomBytes);
        assertThat(this.hmacEncodeDecodeAsPasswordTestHelper(hmacKeyString1), is(true));

        // Generate HMAC UTF8 bytes. This is usable as an OIDC HMAC key setting.
        final OctetSequenceKey hmacKeyString2 = JwtTestCase.toOidcJwkHmac(hmacKeyRandomBytes);
        assertThat(this.hmacEncodeDecodeAsPasswordTestHelper(hmacKeyString2), is(true));
    }

    private boolean hmacEncodeDecodeAsPasswordTestHelper(final OctetSequenceKey hmacKey) {
        // Encode input key as Base64(keyBytes) and Utf8String(keyBytes)
        final String keyBytesToBase64 = hmacKey.getKeyValue().toString();
        final String keyBytesAsUtf8 = hmacKey.getKeyValue().decodeToString();

        // Decode Base64(keyBytes) into new key and compare to original. This always works.
        final OctetSequenceKey decodeFromBase64 = new OctetSequenceKey.Builder(new Base64URL(keyBytesToBase64)).build();
        LOGGER.info("Base64 enc/dec test:\ngen: [" + hmacKey + "]\nenc: [" + keyBytesToBase64 + "]\ndec: [" + decodeFromBase64 + "]\n");
        if (decodeFromBase64.equals(hmacKey) == false) {
            return false;
        }

        // Decode Utf8String(keyBytes) into new key and compare to original. Only works for randomJwkHmacString, fails for randomJwkHmac.
        final OctetSequenceKey decodeFromUtf8 = new OctetSequenceKey.Builder(keyBytesAsUtf8.getBytes(StandardCharsets.UTF_8)).build();
        LOGGER.info("UTF8 enc/dec test:\ngen: [" + hmacKey + "]\nenc: [" + keyBytesAsUtf8 + "]\ndec: [" + decodeFromUtf8 + "]\n");
        return decodeFromUtf8.equals(hmacKey);
    }

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
        final Tuple<List<JWK>, List<String>> filtered = JwkValidateUtil.filterJwksAndAlgorithms(jwks, algs);
        assertThat(jwks.size(), equalTo(filtered.v1().size()));
        assertThat(algs.size(), equalTo(filtered.v2().size()));
    }

    public void testValidateAlgsJwksPkc() throws Exception {
        final List<String> algs = randomOf(randomIntBetween(1, 3), JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC);
        final Map<String, List<JWK>> algsToJwks = JwtTestCase.randomJwks(algs);
        final List<JWK> jwks = algsToJwks.values().stream().flatMap(List::stream).toList();
        // If RSA/EC JWKSet and algorithms are present, verify they are accepted
        final Tuple<List<JWK>, List<String>> filtered = JwkValidateUtil.filterJwksAndAlgorithms(jwks, algs);
        assertThat(jwks.size(), equalTo(filtered.v1().size()));
        assertThat(algs.size(), equalTo(filtered.v2().size()));
    }
}
