/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.PemUtils;

import javax.crypto.spec.SecretKeySpec;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;

import static org.hamcrest.Matchers.containsString;

public class JwtSignatureValidatorTests extends ESTestCase {

    public void testOnlyAcceptCorrectKeyAndAlgorithm() throws Exception {
        SignatureAlgorithm hmacAlgo = randomFrom(SignatureAlgorithm.getHmacAlgorithms());
        final SecretKeySpec hmacKey =
            new SecretKeySpec("144753a689a6508d7c7cd02752d7138e".getBytes(), hmacAlgo.getJcaAlgoName());
        SignatureAlgorithm rsaAlgo = randomFrom(SignatureAlgorithm.getRsaAlgorithms());
        Path publicKeyPath = PathUtils.get(IdTokenParserTests.class.getResource
            ("/org/elasticsearch/xpack/security/authc/oidc/rsa_public_key.pem").toURI());
        final PublicKey rsaPublicKey = PemUtils.readPublicKey(publicKeyPath);
        SignatureAlgorithm ecAlgo = randomFrom(SignatureAlgorithm.getEcAlgorithms());
        Tuple<PrivateKey, PublicKey> ecKeyPair = getEcKeyPairForAlgorithm(ecAlgo);

        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> {
            new HmacSignatureValidator(hmacAlgo, rsaPublicKey);
        });
        assertThat(e1.getMessage(), containsString("using a SecretKey but a [sun.security.rsa.RSAPublicKeyImpl] is provided"));

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> {
            new HmacSignatureValidator(rsaAlgo, hmacKey);
        });
        assertThat(e2.getMessage(), containsString("Unsupported algorithm RS"));

        IllegalArgumentException e3 = expectThrows(IllegalArgumentException.class, () -> {
            new RsaSignatureValidator(hmacAlgo, rsaPublicKey);
        });
        assertThat(e3.getMessage(), containsString("Unsupported algorithm HS"));

        IllegalArgumentException e4 = expectThrows(IllegalArgumentException.class, () -> {
            new RsaSignatureValidator(rsaAlgo, hmacKey);
        });
        assertThat(e4.getMessage(), containsString("using a PublicKey but a [javax.crypto.spec.SecretKeySpec] is provided"));

        IllegalArgumentException e5 = expectThrows(IllegalArgumentException.class, () -> {
            new EcSignatureValidator(ecAlgo, ecKeyPair.v1());
        });
        assertThat(e5.getMessage(), containsString("using an ECPublicKey but a [sun.security.ec.ECPrivateKeyImpl] is provided"));
    }

    private Tuple<PrivateKey, PublicKey> getEcKeyPairForAlgorithm(SignatureAlgorithm algorithm) throws Exception {
        String keyLength = algorithm.name().replace("ES", "");
        Path privateKeyPath = PathUtils.get(IdTokenParserTests.class.getResource
            ("/org/elasticsearch/xpack/security/authc/oidc/ec_private_key_" + keyLength + ".pem").toURI());
        Path publicKeyPath = PathUtils.get(IdTokenParserTests.class.getResource
            ("/org/elasticsearch/xpack/security/authc/oidc/ec_public_key_" + keyLength + ".pem").toURI());
        return new Tuple<>(PemUtils.readPrivateKey(privateKeyPath, () -> null), PemUtils.readPublicKey(publicKeyPath));
    }
}
