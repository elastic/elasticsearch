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
import org.elasticsearch.xpack.security.authc.oidc.IdTokenParserTests;

import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;

import static org.hamcrest.Matchers.containsString;

public class JwtSignerTests extends ESTestCase {

    public void testOnlyAcceptCorrectKeyAndAlgorithm() throws Exception {
        SignatureAlgorithm hmacAlgo = randomFrom(SignatureAlgorithm.getHmacAlgorithms());
        final SecretKeySpec hmacKey =
            new SecretKeySpec("144753a689a6508d7c7cd02752d7138e".getBytes(StandardCharsets.UTF_8.name()), hmacAlgo.getJcaAlgoName());
        SignatureAlgorithm rsaAlgo = randomFrom(SignatureAlgorithm.getRsaAlgorithms());
        Path privateKeyPath = PathUtils.get(IdTokenParserTests.class.getResource
            ("/org/elasticsearch/xpack/security/authc/oidc/rsa_private_key.pem").toURI());
        final PrivateKey rsaPrivateKey = PemUtils.readPrivateKey(privateKeyPath, () -> null);
        SignatureAlgorithm ecAlgo = randomFrom(SignatureAlgorithm.getEcAlgorithms());
        Tuple<PrivateKey, PublicKey> ecKeyPair = getEcKeyPairForAlgorithm(ecAlgo);

        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> {
            new HmacSigner(hmacAlgo, rsaPrivateKey);
        });
        assertThat(e1.getMessage(), containsString("using a SecretKey but a [sun.security.rsa.RSAPrivateCrtKeyImpl] is provided"));

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> {
            new HmacSigner(rsaAlgo, hmacKey);
        });
        assertThat(e2.getMessage(), containsString("Unsupported algorithm RS"));

        IllegalArgumentException e3 = expectThrows(IllegalArgumentException.class, () -> {
            new RsaSigner(hmacAlgo, rsaPrivateKey);
        });
        assertThat(e3.getMessage(), containsString("Unsupported algorithm HS"));

        IllegalArgumentException e4 = expectThrows(IllegalArgumentException.class, () -> {
            new RsaSigner(rsaAlgo, hmacKey);
        });
        assertThat(e4.getMessage(), containsString("using a PrivateKey but a [javax.crypto.spec.SecretKeySpec] is provided"));

        IllegalArgumentException e5 = expectThrows(IllegalArgumentException.class, () -> {
            new EcSigner(ecAlgo, ecKeyPair.v2());
        });
        assertThat(e5.getMessage(), containsString("using a ECPrivateKey but a [sun.security.ec.ECPublicKeyImpl] is provided"));
    }

    public void testSignAndValidateHmacSignatures() throws Exception {
        SignatureAlgorithm algorithm = randomFrom(SignatureAlgorithm.getHmacAlgorithms());
        JsonWebToken token = new JsonWebTokenBuilder()
            .algorithm(algorithm.name())
            .type("JWT")
            .subject("subject")
            .issuer("theissuer")
            .build();
        final SecretKeySpec key =
            new SecretKeySpec("144753a689a6508d7c7cd02752d7138e".getBytes(StandardCharsets.UTF_8.name()), algorithm.getJcaAlgoName());
        JwtSigner signer = new HmacSigner(algorithm, key);
        JwtSignatureValidator validator = new HmacSignatureValidator(algorithm, key);
        validator.validateSignature(token.encodeSignableContent(), signer.sign(token.encodeSignableContent()));
    }

    public void testSignAndValidateRsaSignatures() throws Exception {
        Path publicKeyPath = PathUtils.get(IdTokenParserTests.class.getResource
            ("/org/elasticsearch/xpack/security/authc/oidc/rsa_public_key.pem").toURI());
        final PublicKey publicKey = PemUtils.readPublicKey(publicKeyPath);
        Path privateKeyPath = PathUtils.get(IdTokenParserTests.class.getResource
            ("/org/elasticsearch/xpack/security/authc/oidc/rsa_private_key.pem").toURI());
        final PrivateKey privateKey = PemUtils.readPrivateKey(privateKeyPath, () -> null);
        SignatureAlgorithm algorithm = randomFrom(SignatureAlgorithm.getRsaAlgorithms());
        JsonWebToken token = new JsonWebTokenBuilder()
            .algorithm(algorithm.name())
            .type("JWT")
            .subject("subject")
            .issuer("theissuer")
            .build();
        JwtSigner signer = new RsaSigner(algorithm, privateKey);
        JwtSignatureValidator validator = new RsaSignatureValidator(algorithm, publicKey);
        validator.validateSignature(token.encodeSignableContent(), signer.sign(token.encodeSignableContent()));
    }

    public void testSignAndValidateEcSignatures() throws Exception {
        SignatureAlgorithm algorithm = randomFrom(SignatureAlgorithm.getEcAlgorithms());
        JsonWebToken token = new JsonWebTokenBuilder()
            .algorithm(algorithm.name())
            .type("JWT")
            .subject("subject")
            .issuer("theissuer")
            .build();
        Tuple<PrivateKey, PublicKey> keyPair = getEcKeyPairForAlgorithm(algorithm);
        JwtSigner signer = new EcSigner(algorithm, keyPair.v1());
        JwtSignatureValidator validator = new EcSignatureValidator(algorithm, keyPair.v2());
        validator.validateSignature(token.encodeSignableContent(), signer.sign(token.encodeSignableContent()));
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
