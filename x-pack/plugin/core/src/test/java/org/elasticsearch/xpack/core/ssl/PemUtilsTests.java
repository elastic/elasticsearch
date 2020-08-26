/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.test.ESTestCase;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AlgorithmParameters;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.StringContains.containsString;

public class PemUtilsTests extends ESTestCase {

    public void testReadPKCS8RsaKey() throws Exception {
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/rsa_key_pkcs8_plain.pem"), ""::toCharArray);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadPKCS8RsaKeyWithBagAttrs() throws Exception {
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_with_bagattrs.pem"), ""::toCharArray);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadPKCS8DsaKey() throws Exception {
        Key key = getKeyFromKeystore("DSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/dsa_key_pkcs8_plain.pem"), ""::toCharArray);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadPKCS8EcKey() throws Exception {
        Key key = getKeyFromKeystore("EC");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
            ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/ec_key_pkcs8_plain.pem"), ""::toCharArray);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEcKeyCurves() throws Exception {
        String curve = randomFrom("secp256r1", "secp384r1", "secp521r1");
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
            ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/private_" + curve + ".pem"), ""::toCharArray);
        assertThat(privateKey, instanceOf(ECPrivateKey.class));
        ECParameterSpec parameterSpec = ((ECPrivateKey) privateKey).getParams();
        ECGenParameterSpec algorithmParameterSpec = new ECGenParameterSpec(curve);
        AlgorithmParameters algoParameters = AlgorithmParameters.getInstance("EC");
        algoParameters.init(algorithmParameterSpec);
        assertThat(parameterSpec, equalTo(algoParameters.getParameterSpec(ECParameterSpec.class)));
    }

    public void testReadEncryptedPKCS8Key() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBE KeySpec is not available", inFipsJvm());
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
            ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/key_pkcs8_encrypted.pem"), "testnode"::toCharArray);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadDESEncryptedPKCS1Key() throws Exception {
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"), "testnode"::toCharArray);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadAESEncryptedPKCS1Key() throws Exception {
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        String bits = randomFrom("128", "192", "256");
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                        ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-aes" + bits + ".pem"),
                "testnode"::toCharArray);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadPKCS1RsaKey() throws Exception {
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                        ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-unprotected.pem"),
                "testnode"::toCharArray);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslDsaKey() throws Exception {
        Key key = getKeyFromKeystore("DSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/dsa_key_openssl_plain.pem"),
            ""::toCharArray);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslDsaKeyWithParams() throws Exception {
        Key key = getKeyFromKeystore("DSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/dsa_key_openssl_plain_with_params.pem"),
            ""::toCharArray);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedOpenSslDsaKey() throws Exception {
        Key key = getKeyFromKeystore("DSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/dsa_key_openssl_encrypted.pem"),
            "testnode"::toCharArray);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslEcKey() throws Exception {
        Key key = getKeyFromKeystore("EC");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/ec_key_openssl_plain.pem"),
            ""::toCharArray);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslEcKeyWithParams() throws Exception {
        Key key = getKeyFromKeystore("EC");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/ec_key_openssl_plain_with_params.pem"),
            ""::toCharArray);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedOpenSslEcKey() throws Exception {
        Key key = getKeyFromKeystore("EC");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/ec_key_openssl_encrypted.pem"),
            "testnode"::toCharArray);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadUnsupportedKey() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> PemUtils.readPrivateKey(getDataPath
                        ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/key_unsupported.pem"),
                "testnode"::toCharArray));
        assertThat(e.getMessage(), containsString("File did not contain a supported key format"));
    }

    public void testReadUnsupportedPemFile() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> PemUtils.readPrivateKey(getDataPath
                        ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"),
                "testnode"::toCharArray));
        assertThat(e.getMessage(), containsString("File did not contain a supported key format"));
    }

    public void testReadCorruptedKey() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> PemUtils.readPrivateKey(getDataPath
                        ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/corrupted_key_pkcs8_plain.pem"),
                "testnode"::toCharArray));
        assertThat(e.getMessage(), containsString("Error parsing Private Key from"));
        assertThat(e.getCause().getMessage(), containsString("Malformed PEM file, PEM footer is invalid or missing"));
    }

    public void testReadEmptyFile() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> PemUtils.readPrivateKey(getDataPath
                        ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/empty.pem"),
                "testnode"::toCharArray));
        assertThat(e.getMessage(), containsString("File is empty"));
    }

    private Key getKeyFromKeystore(String algo) throws Exception {
        Path keystorePath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        try (InputStream in = Files.newInputStream(keystorePath)) {
            KeyStore keyStore = KeyStore.getInstance("jks");
            keyStore.load(in, "testnode".toCharArray());
            return keyStore.getKey("testnode_" + algo, "testnode".toCharArray());
        }
    }
}
