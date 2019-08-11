/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPrivateKey;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CertParsingUtilsTests extends ESTestCase {
    public void testReadKeysCorrectly() throws Exception {
        // read in keystore version
        Path keystorePath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        Key key;
        try (InputStream in = Files.newInputStream(keystorePath)) {
            KeyStore keyStore = KeyStore.getInstance("jks");
            keyStore.load(in, "testnode".toCharArray());
            key = keyStore.getKey("testnode_RSA", "testnode".toCharArray());
        }
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));

        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"), "testnode"::toCharArray);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadCertsCorrectly() throws Exception {
        // read in keystore version
        Path keystorePath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        Certificate certificate;
        try (InputStream in = Files.newInputStream(keystorePath)) {
            KeyStore keyStore = KeyStore.getInstance("jks");
            keyStore.load(in, "testnode".toCharArray());
            certificate = keyStore.getCertificate("testnode_rsa");
        }
        assertThat(certificate, notNullValue());
        assertThat(certificate, instanceOf(X509Certificate.class));

        Certificate pemCert;
        try (InputStream input =
                     Files.newInputStream(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))) {
            List<Certificate> certificateList = CertParsingUtils.readCertificates(input);
            assertThat(certificateList.size(), is(1));
            pemCert = certificateList.get(0);
        }
        assertThat(pemCert, notNullValue());
        assertThat(pemCert, equalTo(certificate));
    }

    public void testReadEllipticCurveCertificateAndKey() throws Exception {
        Path keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/prime256v1-key.pem");
        verifyPrime256v1ECKey(keyPath);

        Path keyPkcs8Path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/prime256v1-key-noparam-pkcs8.pem");
        verifyPrime256v1ECKey(keyPkcs8Path);

        Path keyNoSpecPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/prime256v1-key-noparam.pem");
        verifyPrime256v1ECKey(keyNoSpecPath);

        Path certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/prime256v1-cert.pem");
        Certificate[] certs = CertParsingUtils.readCertificates(Collections.singletonList(certPath.toString()), newEnvironment());
        assertEquals(1, certs.length);
        Certificate cert = certs[0];
        assertNotNull(cert);
        assertEquals("EC", cert.getPublicKey().getAlgorithm());
    }

    private void verifyPrime256v1ECKey(Path keyPath) throws IOException {
        PrivateKey privateKey = PemUtils.readPrivateKey(keyPath, () -> null);
        assertEquals("EC", privateKey.getAlgorithm());
        assertThat(privateKey, instanceOf(ECPrivateKey.class));
    }
}
