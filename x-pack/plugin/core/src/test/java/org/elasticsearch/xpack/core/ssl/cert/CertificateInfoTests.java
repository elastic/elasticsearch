/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl.cert;

import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.CertUtils;

import javax.security.auth.x500.X500Principal;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

import static org.hamcrest.Matchers.equalTo;

public class CertificateInfoTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final X500Principal principal = new X500Principal("CN=foo");
        final X509Certificate certificate = CertUtils.generateSignedCertificate(principal, new GeneralNames(new GeneralName[0]),
                getKeyPair(), null, null, 90);
        final CertificateInfo cert1 = new CertificateInfo("/path/to/cert.jks", "jks", "key", true, certificate);
        final CertificateInfo cert2 = serializeAndDeserialize(cert1);
        final CertificateInfo cert3 = serializeAndDeserialize(cert2);
        assertThat(cert1, equalTo(cert2));
        assertThat(cert1, equalTo(cert3));
        assertThat(cert2, equalTo(cert3));
    }

    private CertificateInfo serializeAndDeserialize(CertificateInfo cert1) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        cert1.writeTo(output);
        return new CertificateInfo(output.bytes().streamInput());
    }

    private KeyPair getKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        return keyPairGenerator.generateKeyPair();
    }

}