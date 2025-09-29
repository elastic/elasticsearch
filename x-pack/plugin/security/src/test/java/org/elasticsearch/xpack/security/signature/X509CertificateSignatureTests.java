/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.signature;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.transport.X509CertificateSignature;
import org.hamcrest.Matchers;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;

public class X509CertificateSignatureTests extends ESTestCase {

    public void testEncodeDecode() throws Exception {
        final List<Certificate> certificates = PemUtils.readCertificates(
            List.of(getResourceDataPath(getClass(), "signing_rsa.crt"), getResourceDataPath(getClass(), "signing_ec.crt"))
        );
        assertThat(certificates, hasSize(2));
        final BytesReference bytes = randomBytesReference(randomIntBetween(8, 50));
        final X509CertificateSignature original = new X509CertificateSignature(
            certificates.stream().map(cert -> (X509Certificate) cert).toArray(X509Certificate[]::new),
            "SHA256withRSA",
            bytes
        );

        final String encoded = original.encodeToString();
        final X509CertificateSignature decoded = X509CertificateSignature.decode(encoded);
        assertThat(decoded.toString(), Matchers.equalTo(original.toString()));
        assertThat(decoded, Matchers.equalTo(original));
    }

}
