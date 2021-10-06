/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl.cert;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;


import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class CertificateInfoTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final X509Certificate certificate = readSampleCertificate();
        final CertificateInfo cert1 = new CertificateInfo("/path/to/cert.jks", "jks", "key", true, certificate);
        final CertificateInfo cert2 = serializeAndDeserialize(cert1);
        final CertificateInfo cert3 = serializeAndDeserialize(cert2);
        assertThat(cert1, equalTo(cert2));
        assertThat(cert1, equalTo(cert3));
        assertThat(cert2, equalTo(cert3));
    }

    public void testCompareTo() throws Exception {
        final X509Certificate certificate = readSampleCertificate();
        CertificateInfo pkcs11 = new CertificateInfo(null, "PKCS11", "alias1", true, certificate);
        CertificateInfo pkcs12 = new CertificateInfo("http.p12", "PKCS12", "http", true, certificate);
        CertificateInfo pem1 = new CertificateInfo("cert.crt", "PEM", null, true, certificate);
        CertificateInfo pem2 = new CertificateInfo("ca.crt", "PEM", null, false, certificate);
        CertificateInfo jks1 = new CertificateInfo("keystore.jks", "jks", "instance", true, certificate);
        CertificateInfo jks2 = new CertificateInfo("keystore.jks", "jks", "ca", false, certificate);

        List<CertificateInfo> list = Arrays.asList(pkcs11, pkcs12, pem1, pem2, jks1, jks2);
        Collections.shuffle(list, random());
        Collections.sort(list);

        assertThat(list, contains(pem2, pem1, pkcs12, jks2, jks1, pkcs11));
    }

    private X509Certificate readSampleCertificate() throws CertificateException, IOException {
        return CertParsingUtils.
            readX509Certificates(Collections.singletonList(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")))[0];
    }

    private CertificateInfo serializeAndDeserialize(CertificateInfo cert1) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        cert1.writeTo(output);
        return new CertificateInfo(output.bytes().streamInput());
    }
}
