/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl.cert;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class CertificateInfoTests extends ESTestCase {

    private static final String selfSignedCertPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt";
    private static final String rootSignedCertPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/openldap.crt";

    public void testSerialization() throws Exception {
        final X509Certificate certificate = readSampleCertificate(selfSignedCertPath);
        final CertificateInfo cert1 = new CertificateInfo("/path/to/cert.jks", "jks", "key", true, certificate);
        final CertificateInfo cert2 = serializeAndDeserialize(cert1);
        final CertificateInfo cert3 = serializeAndDeserialize(cert2);
        assertThat(cert1, equalTo(cert2));
        assertThat(cert1, equalTo(cert3));
        assertThat(cert2, equalTo(cert3));
    }

    public void testCompareTo() throws Exception {
        final X509Certificate certificate = readSampleCertificate(selfSignedCertPath);
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

    public void testExtractIssuer() throws Exception {
        // self signed
        X509Certificate certificate = readSampleCertificate(selfSignedCertPath);
        CertificateInfo certificateInfo = new CertificateInfo(
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomBoolean(),
            certificate
        );
        assertEquals(certificate.getSubjectX500Principal().toString(), certificateInfo.issuer());
        assertEquals(certificate.getIssuerX500Principal().toString(), certificateInfo.issuer());

        // root signed
        certificate = readSampleCertificate(rootSignedCertPath);
        certificateInfo = new CertificateInfo(
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomBoolean(),
            certificate
        );
        assertNotEquals(certificate.getSubjectX500Principal().toString(), certificateInfo.issuer());
        assertEquals("CN=root-ca, OU=test, O=elasticsearch, C=US", certificateInfo.issuer());
    }

    public void testMissingIssuer() throws Exception {
        // only possible in mixed versions if object is serialized from an old version
        final CertificateInfo certInfo = new CertificateInfo("/path/to/cert", "jks", "a", true, readSampleCertificate(selfSignedCertPath));
        // send from old
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(Version.V_8_3_0);
        certInfo.writeTo(out);
        // receive from old
        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        StreamInput in = new InputStreamStreamInput(inBuffer);
        in.setVersion(Version.V_8_3_0);
        CertificateInfo certInfoFromOld = new CertificateInfo(in);
        // convert to a JSON string
        String toXContentString = Strings.toString(certInfoFromOld.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        assertThat(toXContentString, not(containsString("issuer")));
    }

    private X509Certificate readSampleCertificate(String dataPath) throws CertificateException, IOException {
        return CertParsingUtils.readX509Certificates(Collections.singletonList(getDataPath(dataPath)))[0];
    }

    private CertificateInfo serializeAndDeserialize(CertificateInfo cert1) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        cert1.writeTo(output);
        return new CertificateInfo(output.bytes().streamInput());
    }
}
