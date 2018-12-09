/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl.cert;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;


import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class CertificateInfoTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final X509Certificate certificate = CertParsingUtils.
                readX509Certificates(Collections.singletonList(getDataPath
                        ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")))[0];
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
}