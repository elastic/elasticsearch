/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationRequest;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import javax.security.auth.x500.X500Principal;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.arrayContaining;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DelegatePkiAuthenticationRequestTests extends ESTestCase {

    public void testRequestValidation() {
        DelegatePkiAuthenticationRequest request = new DelegatePkiAuthenticationRequest(((X509Certificate[]) null));
        ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0), is("certificates chain array must not be null"));

        request = new DelegatePkiAuthenticationRequest(new X509Certificate[0]);
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0), is("certificates chain array must not be empty"));

        X509Certificate[] mockCertChain = new X509Certificate[2];
        mockCertChain[0] = mock(X509Certificate.class);
        when(mockCertChain[0].getIssuerX500Principal()).thenReturn(new X500Principal("CN=Test, OU=elasticsearch, O=org"));
        mockCertChain[1] = mock(X509Certificate.class);
        when(mockCertChain[1].getSubjectX500Principal()).thenReturn(new X500Principal("CN=Not Test, OU=elasticsearch, O=org"));
        request = new DelegatePkiAuthenticationRequest(mockCertChain);
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0), is("certificates chain array is not ordered"));

        request = new DelegatePkiAuthenticationRequest(randomArray(1, 3, X509Certificate[]::new, () -> {
            X509Certificate mockX509Certificate = mock(X509Certificate.class);
            when(mockX509Certificate.getSubjectX500Principal()).thenReturn(new X500Principal("CN=Test, OU=elasticsearch, O=org"));
            when(mockX509Certificate.getIssuerX500Principal()).thenReturn(new X500Principal("CN=Test, OU=elasticsearch, O=org"));
            return mockX509Certificate;
        }));
        ve = request.validate();
        assertNull(ve);
    }

    public void testSerialization() throws Exception {
        X509Certificate[] certificates = randomArray(1, 3, X509Certificate[]::new, () -> {
            try {
                return readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/"
                        + randomFrom("testclient.crt", "testnode.crt", "testnode-ip-only.crt", "openldap.crt", "samba4.crt")));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        DelegatePkiAuthenticationRequest request = new DelegatePkiAuthenticationRequest(certificates);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final DelegatePkiAuthenticationRequest serialized = new DelegatePkiAuthenticationRequest(in);
                assertThat(request.getCertificates(), arrayContaining(certificates));
                assertThat(request, is(serialized));
                assertThat(request.hashCode(), is(serialized.hashCode()));
            }
        }
    }

    static X509Certificate readCert(Path path) throws Exception {
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }
}
