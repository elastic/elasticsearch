/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.AbstractRequestTestCase;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.security.auth.x500.X500Principal;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DelegatePkiAuthenticationRequestTests extends AbstractRequestTestCase<DelegatePkiAuthenticationRequest,
        org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationRequest> {

    public void testEmptyOrNullCertificateChain() throws Exception {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            new DelegatePkiAuthenticationRequest((List<X509Certificate>)null);
        });
        assertThat(e.getMessage(), is("certificate chain must not be empty or null"));
        e = expectThrows(IllegalArgumentException.class, () -> {
            new DelegatePkiAuthenticationRequest(Collections.emptyList());
        });
        assertThat(e.getMessage(), is("certificate chain must not be empty or null"));
    }

    public void testUnorderedCertificateChain() throws Exception {
        List<X509Certificate> mockCertChain = new ArrayList<>(2);
        mockCertChain.add(mock(X509Certificate.class));
        when(mockCertChain.get(0).getIssuerX500Principal()).thenReturn(new X500Principal("CN=Test, OU=elasticsearch, O=org"));
        mockCertChain.add(mock(X509Certificate.class));
        when(mockCertChain.get(1).getSubjectX500Principal()).thenReturn(new X500Principal("CN=Not Test, OU=elasticsearch, O=org"));
        DelegatePkiAuthenticationRequest request = new DelegatePkiAuthenticationRequest(mockCertChain);
        Optional<ValidationException> ve = request.validate();
        assertThat(ve.isPresent(), is(true));
        assertThat(ve.get().validationErrors().size(), is(1));
        assertThat(ve.get().validationErrors().get(0), is("certificates chain must be an ordered chain"));
    }

    @Override
    protected DelegatePkiAuthenticationRequest createClientTestInstance() {
        List<X509Certificate> certificates = randomCertificateList();
        return new DelegatePkiAuthenticationRequest(certificates);
    }

    @Override
    protected org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationRequest doParseToServerInstance(XContentParser parser)
            throws IOException {
        return org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationRequest.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationRequest serverInstance,
            DelegatePkiAuthenticationRequest clientTestInstance) {
        assertThat(serverInstance.getCertificateChain(), is(clientTestInstance.getCertificateChain()));
    }

    private List<X509Certificate> randomCertificateList() {
        List<X509Certificate> certificates = Arrays.asList(randomArray(1, 3, X509Certificate[]::new, () -> {
            try {
                return readCertForPkiDelegation(randomFrom("testClient.crt", "testIntermediateCA.crt", "testRootCA.crt"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
        return certificates;
    }

    private X509Certificate readCertForPkiDelegation(String certificateName) throws Exception {
        Path path = getDataPath("/org/elasticsearch/client/security/delegate_pki/" + certificateName);
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }
}
