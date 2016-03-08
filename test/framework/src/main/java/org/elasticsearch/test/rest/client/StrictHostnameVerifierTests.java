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
package org.elasticsearch.test.rest.client;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.security.auth.x500.X500Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link StrictHostnameVerifier} to validate that it can verify IPv6 addresses with and without bracket notation, in
 * addition to other address types.
 */
public class StrictHostnameVerifierTests extends ESTestCase {

    private static final int IP_SAN_TYPE = 7;
    private static final int DNS_SAN_TYPE = 2;

    private static final String[] CNS = new String[] { "my node" };
    private static final String[] IP_SANS = new String[] { "127.0.0.1", "192.168.1.1", "::1" };
    private static final String[] DNS_SANS = new String[] { "localhost", "computer", "localhost6" };

    private SSLSocket sslSocket;
    private SSLSession sslSession;
    private X509Certificate certificate;

    @Before
    public void setupMocks() throws Exception {
        sslSocket = mock(SSLSocket.class);
        sslSession = mock(SSLSession.class);
        certificate = mock(X509Certificate.class);
        Collection<List<?>> subjectAlternativeNames = new ArrayList<>();
        for (String san : IP_SANS) {
            subjectAlternativeNames.add(Arrays.asList(IP_SAN_TYPE, san));
        }
        for (String san : DNS_SANS) {
            subjectAlternativeNames.add(Arrays.asList(DNS_SAN_TYPE, san));
        }

        when(sslSocket.getSession()).thenReturn(sslSession);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[] { certificate });
        when(certificate.getSubjectX500Principal()).thenReturn(new X500Principal("CN=" + CNS[0]));
        when(certificate.getSubjectAlternativeNames()).thenReturn(subjectAlternativeNames);
    }

    public void testThatIPv6WithBracketsWorks() throws Exception {
        final String ipv6Host = "[::1]";

        // an exception will be thrown if verification fails
        StrictHostnameVerifier.INSTANCE.verify(ipv6Host, CNS, IP_SANS);
        StrictHostnameVerifier.INSTANCE.verify(ipv6Host, sslSocket);
        StrictHostnameVerifier.INSTANCE.verify(ipv6Host, certificate);

        // this is the only one we can assert on
        assertTrue(StrictHostnameVerifier.INSTANCE.verify(ipv6Host, sslSession));
    }

    public void testThatIPV6WithoutBracketWorks() throws Exception {
        final String ipv6Host = "::1";

        // an exception will be thrown if verification fails
        StrictHostnameVerifier.INSTANCE.verify(ipv6Host, CNS, IP_SANS);
        StrictHostnameVerifier.INSTANCE.verify(ipv6Host, sslSocket);
        StrictHostnameVerifier.INSTANCE.verify(ipv6Host, certificate);

        // this is the only one we can assert on
        assertTrue(StrictHostnameVerifier.INSTANCE.verify(ipv6Host, sslSession));
    }

    public void testThatIPV4Works() throws Exception {
        final String ipv4Host = randomFrom("127.0.0.1", "192.168.1.1");

        // an exception will be thrown if verification fails
        StrictHostnameVerifier.INSTANCE.verify(ipv4Host, CNS, IP_SANS);
        StrictHostnameVerifier.INSTANCE.verify(ipv4Host, sslSocket);
        StrictHostnameVerifier.INSTANCE.verify(ipv4Host, certificate);

        // this is the only one we can assert on
        assertTrue(StrictHostnameVerifier.INSTANCE.verify(ipv4Host, sslSession));
    }

    public void testThatHostnameWorks() throws Exception {
        final String host = randomFrom(DNS_SANS);

        // an exception will be thrown if verification fails
        StrictHostnameVerifier.INSTANCE.verify(host, CNS, DNS_SANS);
        StrictHostnameVerifier.INSTANCE.verify(host, sslSocket);
        StrictHostnameVerifier.INSTANCE.verify(host, certificate);

        // this is the only one we can assert on
        assertTrue(StrictHostnameVerifier.INSTANCE.verify(host, sslSession));
    }
}
