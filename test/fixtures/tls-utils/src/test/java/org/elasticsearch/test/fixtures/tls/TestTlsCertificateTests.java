/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.tls;

import org.bouncycastle.asn1.x509.GeneralName;
import org.elasticsearch.test.ESTestCase;

import java.security.KeyFactory;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.RSAPublicKeySpec;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.security.auth.x500.X500Principal;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class TestTlsCertificateTests extends ESTestCase {

    public void testGenerateReturnsValidSelfSignedCertificate() throws Exception {
        final var hostName0 = randomIdentifier("host-0-");
        final var hostName1 = randomIdentifier("host-1-");
        final TestTlsCertificate testTlsCertificate = TestTlsCertificate.generate(hostName0, hostName1);
        assertThat(testTlsCertificate, notNullValue());

        final X509Certificate certificate = testTlsCertificate.certificate();
        assertThat(certificate, notNullValue());
        assertThat(certificate.getSubjectX500Principal(), equalTo(new X500Principal("CN=" + hostName0)));
        assertThat(certificate.getNotBefore().toInstant(), lessThanOrEqualTo(Instant.now()));
        assertThat(certificate.getNotAfter().toInstant(), greaterThan(Instant.now()));

        final Collection<List<?>> subjectAlternativeNames = certificate.getSubjectAlternativeNames();
        assertThat(subjectAlternativeNames, notNullValue());
        assertThat(subjectAlternativeNames, hasSize(2));
        final List<String> dnsNames = new ArrayList<>(2);
        for (List<?> subjectAlternativeName : subjectAlternativeNames) {
            assertThat(subjectAlternativeName, hasSize(2));
            assertThat(subjectAlternativeName.get(0), equalTo(GeneralName.dNSName));
            dnsNames.add(asInstanceOf(String.class, subjectAlternativeName.get(1)));
        }
        assertThat(dnsNames, containsInAnyOrder(hostName0, hostName1));

        final RSAPrivateCrtKey privateKey = asInstanceOf(RSAPrivateCrtKey.class, testTlsCertificate.privateKey());
        final var publicKey = KeyFactory.getInstance("RSA")
            .generatePublic(new RSAPublicKeySpec(privateKey.getModulus(), privateKey.getPublicExponent()));
        certificate.verify(publicKey);

        try (var pemStream = testTlsCertificate.getPemCertificateStream()) {
            final Collection<?> parsedCertificates = CertificateFactory.getInstance("X.509").generateCertificates(pemStream);
            assertThat(parsedCertificates, hasSize(1));
            assertThat(parsedCertificates.iterator().next(), equalTo(certificate));
        }
    }
}
