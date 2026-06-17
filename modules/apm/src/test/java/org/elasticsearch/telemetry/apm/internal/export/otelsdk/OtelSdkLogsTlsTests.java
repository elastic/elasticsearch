/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import javax.net.ssl.X509TrustManager;

import static org.hamcrest.Matchers.hasItem;

/**
 * Unit tests for {@link OtelSdkLogsTls}.
 * PEM resources (ca.crt, cert1.crt, cert1.key) are copied from the ssl-config library's test set
 * ({@code libs/ssl-config/src/test/resources/certs/}); the key is unencrypted.
 */
public class OtelSdkLogsTlsTests extends ESTestCase {

    /** No SSL settings → factory returns null; exporter uses its own defaults. */
    public void testNoSettingsReturnsNull() {
        OtelSdkLogsTls result = OtelSdkLogsTls.fromSettings(Settings.EMPTY, createTempDir());
        assertNull("no SSL settings must produce null", result);
    }

    /** certificate set without key → IllegalArgumentException. */
    public void testCertificateWithoutKeyThrows() {
        Path ca = getDataPath("tls/ca.crt");
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE.getKey(), getDataPath("tls/cert1.crt").toString())
            .build();
        expectThrows(IllegalArgumentException.class, () -> OtelSdkLogsTls.fromSettings(settings, ca.getParent()));
    }

    /** key set without certificate → IllegalArgumentException. */
    public void testKeyWithoutCertificateThrows() {
        Path ca = getDataPath("tls/ca.crt");
        Settings settings = Settings.builder()
            .put(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_KEY.getKey(), getDataPath("tls/cert1.key").toString())
            .build();
        expectThrows(IllegalArgumentException.class, () -> OtelSdkLogsTls.fromSettings(settings, ca.getParent()));
    }

    /** CA only → trust manager accepts certs issued by the configured CA. */
    public void testCaOnlyProducesValidContext() throws Exception {
        Path caPath = getDataPath("tls/ca.crt");
        Settings settings = Settings.builder()
            .putList(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE_AUTHORITIES.getKey(), caPath.toString())
            .build();
        OtelSdkLogsTls tls = OtelSdkLogsTls.fromSettings(settings, caPath.getParent());

        assertThat(acceptedIssuerSubjects(tls.trustManager()), hasItem(subjectOf(caPath)));
    }

    /** Full mTLS settings → trust manager trusts exactly the configured CA; key manager is present. */
    public void testFullMtlsSettings() throws Exception {
        Path caPath = getDataPath("tls/ca.crt");
        Settings settings = Settings.builder()
            .putList(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE_AUTHORITIES.getKey(), caPath.toString())
            .put(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE.getKey(), getDataPath("tls/cert1.crt").toString())
            .put(OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_KEY.getKey(), getDataPath("tls/cert1.key").toString())
            .build();
        OtelSdkLogsTls tls = OtelSdkLogsTls.fromSettings(settings, caPath.getParent());

        assertThat(acceptedIssuerSubjects(tls.trustManager()), hasItem(subjectOf(caPath)));
        assertThat(tls.sslContext().getProtocol(), equalTo("TLS"));
    }

    private static String subjectOf(Path pemPath) throws Exception {
        try (InputStream in = Files.newInputStream(pemPath)) {
            return ((X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(in)).getSubjectX500Principal().toString();
        }
    }

    private static Set<String> acceptedIssuerSubjects(X509TrustManager tm) {
        return Arrays.stream(tm.getAcceptedIssuers())
            .map(X509Certificate::getSubjectX500Principal)
            .map(Object::toString)
            .collect(Collectors.toSet());
    }
}
