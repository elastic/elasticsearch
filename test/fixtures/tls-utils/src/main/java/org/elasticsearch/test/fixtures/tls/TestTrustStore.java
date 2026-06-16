/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.tls;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.tests.util.LuceneTestCase.createTempDir;
import static org.elasticsearch.test.ESTestCase.inFipsJvm;
import static org.junit.Assert.assertTrue;

public class TestTrustStore extends ExternalResource {

    private static final boolean FIPS = inFipsJvm();
    private static final String TRUST_STORE_TYPE = FIPS ? "bcfks" : "jks";
    private static final char[] TRUST_STORE_PASSWORD = FIPS ? "password".toCharArray() : new char[0];

    private final CheckedSupplier<InputStream, IOException> pemStreamSupplier;

    public TestTrustStore(CheckedSupplier<InputStream, IOException> pemStreamSupplier) {
        this.pemStreamSupplier = pemStreamSupplier;
    }

    private Path trustStorePath;

    public Path getTrustStorePath() {
        return Objects.requireNonNullElseGet(trustStorePath, () -> ESTestCase.fail(null, "trust store not created"));
    }

    public String getTrustStoreType() {
        return TRUST_STORE_TYPE;
    }

    @Override
    protected void before() {
        final var tmpDir = createTempDir();
        final var tmpTrustStorePath = tmpDir.resolve("trust-store.jks");
        try (var pemStream = pemStreamSupplier.get(); var outStream = Files.newOutputStream(tmpTrustStorePath)) {
            final List<Certificate> certificates = CertificateFactory.getInstance("X.509")
                .generateCertificates(pemStream)
                .stream()
                .map(i -> (Certificate) i)
                .toList();
            final var trustStore = buildTrustStore(certificates);
            trustStore.store(outStream, TRUST_STORE_PASSWORD);
            trustStorePath = tmpTrustStorePath;
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }

    private static KeyStore buildTrustStore(List<Certificate> certificates) throws GeneralSecurityException, IOException {
        final KeyStore trustStore = FIPS ? KeyStore.getInstance(TRUST_STORE_TYPE, "BCFIPS") : KeyStore.getInstance(TRUST_STORE_TYPE);
        trustStore.load(null, null);
        int counter = 0;
        for (Certificate certificate : certificates) {
            trustStore.setCertificateEntry("cert-" + counter, certificate);
            counter++;
        }
        return trustStore;
    }

    @Override
    protected void after() {
        assertTrue(trustStorePath + " should still exist at teardown", Files.exists(trustStorePath));
    }

    public void apply(LocalClusterSpecBuilder<?> builder, boolean enabled) {
        if (enabled == false) {
            return;
        }
        if (inFipsJvm()) {
            // In FIPS mode, FipsEnabledClusterConfigProvider sets javax.net.ssl.trustStore to ${ES_PATH_CONF}/cacerts.bcfks
            // Replace the cacerts.bcfks config file content with a combined store that includes both the FIPS CAs and the fixture's cert.
            builder.configFile("cacerts.bcfks", Resource.fromFile(this::getTrustStorePath));
        } else {
            builder.systemProperty("javax.net.ssl.trustStore", () -> this.getTrustStorePath().toString())
                .systemProperty("javax.net.ssl.trustStoreType", this::getTrustStoreType);
        }
    }
}
