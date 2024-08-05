/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.tests.util.LuceneTestCase.createTempDir;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestTrustStore extends ExternalResource {

    private final CheckedSupplier<InputStream, IOException> pemStreamSupplier;

    public TestTrustStore(CheckedSupplier<InputStream, IOException> pemStreamSupplier) {
        this.pemStreamSupplier = pemStreamSupplier;
    }

    private Path trustStorePath;

    public Path getTrustStorePath() {
        assertFalse("Tests in FIPS mode cannot supply a custom trust store", ESTestCase.inFipsJvm());
        return Objects.requireNonNullElseGet(trustStorePath, () -> ESTestCase.fail(null, "trust store not created"));
    }

    @Override
    protected void before() {
        final var tmpDir = createTempDir();
        final var tmpTrustStorePath = tmpDir.resolve("trust-store.jks");
        try (var pemStream = pemStreamSupplier.get(); var jksStream = Files.newOutputStream(tmpTrustStorePath)) {
            final List<Certificate> certificates = CertificateFactory.getInstance("X.509")
                .generateCertificates(pemStream)
                .stream()
                .map(i -> (Certificate) i)
                .toList();
            final var trustStore = KeyStoreUtil.buildTrustStore(certificates, "jks");
            trustStore.store(jksStream, new char[0]);
            trustStorePath = tmpTrustStorePath;
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }

    @Override
    protected void after() {
        assertTrue(trustStorePath + " should still exist at teardown", Files.exists(trustStorePath));
    }
}
