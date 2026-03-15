/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import static org.junit.Assert.assertTrue;

/**
 * JUnit rule that builds a trust store at runtime from a PEM certificate stream.
 * In FIPS mode the store type is BCFKS; otherwise JKS.
 * See <a href="https://github.com/elastic/elasticsearch/issues/111532">#111532</a>
 * for broader trust store infrastructure improvements.
 */
public class TestTrustStore extends ExternalResource {

    private static final boolean FIPS = ESTestCase.inFipsJvm();
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
        final var tmpTrustStorePath = tmpDir.resolve(FIPS ? "trust-store.bcfks" : "trust-store.jks");
        try (var pemStream = pemStreamSupplier.get(); var outStream = Files.newOutputStream(tmpTrustStorePath)) {
            final List<Certificate> certificates = CertificateFactory.getInstance("X.509")
                .generateCertificates(pemStream)
                .stream()
                .map(i -> (Certificate) i)
                .toList();
            final var trustStore = KeyStoreUtil.buildTrustStore(certificates, TRUST_STORE_TYPE);
            trustStore.store(outStream, TRUST_STORE_PASSWORD);
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
