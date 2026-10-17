/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.cli.bc.CertGenUtils;
import org.elasticsearch.xpack.security.cli.bc.KeyStoreFormat;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import javax.security.auth.x500.X500Principal;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class KeyStoreFormatTests extends ESTestCase {

    public void testFormatProperties() {
        assertThat(KeyStoreFormat.PKCS12.type(), equalTo("PKCS12"));
        assertThat(KeyStoreFormat.PKCS12.description(), equalTo("PKCS#12"));
        assertThat(KeyStoreFormat.PKCS12.extension(), equalTo(".p12"));
        assertThat(KeyStoreFormat.BCFKS.type(), equalTo("BCFKS"));
        assertThat(KeyStoreFormat.BCFKS.description(), equalTo("BCFKS"));
        assertThat(KeyStoreFormat.BCFKS.extension(), equalTo(".bcfks"));
    }

    public void testForFileName() {
        assertThat(KeyStoreFormat.forFileName("ca.bcfks"), sameInstance(KeyStoreFormat.BCFKS));
        assertThat(KeyStoreFormat.forFileName("CA.BCFKS"), sameInstance(KeyStoreFormat.BCFKS));
        assertThat(KeyStoreFormat.forFileName("ca.p12"), sameInstance(KeyStoreFormat.PKCS12));
        assertThat(KeyStoreFormat.forFileName("ca.pfx"), sameInstance(KeyStoreFormat.PKCS12));
        assertThat(KeyStoreFormat.forFileName("ca"), sameInstance(KeyStoreFormat.PKCS12));
        assertThat(KeyStoreFormat.forFileName(null), sameInstance(KeyStoreFormat.PKCS12));
    }

    public void testNewKeyStoreHasRequestedType() throws Exception {
        for (KeyStoreFormat format : KeyStoreFormat.values()) {
            final KeyStore keyStore = format.newKeyStore();
            assertThat(format.name(), keyStore.getType(), equalTo(format.type()));
            assertThat(format.name(), keyStore.size(), equalTo(0));
        }
    }

    /**
     * A key pair written in each format must be readable back through the same facade. For BCFKS in particular this
     * exercises the bundled (non-FIPS) Bouncy Castle provider, which is what lets the tools emit BCFKS without a FIPS JVM.
     */
    public void testWriteAndReadKeyStore() throws Exception {
        final KeyStoreFormat format = randomFrom(KeyStoreFormat.values());
        final KeyPair keyPair = CertGenUtils.generateKeyPair(2048);
        final X509Certificate certificate = CertGenUtils.generateSignedCertificate(
            new X500Principal("CN=" + randomAlphaOfLength(8)),
            null,
            keyPair,
            null,
            null,
            randomIntBetween(1, 30)
        );
        final char[] password = randomAlphaOfLengthBetween(8, 16).toCharArray();
        final String alias = randomAlphaOfLength(6);

        final KeyStore written = format.newKeyStore();
        written.setKeyEntry(alias, keyPair.getPrivate(), password, new Certificate[] { certificate });
        final Path path = createTempDir().resolve("keystore" + format.extension());
        try (OutputStream out = Files.newOutputStream(path)) {
            written.store(out, password);
        }

        final KeyStore read = format.readKeyStore(path, password);
        assertThat(read.getType(), equalTo(format.type()));
        assertThat(read.size(), equalTo(1));
        assertThat(read.isKeyEntry(alias), is(true));
        assertThat(read.getKey(alias, password), notNullValue());
        assertThat(read.getKey(alias, password), equalTo(keyPair.getPrivate()));
        assertThat(read.getCertificate(alias), equalTo(certificate));
    }
}
