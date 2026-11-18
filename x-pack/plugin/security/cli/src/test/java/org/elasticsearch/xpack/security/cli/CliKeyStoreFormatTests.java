/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.cli.bc.CertGenUtils;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.security.auth.x500.X500Principal;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CliKeyStoreFormatTests extends ESTestCase {

    public void testForRuntimeIsPkcs12() {
        assertThat(CliKeyStoreFormat.forRuntime(), sameInstance(CliKeyStoreFormat.PKCS12));
    }

    public void testPkcs12Naming() {
        assertThat(CliKeyStoreFormat.PKCS12.description(), equalTo("PKCS#12"));
        assertThat(CliKeyStoreFormat.PKCS12.extension(), equalTo(".p12"));
        // Elasticsearch infers PKCS12 from the .p12 extension, so generated configuration does not need a type setting
        assertThat(CliKeyStoreFormat.PKCS12.settingsType(), nullValue());
    }

    public void testWriteAndReadKeyPairs() throws Exception {
        final CliKeyStoreFormat format = CliKeyStoreFormat.forRuntime();
        final KeyPair keyPair = CertGenUtils.generateKeyPair(2048);
        final X509Certificate cert = CertGenUtils.generateSignedCertificate(new X500Principal("CN=test"), null, keyPair, null, null, 30);
        final char[] password = randomAlphaOfLengthBetween(14, 20).toCharArray();

        final KeyStore keyStore = format.newKeyStore();
        keyStore.setKeyEntry("key", keyPair.getPrivate(), password, new Certificate[] { cert });
        final Path path = createTempDir().resolve("test.keystore");
        try (OutputStream out = Files.newOutputStream(path)) {
            keyStore.store(out, password);
        }

        final Map<Certificate, Key> keyPairs = format.readKeyPairs(path, password, alias -> password);
        assertThat(keyPairs, aMapWithSize(1));
        assertThat(keyPairs.get(cert), equalTo(keyPair.getPrivate()));
    }
}
