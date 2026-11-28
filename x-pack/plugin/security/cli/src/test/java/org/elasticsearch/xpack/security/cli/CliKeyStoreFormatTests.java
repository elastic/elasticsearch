/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.cli.bc.CertGenUtils;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.security.auth.x500.X500Principal;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * BCFKS is exercised through the non-FIPS Bouncy Castle provider from the test classpath, passed as an instance and never
 * registered with the JVM, which is how the tools are given the BCFIPS provider in production.
 */
public class CliKeyStoreFormatTests extends ESTestCase {

    public void testForRuntimeWithoutBcFipsIsPkcs12() {
        assumeTrue("BCFIPS is registered", Security.getProvider(CliKeyStoreFormat.BC_FIPS_PROVIDER_NAME) == null);
        assertThat(CliKeyStoreFormat.forRuntime(), sameInstance(CliKeyStoreFormat.PKCS12));
    }

    public void testForProvidersWithoutBcFipsIsPkcs12() {
        assertThat(CliKeyStoreFormat.forProviders(name -> null), sameInstance(CliKeyStoreFormat.PKCS12));
    }

    public void testForProvidersWithBcFipsIsBcfks() throws Exception {
        final CliKeyStoreFormat format = CliKeyStoreFormat.forProviders(
            Map.of(CliKeyStoreFormat.BC_FIPS_PROVIDER_NAME, new BouncyCastleProvider())::get
        );
        assertThat(format.type(), equalTo("BCFKS"));
        assertWriteAndReadKeyPairs(format);
    }

    public void testCompatibilityNote() {
        assertThat(CliKeyStoreFormat.PKCS12.compatibilityNote(), nullValue());
        assertThat(bcfks().compatibilityNote(), containsString("Bouncy Castle FIPS provider"));
    }

    public void testPkcs12Naming() {
        assertThat(CliKeyStoreFormat.PKCS12.description(), equalTo("PKCS#12"));
        assertThat(CliKeyStoreFormat.PKCS12.extension(), equalTo(".p12"));
        // Elasticsearch infers PKCS12 from the .p12 extension, so generated configuration does not need a type setting
        assertThat(CliKeyStoreFormat.PKCS12.settingsType(), nullValue());
    }

    public void testBcfksNaming() {
        final CliKeyStoreFormat format = bcfks();
        assertThat(format.description(), equalTo("BCFKS"));
        assertThat(format.extension(), equalTo(".bcfks"));
        assertThat(format.settingsType(), equalTo("BCFKS"));
    }

    public void testPkcs12PasswordRules() {
        assertThat(CliKeyStoreFormat.PKCS12.passwordError(new char[0]), nullValue());
        assertThat(CliKeyStoreFormat.PKCS12.passwordError(randomAlphaOfLengthBetween(1, 30).toCharArray()), nullValue());
        assertThat(CliKeyStoreFormat.PKCS12.passwordError("pässwörd".toCharArray()), equalTo("PKCS#12 passwords must be plain ASCII"));
    }

    public void testBcfksPasswordRules() {
        final CliKeyStoreFormat format = bcfks();
        assertThat(format.passwordError(new char[0]), nullValue());
        assertThat(format.passwordError(randomAlphaOfLengthBetween(1, 30).toCharArray()), nullValue());
        assertThat(format.passwordError("pässwörd".toCharArray()), nullValue());
    }

    public void testPkcs12WriteAndReadKeyPairs() throws Exception {
        assertWriteAndReadKeyPairs(CliKeyStoreFormat.PKCS12);
    }

    public void testBcfksWriteAndReadKeyPairs() throws Exception {
        assertWriteAndReadKeyPairs(bcfks());
    }

    public void testBcfksDoesNotRegisterProvider() throws Exception {
        final BouncyCastleProvider provider = new BouncyCastleProvider();
        assumeTrue("provider is already registered", Security.getProvider(provider.getName()) == null);
        assertWriteAndReadKeyPairs(CliKeyStoreFormat.bcfks(provider));
        assertThat(Security.getProvider(provider.getName()), nullValue());
    }

    static CliKeyStoreFormat bcfks() {
        return CliKeyStoreFormat.bcfks(new BouncyCastleProvider());
    }

    private void assertWriteAndReadKeyPairs(CliKeyStoreFormat format) throws Exception {
        final KeyPair keyPair = CertGenUtils.generateKeyPair(2048);
        final X509Certificate cert = CertGenUtils.generateSignedCertificate(new X500Principal("CN=test"), null, keyPair, null, null, 30);
        final char[] password = randomAlphaOfLengthBetween(0, 20).toCharArray();

        final KeyStore keyStore = format.newKeyStore();
        keyStore.setKeyEntry("key", keyPair.getPrivate(), password, new Certificate[] { cert });
        final Path path = createTempDir().resolve("test" + format.extension());
        try (OutputStream out = Files.newOutputStream(path)) {
            keyStore.store(out, password);
        }

        final Map<Certificate, Key> keyPairs = format.readKeyPairs(path, password, alias -> password);
        assertThat(keyPairs, aMapWithSize(1));
        assertThat(keyPairs.get(cert), equalTo(keyPair.getPrivate()));
    }
}
