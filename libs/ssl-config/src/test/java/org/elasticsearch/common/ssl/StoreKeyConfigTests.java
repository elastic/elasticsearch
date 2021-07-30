/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StoreKeyConfigTests extends ESTestCase {

    private static final int IP_NAME = 7;
    private static final int DNS_NAME = 2;

    private static final char[] P12_PASS = "p12-pass".toCharArray();
    private static final char[] JKS_PASS = "jks-pass".toCharArray();

    private Path configBasePath;

    @Before
    public void setupPath() {
        configBasePath = getDataPath("/certs");
    }

    public void testLoadSingleKeyPKCS12() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path p12 = getDataPath("/certs/cert1/cert1.p12");
        final StoreKeyConfig keyConfig = config(p12, P12_PASS, "PKCS12");
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(p12));
        assertKeysLoaded(keyConfig, "cert1");
    }

    public void testLoadMultipleKeyPKCS12() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path p12 = getDataPath("/certs/cert-all/certs.p12");
        final StoreKeyConfig keyConfig = config(p12, P12_PASS, "PKCS12");
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(p12));
        assertKeysLoaded(keyConfig, "cert1", "cert2");
    }

    public void testLoadMultipleKeyJksWithSeparateKeyPassword() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final String jks = "cert-all/certs.jks";
        final StoreKeyConfig keyConfig = new StoreKeyConfig(jks, JKS_PASS, "jks", "key-pass".toCharArray(),
            KeyManagerFactory.getDefaultAlgorithm(), configBasePath);
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(configBasePath.resolve(jks)));
        assertKeysLoaded(keyConfig, "cert1", "cert2");
    }

    public void testKeyManagerFailsWithIncorrectStorePassword() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final String jks = "cert-all/certs.jks";
        final StoreKeyConfig keyConfig = new StoreKeyConfig(jks, P12_PASS, "jks", "key-pass".toCharArray(),
            KeyManagerFactory.getDefaultAlgorithm(), configBasePath);
        final Path path = configBasePath.resolve(jks);
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(path));
        assertPasswordIsIncorrect(keyConfig, path);
    }

    public void testKeyManagerFailsWithIncorrectKeyPassword() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path jks = getDataPath("/certs/cert-all/certs.jks");
        final StoreKeyConfig keyConfig = config(jks, JKS_PASS, "jks");
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(jks));
        assertPasswordIsIncorrect(keyConfig, jks);
    }

    public void testKeyManagerFailsWithMissingKeystoreFile() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path path = getDataPath("/certs/cert-all/certs.jks").getParent().resolve("dne.jks");
        final StoreKeyConfig keyConfig = config(path, JKS_PASS, "jks");
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(path));
        assertFileNotFound(keyConfig, path);
    }

    public void testMissingKeyEntriesFailsWithMeaningfulMessage() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path ks;
        final char[] password;
        final String type;
        if (randomBoolean()) {
            type = "PKCS12";
            ks = getDataPath("/certs/ca-all/ca.p12");
            password = P12_PASS;
        } else {
            type = "jks";
            ks = getDataPath("/certs/ca-all/ca.jks");
            password = JKS_PASS;
        }
        final StoreKeyConfig keyConfig = config(ks, password, type);
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertNoPrivateKeyEntries(keyConfig, ks);
    }

    public void testKeyConfigReloadsFileContents() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path cert1 = getDataPath("/certs/cert1/cert1.p12");
        final Path cert2 = getDataPath("/certs/cert2/cert2.p12");
        final Path jks = getDataPath("/certs/cert-all/certs.jks");

        final Path p12 = createTempFile("cert", ".p12");

        final StoreKeyConfig keyConfig = config(p12, P12_PASS, "PKCS12");

        Files.copy(cert1, p12, StandardCopyOption.REPLACE_EXISTING);
        assertKeysLoaded(keyConfig, "cert1");
        assertKeysNotLoaded(keyConfig, "cert2");

        Files.copy(jks, p12, StandardCopyOption.REPLACE_EXISTING);
        // Because (a) cannot load a JKS as a PKCS12 & (b) the password is wrong.
        assertBadKeyStore(keyConfig, p12);

        Files.copy(cert2, p12, StandardCopyOption.REPLACE_EXISTING);
        assertKeysLoaded(keyConfig, "cert2");
        assertKeysNotLoaded(keyConfig, "cert1");

        Files.delete(p12);
        assertFileNotFound(keyConfig, p12);
    }

    private StoreKeyConfig config(Path path, char[] password, String type) {
        final String pathName = path == null ? null : path.toString();
        return new StoreKeyConfig(pathName, password, type, password, KeyManagerFactory.getDefaultAlgorithm(), configBasePath);
    }

    private void assertKeysLoaded(StoreKeyConfig keyConfig, String... names) throws CertificateParsingException {
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager();
        assertThat(keyManager, notNullValue());

        for (String name : names) {
            final PrivateKey privateKey = keyManager.getPrivateKey(name);
            assertThat(privateKey, notNullValue());
            assertThat(privateKey.getAlgorithm(), is("RSA"));

            final X509Certificate[] chain = keyManager.getCertificateChain(name);
            assertThat(chain, notNullValue());
            assertThat(chain, arrayWithSize(1));
            final X509Certificate certificate = chain[0];
            assertThat(certificate.getIssuerDN().getName(), is("CN=Test CA 1"));
            assertThat(certificate.getSubjectDN().getName(), is("CN=" + name));
            assertThat(certificate.getSubjectAlternativeNames(), iterableWithSize(2));
            assertThat(certificate.getSubjectAlternativeNames(), containsInAnyOrder(
                Arrays.asList(DNS_NAME, "localhost"),
                Arrays.asList(IP_NAME, "127.0.0.1")
            ));
        }

        final List<Tuple<PrivateKey, X509Certificate>> keys = keyConfig.getKeys();
        assertThat(keys, iterableWithSize(names.length));
        for (Tuple<PrivateKey, X509Certificate> tup : keys) {
            PrivateKey privateKey = tup.v1();
            assertThat(privateKey, notNullValue());
            assertThat(privateKey.getAlgorithm(), is("RSA"));

            final X509Certificate certificate = tup.v2();
            assertThat(certificate.getIssuerDN().getName(), is("CN=Test CA 1"));
        }
    }
    
    private void assertKeysNotLoaded(StoreKeyConfig keyConfig, String... names) throws CertificateParsingException {
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager();
        assertThat(keyManager, notNullValue());

        for (String name : names) {
            final PrivateKey privateKey = keyManager.getPrivateKey(name);
            assertThat(privateKey, nullValue());
        }
    }

    private void assertPasswordIsIncorrect(StoreKeyConfig keyConfig, Path key) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString("keystore"));
        assertThat(exception.getMessage(), containsString(key.toAbsolutePath().toString()));
        if (exception.getCause() instanceof GeneralSecurityException) {
            assertThat(exception.getMessage(), containsString("password"));
        } else {
            assertThat(exception.getCause(), instanceOf(IOException.class));
            assertThat(exception.getCause().getMessage(), containsString("password"));
        }
    }

    private void assertBadKeyStore(StoreKeyConfig keyConfig, Path key) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString("keystore"));
        assertThat(exception.getMessage(), containsString(key.toAbsolutePath().toString()));
        assertThat(exception.getCause(), instanceOf(IOException.class));
    }

    private void assertFileNotFound(StoreKeyConfig keyConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString("keystore"));
        assertThat(exception.getMessage(), containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), containsString("does not exist"));
        assertThat(exception.getCause(), instanceOf(NoSuchFileException.class));
    }

    private void assertNoPrivateKeyEntries(StoreKeyConfig keyConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString("keystore"));
        assertThat(exception.getMessage(), containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), containsString("does not contain a private key entry"));
    }
}
