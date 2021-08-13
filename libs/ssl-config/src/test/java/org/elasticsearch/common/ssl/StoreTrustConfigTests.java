/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class StoreTrustConfigTests extends ESTestCase {

    private static final char[] P12_PASS = "p12-pass".toCharArray();
    private static final char[] JKS_PASS = "jks-pass".toCharArray();
    private static final String DEFAULT_ALGORITHM = TrustManagerFactory.getDefaultAlgorithm();

    private Path configBasePath;

    @Before
    public void setupPath() {
        configBasePath = getDataPath("/certs");
    }

    public void testBuildTrustConfigFromPKCS12() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final String ks = "ca1/ca.p12";
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, P12_PASS, "PKCS12", DEFAULT_ALGORITHM, true, configBasePath);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(resolve(ks)));
        assertCertificateChain(trustConfig, "CN=Test CA 1");
    }

    public void testBuildTrustConfigFromJKS() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final String ks = "ca-all/ca.jks";
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, JKS_PASS, "jks", DEFAULT_ALGORITHM, true, configBasePath);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(resolve(ks)));
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 2", "CN=Test CA 3");
    }

    public void testBadKeyStoreFormatFails() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path ks = createTempFile("ca", ".p12");
        Files.write(ks, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        final String type = randomFrom("PKCS12", "jks");
        final String fileName = ks.toString();
        final StoreTrustConfig trustConfig = new StoreTrustConfig(fileName, new char[0], type, DEFAULT_ALGORITHM, true, configBasePath);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertInvalidFileFormat(trustConfig, ks);
    }

    public void testMissingKeyStoreFailsWithMeaningfulMessage() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final String ks = "ca-all/keystore.dne";
        final String type = randomFrom("PKCS12", "jks");
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, new char[0], type, DEFAULT_ALGORITHM, true, configBasePath);
        final Path path = resolve(ks);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(path));
        assertFileNotFound(trustConfig, path);
    }

    public void testIncorrectPasswordFailsWithMeaningfulMessage() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final String ks = "ca1/ca.p12";
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, new char[0], "PKCS12", DEFAULT_ALGORITHM, true, configBasePath);
        final Path path = resolve(ks);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(path));
        assertPasswordIsIncorrect(trustConfig, path);
    }

    public void testMissingTrustEntriesFailsWithMeaningfulMessage() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final String ks;
        final char[] password;
        final String type;
        if (randomBoolean()) {
            type = "PKCS12";
            ks = "cert-all/certs.p12";
            password = P12_PASS;
        } else {
            type = "jks";
            ks = "cert-all/certs.jks";
            password = JKS_PASS;
        }
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, password, type, DEFAULT_ALGORITHM, true, configBasePath);
        final Path path = resolve(ks);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(path));
        assertNoCertificateEntries(trustConfig, path);
    }

    public void testTrustConfigReloadsKeysStoreContents() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path ks1 = getDataPath("/certs/ca1/ca.p12");
        final Path ksAll = getDataPath("/certs/ca-all/ca.p12");

        final Path ks = createTempFile("ca", "p12");

        final String fileName = ks.toString();
        final StoreTrustConfig trustConfig = new StoreTrustConfig(fileName, P12_PASS, "PKCS12", DEFAULT_ALGORITHM, true, configBasePath);

        Files.copy(ks1, ks, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateChain(trustConfig, "CN=Test CA 1");

        Files.delete(ks);
        assertFileNotFound(trustConfig, ks);

        Files.write(ks, randomByteArrayOfLength(128), StandardOpenOption.CREATE);
        assertInvalidFileFormat(trustConfig, ks);

        Files.copy(ksAll, ks, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 2", "CN=Test CA 3");
    }

    private Path resolve(String name) {
        return configBasePath.resolve(name);
    }

    private void assertCertificateChain(StoreTrustConfig trustConfig, String... caNames) {
        final X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
        final X509Certificate[] issuers = trustManager.getAcceptedIssuers();
        final Set<String> issuerNames = Stream.of(issuers)
            .map(X509Certificate::getSubjectDN)
            .map(Principal::getName)
            .collect(Collectors.toSet());

        assertThat(issuerNames, Matchers.containsInAnyOrder(caNames));
    }

    private void assertInvalidFileFormat(StoreTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), containsString("cannot read"));
        assertThat(exception.getMessage(), containsString("keystore"));
        assertThat(exception.getMessage(), containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getCause(), Matchers.instanceOf(IOException.class));
    }

    private void assertFileNotFound(StoreTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), containsString("file does not exist"));
        assertThat(exception.getMessage(), containsString("keystore"));
        assertThat(exception.getMessage(), containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getCause(), instanceOf(NoSuchFileException.class));
    }

    private void assertPasswordIsIncorrect(StoreTrustConfig trustConfig, Path key) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), containsString("keystore"));
        assertThat(exception.getMessage(), containsString(key.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), containsString("password"));
    }

    private void assertNoCertificateEntries(StoreTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), containsString("does not contain any trusted certificate entries"));
        assertThat(exception.getMessage(), containsString("truststore"));
        assertThat(exception.getMessage(), containsString(file.toAbsolutePath().toString()));
    }

}
