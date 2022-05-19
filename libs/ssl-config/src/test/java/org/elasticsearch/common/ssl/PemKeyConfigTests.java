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

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import javax.net.ssl.X509ExtendedKeyManager;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PemKeyConfigTests extends ESTestCase {
    private static final int IP_NAME = 7;
    private static final int DNS_NAME = 2;

    private Path configBasePath;

    @Before
    public void setupPath() {
        configBasePath = getDataPath("/certs");
    }

    public void testBuildKeyConfigFromPkcs1PemFilesWithoutPassword() throws Exception {
        final String cert = "cert1/cert1.crt";
        final String key = "cert1/cert1.key";
        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, new char[0], configBasePath);
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(resolve(cert, key)));
        assertCertificateAndKey(keyConfig, "CN=cert1");
    }

    public void testBuildKeyConfigFromPkcs1PemFilesWithPassword() throws Exception {
        final String cert = "cert2/cert2.crt";
        final String key = "cert2/cert2.key";
        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, "c2-pass".toCharArray(), configBasePath);
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(resolve(cert, key)));
        assertCertificateAndKey(keyConfig, "CN=cert2");
    }

    public void testBuildKeyConfigFromPkcs8PemFilesWithoutPassword() throws Exception {
        final String cert = "cert1/cert1.crt";
        final String key = "cert1/cert1-pkcs8.key";
        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, new char[0], configBasePath);
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(resolve(cert, key)));
        assertCertificateAndKey(keyConfig, "CN=cert1");
    }

    public void testBuildKeyConfigFromPkcs8PemFilesWithPassword() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBE KeySpec is not available", inFipsJvm());
        final String cert = "cert2/cert2.crt";
        final String key = "cert2/cert2-pkcs8.key";
        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, "c2-pass".toCharArray(), configBasePath);
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(resolve(cert, key)));
        assertCertificateAndKey(keyConfig, "CN=cert2");
    }

    public void testBuildKeyConfigUsingCertificateChain() throws Exception {
        final String ca = "ca1/ca.crt";
        final String cert = "cert1/cert1.crt";
        final String key = "cert1/cert1.key";

        final Path chain = createTempFile("chain", ".crt");
        Files.write(chain, Files.readAllBytes(configBasePath.resolve(cert)), StandardOpenOption.APPEND);
        Files.write(chain, Files.readAllBytes(configBasePath.resolve(ca)), StandardOpenOption.APPEND);

        final PemKeyConfig keyConfig = new PemKeyConfig(chain.toString(), key, new char[0], configBasePath);
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(chain, configBasePath.resolve(key)));
        assertCertificateAndKey(keyConfig, "CN=cert1", "CN=Test CA 1");
        final Collection<? extends StoredCertificate> certificates = keyConfig.getConfiguredCertificates();
        assertThat(certificates, Matchers.hasSize(2));
        final Iterator<? extends StoredCertificate> iterator = certificates.iterator();
        StoredCertificate c1 = iterator.next();
        StoredCertificate c2 = iterator.next();

        assertThat(c1.certificate().getSubjectX500Principal().toString(), equalTo("CN=cert1"));
        assertThat(c1.hasPrivateKey(), equalTo(true));
        assertThat(c1.alias(), nullValue());
        assertThat(c1.format(), equalTo("PEM"));
        assertThat(c1.path(), equalTo(chain.toString()));

        assertThat(c2.certificate().getSubjectX500Principal().toString(), equalTo("CN=Test CA 1"));
        assertThat(c2.hasPrivateKey(), equalTo(false));
        assertThat(c2.alias(), nullValue());
        assertThat(c2.format(), equalTo("PEM"));
        assertThat(c2.path(), equalTo(chain.toString()));

        final List<Tuple<PrivateKey, X509Certificate>> keys = keyConfig.getKeys();
        assertThat(keys, iterableWithSize(1));
        assertThat(keys.get(0).v1(), notNullValue());
        assertThat(keys.get(0).v1().getAlgorithm(), equalTo("RSA"));
        assertThat(keys.get(0).v2(), notNullValue());
        assertThat(keys.get(0).v2().getSubjectX500Principal().toString(), equalTo("CN=cert1"));
    }

    public void testInvertedCertificateChainFailsToCreateKeyManager() throws Exception {
        final String ca = "ca1/ca.crt";
        final String cert = "cert1/cert1.crt";
        final String key = "cert1/cert1.key";

        final Path chain = createTempFile("chain", ".crt");
        // This is (intentionally) the wrong order. It should be cert + ca.
        Files.write(chain, Files.readAllBytes(configBasePath.resolve(ca)), StandardOpenOption.APPEND);
        Files.write(chain, Files.readAllBytes(configBasePath.resolve(cert)), StandardOpenOption.APPEND);

        final PemKeyConfig keyConfig = new PemKeyConfig(chain.toString(), key, new char[0], configBasePath);
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);

        assertThat(exception.getMessage(), containsString("failed to load a KeyManager"));
        final Throwable cause = exception.getCause();
        assertThat(cause, notNullValue());
        if (inFipsJvm()) {
            // BC FKS first checks that the key & cert match (they don't because the key is for 'cert1' not 'ca')
            assertThat(cause.getMessage(), containsString("RSA keys do not have the same modulus"));
        } else {
            // SUN PKCS#12 first checks that the chain is correctly structured (it's not, due to the order)
            assertThat(cause.getMessage(), containsString("Certificate chain is not valid"));
        }
    }

    public void testKeyManagerFailsWithIncorrectPassword() throws Exception {
        final Path cert = getDataPath("/certs/cert2/cert2.crt");
        final Path key = getDataPath("/certs/cert2/cert2.key");
        final PemKeyConfig keyConfig = new PemKeyConfig(cert.toString(), key.toString(), "wrong-password".toCharArray(), configBasePath);
        assertPasswordIsIncorrect(keyConfig, key);
    }

    public void testMissingCertificateFailsWithMeaningfulMessage() throws Exception {
        final Path key = getDataPath("/certs/cert1/cert1.key");
        final Path cert = key.getParent().resolve("dne.crt");

        final PemKeyConfig keyConfig = new PemKeyConfig(cert.toString(), key.toString(), new char[0], configBasePath);
        assertFileNotFound(keyConfig, "certificate", cert);
    }

    public void testMissingKeyFailsWithMeaningfulMessage() throws Exception {
        final Path cert = getDataPath("/certs/cert1/cert1.crt");
        final Path key = cert.getParent().resolve("dne.key");

        final PemKeyConfig keyConfig = new PemKeyConfig(cert.toString(), key.toString(), new char[0], configBasePath);
        assertFileNotFound(keyConfig, "private key", key);
    }

    public void testKeyConfigReloadsFileContents() throws Exception {
        final Path cert1 = getDataPath("/certs/cert1/cert1.crt");
        final Path key1 = getDataPath("/certs/cert1/cert1.key");
        final Path cert2 = getDataPath("/certs/cert2/cert2.crt");
        final Path key2 = getDataPath("/certs/cert2/cert2.key");
        final Path cert = createTempFile("cert", ".crt");
        final Path key = createTempFile("cert", ".key");

        final PemKeyConfig keyConfig = new PemKeyConfig(cert.toString(), key.toString(), new char[0], configBasePath);

        Files.copy(cert1, cert, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(key1, key, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateAndKey(keyConfig, "CN=cert1");

        Files.copy(cert2, cert, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(key2, key, StandardCopyOption.REPLACE_EXISTING);
        assertPasswordIsIncorrect(keyConfig, key);

        Files.copy(cert1, cert, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(key1, key, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateAndKey(keyConfig, "CN=cert1");

        Files.delete(cert);
        assertFileNotFound(keyConfig, "certificate", cert);
    }

    private Path[] resolve(String... names) {
        return Stream.of(names).map(configBasePath::resolve).toArray(Path[]::new);
    }

    private void assertCertificateAndKey(PemKeyConfig keyConfig, String certDN, String... caDN) throws CertificateParsingException {
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager();
        assertThat(keyManager, notNullValue());

        final PrivateKey privateKey = keyManager.getPrivateKey("key");
        assertThat(privateKey, notNullValue());
        assertThat(privateKey.getAlgorithm(), is("RSA"));

        final X509Certificate[] chain = keyManager.getCertificateChain("key");
        assertThat(chain, notNullValue());
        assertThat(chain, arrayWithSize(1 + caDN.length));
        final X509Certificate certificate = chain[0];
        assertThat(certificate.getIssuerX500Principal().getName(), is("CN=Test CA 1"));
        assertThat(certificate.getSubjectX500Principal().getName(), is(certDN));
        assertThat(certificate.getSubjectAlternativeNames(), iterableWithSize(2));
        assertThat(
            certificate.getSubjectAlternativeNames(),
            containsInAnyOrder(Arrays.asList(DNS_NAME, "localhost"), Arrays.asList(IP_NAME, "127.0.0.1"))
        );

        for (int i = 0; i < caDN.length; i++) {
            final X509Certificate ca = chain[i + 1];
            assertThat(ca.getSubjectX500Principal().getName(), is(caDN[i]));
        }
    }

    private void assertPasswordIsIncorrect(PemKeyConfig keyConfig, Path key) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString("PEM private key"));
        assertThat(exception.getMessage(), containsString(key.toAbsolutePath().toString()));
        assertThat(exception.getCause(), instanceOf(GeneralSecurityException.class));
    }

    private void assertFileNotFound(PemKeyConfig keyConfig, String type, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString(type + " ["));
        assertThat(exception.getMessage(), containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), containsString("does not exist"));
        assertThat(exception.getCause(), instanceOf(NoSuchFileException.class));
    }
}
