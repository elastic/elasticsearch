/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import javax.net.ssl.X509ExtendedKeyManager;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;

public class PemKeyConfigTests extends ESTestCase {
    private static final int IP_NAME = 7;
    private static final int DNS_NAME = 2;

    public void testBuildKeyConfigFromPkcs1PemFilesWithoutPassword() throws Exception {
        final Path cert = getDataPath("/certs/cert1/cert1.crt");
        final Path key = getDataPath("/certs/cert1/cert1.key");
        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, new char[0]);
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(cert, key));
        assertCertificateAndKey(keyConfig, "CN=cert1");
    }

    public void testBuildKeyConfigFromPkcs1PemFilesWithPassword() throws Exception {
        final Path cert = getDataPath("/certs/cert2/cert2.crt");
        final Path key = getDataPath("/certs/cert2/cert2.key");
        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, "c2-pass".toCharArray());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(cert, key));
        assertCertificateAndKey(keyConfig, "CN=cert2");
    }

    public void testBuildKeyConfigFromPkcs8PemFilesWithoutPassword() throws Exception {
        final Path cert = getDataPath("/certs/cert1/cert1.crt");
        final Path key = getDataPath("/certs/cert1/cert1-pkcs8.key");
        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, new char[0]);
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(cert, key));
        assertCertificateAndKey(keyConfig, "CN=cert1");
    }

    public void testBuildKeyConfigFromPkcs8PemFilesWithPassword() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBE KeySpec is not available", inFipsJvm());
        final Path cert = getDataPath("/certs/cert2/cert2.crt");
        final Path key = getDataPath("/certs/cert2/cert2-pkcs8.key");
        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, "c2-pass".toCharArray());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(cert, key));
        assertCertificateAndKey(keyConfig, "CN=cert2");
    }

    public void testKeyManagerFailsWithIncorrectPassword() throws Exception {
        final Path cert = getDataPath("/certs/cert2/cert2.crt");
        final Path key = getDataPath("/certs/cert2/cert2.key");
        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, "wrong-password".toCharArray());
        assertPasswordIsIncorrect(keyConfig, key);
    }

    public void testMissingCertificateFailsWithMeaningfulMessage() throws Exception {
        final Path key = getDataPath("/certs/cert1/cert1.key");
        final Path cert = key.getParent().resolve("dne.crt");

        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, new char[0]);
        assertFileNotFound(keyConfig, "certificate", cert);
    }

    public void testMissingKeyFailsWithMeaningfulMessage() throws Exception {
        final Path cert = getDataPath("/certs/cert1/cert1.crt");
        final Path key = cert.getParent().resolve("dne.key");

        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, new char[0]);
        assertFileNotFound(keyConfig, "private key", key);
    }

    public void testKeyConfigReloadsFileContents() throws Exception {
        final Path cert1 = getDataPath("/certs/cert1/cert1.crt");
        final Path key1 = getDataPath("/certs/cert1/cert1.key");
        final Path cert2 = getDataPath("/certs/cert2/cert2.crt");
        final Path key2 = getDataPath("/certs/cert2/cert2.key");
        final Path cert = createTempFile("cert", ".crt");
        final Path key = createTempFile("cert", ".key");

        final PemKeyConfig keyConfig = new PemKeyConfig(cert, key, new char[0]);

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

    private void assertCertificateAndKey(PemKeyConfig keyConfig, String expectedDN) throws CertificateParsingException {
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager();
        assertThat(keyManager, notNullValue());

        final PrivateKey privateKey = keyManager.getPrivateKey("key");
        assertThat(privateKey, notNullValue());
        assertThat(privateKey.getAlgorithm(), is("RSA"));

        final X509Certificate[] chain = keyManager.getCertificateChain("key");
        assertThat(chain, notNullValue());
        assertThat(chain, arrayWithSize(1));
        final X509Certificate certificate = chain[0];
        assertThat(certificate.getIssuerDN().getName(), is("CN=Test CA 1"));
        assertThat(certificate.getSubjectDN().getName(), is(expectedDN));
        assertThat(certificate.getSubjectAlternativeNames(), iterableWithSize(2));
        assertThat(certificate.getSubjectAlternativeNames(), containsInAnyOrder(
            Arrays.asList(DNS_NAME, "localhost"),
            Arrays.asList(IP_NAME, "127.0.0.1")
        ));
    }

    private void assertPasswordIsIncorrect(PemKeyConfig keyConfig, Path key) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString("private key file"));
        assertThat(exception.getMessage(), containsString(key.toAbsolutePath().toString()));
        assertThat(exception.getCause(), instanceOf(GeneralSecurityException.class));
    }

    private void assertFileNotFound(PemKeyConfig keyConfig, String type, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString(type + " file"));
        assertThat(exception.getMessage(), containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), containsString("does not exist"));
        assertThat(exception.getCause(), instanceOf(NoSuchFileException.class));
    }
}
