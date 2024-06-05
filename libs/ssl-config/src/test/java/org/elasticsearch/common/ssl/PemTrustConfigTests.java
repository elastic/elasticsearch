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

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.X509ExtendedTrustManager;

public class PemTrustConfigTests extends ESTestCase {

    private static final String CERTS_DIR = "/certs";
    private Path basePath;

    @Before
    public void setupPath() {
        basePath = getDataPath(CERTS_DIR);
    }

    public void testBuildTrustConfigFromSinglePemFile() throws Exception {
        final String cert = "ca1/ca.crt";
        final PemTrustConfig trustConfig = new PemTrustConfig(Collections.singletonList(cert), basePath);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(resolvePath(cert)));
        assertCertificateChain(trustConfig, "CN=Test CA 1");
    }

    public void testBuildTrustConfigFromMultiplePemFiles() throws Exception {
        final String cert1 = "ca1/ca.crt";
        final String cert2 = "ca2/ca.crt";
        final String cert3 = "ca3/ca.crt";
        final PemTrustConfig trustConfig = new PemTrustConfig(Arrays.asList(cert1, cert2, cert3), basePath);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(resolvePaths(cert1, cert2, cert3)));
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 2", "CN=Test CA 3");
    }

    public void testBadFileFormatFails() throws Exception {
        final Path ca = createTempFile("ca", ".crt");
        Files.write(ca, generateRandomByteArrayOfLength(128), StandardOpenOption.APPEND);
        final PemTrustConfig trustConfig = new PemTrustConfig(List.of(ca.toString()), basePath);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ca));
        assertInvalidFileFormat(trustConfig, ca);
    }

    public void testEmptyFileFails() throws Exception {
        final Path ca = createTempFile("ca", ".crt");
        final PemTrustConfig trustConfig = new PemTrustConfig(List.of(ca.toString()), basePath);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ca));
        assertEmptyFile(trustConfig, ca);
    }

    public void testMissingFileFailsWithMeaningfulMessage() throws Exception {
        final PemTrustConfig trustConfig = new PemTrustConfig(Collections.singletonList("dne.crt"), basePath);
        final Path path = resolvePath("dne.crt");
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(path));
        assertFileNotFound(trustConfig, path);
    }

    public void testOneMissingFileFailsWithMeaningfulMessageEvenIfOtherFileExist() throws Exception {
        final String cert1 = "ca1/ca.crt";
        final String cert2 = "ca2/dne.crt";
        final String cert3 = "ca3/ca.crt";
        final PemTrustConfig trustConfig = new PemTrustConfig(Arrays.asList(cert1, cert2, cert3), basePath);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(resolvePaths(cert1, cert2, cert3)));
        assertFileNotFound(trustConfig, resolvePath(cert2));
    }

    public void testTrustConfigReloadsFileContents() throws Exception {
        final Path cert1 = getDataPath("/certs/ca1/ca.crt");
        final Path cert2 = getDataPath("/certs/ca2/ca.crt");
        final Path cert3 = getDataPath("/certs/ca3/ca.crt");

        final Path ca1 = createTempFile("ca1", ".crt");
        final Path ca2 = createTempFile("ca2", ".crt");

        final PemTrustConfig trustConfig = new PemTrustConfig(Arrays.asList(ca1.toString(), ca2.toString()), basePath);

        Files.copy(cert1, ca1, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(cert2, ca2, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 2");

        Files.copy(cert3, ca2, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 3");

        Files.delete(ca1);
        assertFileNotFound(trustConfig, ca1);

        Files.write(ca1, generateRandomByteArrayOfLength(128), StandardOpenOption.CREATE);
        assertInvalidFileFormat(trustConfig, ca1);
    }

    private void assertCertificateChain(PemTrustConfig trustConfig, String... caNames) {
        final X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
        final X509Certificate[] issuers = trustManager.getAcceptedIssuers();
        final Set<String> issuerNames = Stream.of(issuers)
            .map(X509Certificate::getSubjectX500Principal)
            .map(Principal::getName)
            .collect(Collectors.toSet());

        assertThat(issuerNames, Matchers.containsInAnyOrder(caNames));
    }

    private void assertEmptyFile(PemTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        logger.info("failure", exception);
        assertThat(exception.getMessage(), Matchers.containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), Matchers.containsString("failed to parse any certificates"));
    }

    private void assertInvalidFileFormat(PemTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), Matchers.containsString(file.toAbsolutePath().toString()));
        // When running on BC-FIPS, an invalid file format *might* just fail to parse, without any errors (just like an empty file)
        // or it might behave per the SUN provider, and throw a GSE (depending on exactly what was invalid)
        if (inFipsJvm() && exception.getMessage().contains("failed to parse any certificates")) {
            return;
        }
        assertThat(exception.getMessage(), Matchers.containsString("cannot load"));
        assertThat(exception.getMessage(), Matchers.containsString("PEM certificate"));
        assertThat(exception.getCause(), Matchers.instanceOf(GeneralSecurityException.class));
    }

    private void assertFileNotFound(PemTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), Matchers.containsString("not exist"));
        assertThat(exception.getMessage(), Matchers.containsString("PEM"));
        assertThat(exception.getMessage(), Matchers.containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getCause(), Matchers.instanceOf(NoSuchFileException.class));
    }

    private Path resolvePath(String relativeName) {
        return getDataPath(CERTS_DIR).resolve(relativeName);
    }

    private Path[] resolvePaths(String... names) {
        return Stream.of(names).map(this::resolvePath).toArray(Path[]::new);
    }

    private byte[] generateRandomByteArrayOfLength(int length) {
        byte[] bytes = randomByteArrayOfLength(length);
        /*
         * If the bytes represent DER encoded value indicating ASN.1 SEQUENCE followed by length byte if it is zero then while trying to
         * parse PKCS7 block from the encoded stream, it failed parsing the content type. The DerInputStream.getSequence() method in this
         * case returns an empty DerValue array but ContentType does not check the length of array before accessing the array resulting in a
         * ArrayIndexOutOfBoundsException. This check ensures that when we create random stream of bytes we do not create ASN.1 SEQUENCE
         * followed by zero length which fails the test intermittently.
         */
        while (checkRandomGeneratedBytesRepresentZeroLengthDerSequenceCausingArrayIndexOutOfBound(bytes)) {
            bytes = randomByteArrayOfLength(length);
        }
        return bytes;
    }

    private static boolean checkRandomGeneratedBytesRepresentZeroLengthDerSequenceCausingArrayIndexOutOfBound(byte[] bytes) {
        // Tag value indicating an ASN.1 "SEQUENCE". Reference: sun.security.util.DerValue.tag_Sequence = 0x30
        return bytes[0] == 0x30 && bytes[1] == 0x00;
    }
}
