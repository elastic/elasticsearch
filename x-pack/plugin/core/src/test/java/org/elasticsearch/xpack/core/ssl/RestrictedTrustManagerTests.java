/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.net.ssl.X509ExtendedTrustManager;

public class RestrictedTrustManagerTests extends ESTestCase {

    private X509ExtendedTrustManager baseTrustManager;
    private Map<String, X509Certificate[]> certificates;
    private int numberOfClusters;
    private int numberOfNodes;

    @Before
    public void readCertificates() throws GeneralSecurityException, IOException {

        final Path caPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca.crt");
        baseTrustManager = CertParsingUtils.getTrustManagerFromPEM(List.of(caPath));
        certificates = new HashMap<>();
        Files.walkFileTree(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/self-signed"),
            new SimpleFileVisitor<Path>() {

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        String fileName = file.getFileName().toString();
                        if (fileName.endsWith(".crt")) {
                            certificates.put(
                                fileName.replace(".crt", "/self"),
                                CertParsingUtils.readX509Certificates(Collections.singletonList(file))
                            );
                        }
                        return FileVisitResult.CONTINUE;
                    } catch (CertificateException e) {
                        throw new IOException("Failed to read X.509 Certificate from: " + file.toAbsolutePath().toString());
                    }
                }
            }
        );

        Files.walkFileTree(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca-signed"),
            new SimpleFileVisitor<Path>() {

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        String fileName = file.getFileName().toString();
                        if (fileName.endsWith(".crt")) {
                            certificates.put(
                                fileName.replace(".crt", "/ca"),
                                CertParsingUtils.readX509Certificates(Collections.singletonList(file))
                            );
                        }
                        return FileVisitResult.CONTINUE;
                    } catch (CertificateException e) {
                        throw new IOException("Failed to read X.509 Certificate from: " + file.toAbsolutePath().toString());
                    }
                }
            }
        );

        numberOfClusters = scaledRandomIntBetween(2, 8);
        numberOfNodes = scaledRandomIntBetween(2, 8);
    }

    public void testTrustsExplicitCertificateName() throws Exception {
        final int trustedCluster = randomIntBetween(1, numberOfClusters);
        final List<String> trustedNames = new ArrayList<>(numberOfNodes);
        for (int node = 1; node <= numberOfNodes; node++) {
            trustedNames.add("node" + node + ".cluster" + trustedCluster + ".elasticsearch");
        }
        final CertificateTrustRestrictions restrictions = new CertificateTrustRestrictions(trustedNames);
        final RestrictedTrustManager trustManager = new RestrictedTrustManager(baseTrustManager, restrictions);
        assertSingleClusterIsTrusted(trustedCluster, trustManager, trustedNames);
    }

    public void testTrustsWildcardCertificateName() throws Exception {
        final int trustedCluster = randomIntBetween(1, numberOfClusters);
        final List<String> trustedNames = Collections.singletonList("*.cluster" + trustedCluster + ".elasticsearch");
        final CertificateTrustRestrictions restrictions = new CertificateTrustRestrictions(trustedNames);
        final RestrictedTrustManager trustManager = new RestrictedTrustManager(baseTrustManager, restrictions);
        assertSingleClusterIsTrusted(trustedCluster, trustManager, trustedNames);
    }

    public void testTrustWithRegexCertificateName() throws Exception {
        final int trustedNode = randomIntBetween(1, numberOfNodes);
        final List<String> trustedNames = Collections.singletonList("/node" + trustedNode + ".cluster[0-9].elasticsearch/");
        final CertificateTrustRestrictions restrictions = new CertificateTrustRestrictions(trustedNames);
        final RestrictedTrustManager trustManager = new RestrictedTrustManager(baseTrustManager, restrictions);
        for (int cluster = 1; cluster <= numberOfClusters; cluster++) {
            for (int node = 1; node <= numberOfNodes; node++) {
                if (node == trustedNode) {
                    assertTrusted(trustManager, "n" + node + ".c1/ca");
                } else {
                    assertNotTrusted(trustManager, "n" + node + ".c" + cluster + "/ca", trustedNames);
                }
            }
        }
    }

    public void testThatDelegateTrustManagerIsRespected() throws Exception {
        final CertificateTrustRestrictions restrictions = new CertificateTrustRestrictions(Collections.singletonList("*.elasticsearch"));
        final RestrictedTrustManager trustManager = new RestrictedTrustManager(baseTrustManager, restrictions);
        for (String cert : certificates.keySet()) {
            if (cert.endsWith("/ca")) {
                assertTrusted(trustManager, cert);
            } else {
                assertNotValid(
                    trustManager,
                    cert,
                    inFipsJvm() ? "unable to process certificates: Unable to find certificate chain." : "PKIX path building failed.*"
                );
            }
        }
    }

    private void assertSingleClusterIsTrusted(int trustedCluster, RestrictedTrustManager trustManager, List<String> trustedNames)
        throws Exception {
        for (int cluster = 1; cluster <= numberOfClusters; cluster++) {
            for (int node = 1; node <= numberOfNodes; node++) {
                final String certAlias = "n" + node + ".c" + cluster + "/ca";
                if (cluster == trustedCluster) {
                    assertTrusted(trustManager, certAlias);
                } else {
                    assertNotTrusted(trustManager, certAlias, trustedNames);
                }
            }
        }
    }

    private void assertTrusted(RestrictedTrustManager trustManager, String certAlias) throws Exception {
        final X509Certificate[] chain = Objects.requireNonNull(this.certificates.get(certAlias));
        try {
            trustManager.checkClientTrusted(chain, "ignore");
            // pass
        } catch (CertificateException e) {
            Assert.fail("Certificate " + describe(chain) + " is not trusted - " + e);
        }
    }

    private void assertNotTrusted(RestrictedTrustManager trustManager, String certAlias, List<String> trustedNames) throws Exception {
        final String expectedError = ".* does not match the trusted names \\[.*" + Pattern.quote(trustedNames.get(0)) + ".*";
        assertNotValid(trustManager, certAlias, expectedError);
    }

    private void assertNotValid(RestrictedTrustManager trustManager, String certAlias, String expectedError) throws Exception {
        final X509Certificate[] chain = Objects.requireNonNull(this.certificates.get(certAlias));
        try {
            trustManager.checkClientTrusted(chain, "ignore");
            Assert.fail("Certificate " + describe(chain) + " is trusted but shouldn't be");
        } catch (CertificateException e) {
            assertThat(e.getMessage(), new TypeSafeMatcher<>() {
                @Override
                public void describeTo(Description description) {
                    description.appendText("matches pattern ").appendText(expectedError);
                }

                @Override
                protected boolean matchesSafely(String item) {
                    return item.matches(expectedError);
                }
            });
        }
    }

    private String describe(X509Certificate[] cert) {
        return Arrays.stream(cert).map(c -> c.getSubjectX500Principal().getName()).collect(Collectors.joining(", "));
    }

}
