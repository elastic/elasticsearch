/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.operator.OperatorException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Before;

import javax.net.ssl.X509ExtendedTrustManager;
import javax.security.auth.x500.X500Principal;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.cert.Certificate;
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

import static org.elasticsearch.xpack.core.ssl.CertUtils.generateSignedCertificate;

public class RestrictedTrustManagerTests extends ESTestCase {

    /**
     * Use a small keysize for performance, since the keys are only used in this test, but a large enough keysize
     * to get past the SSL algorithm checker
     */
    private static final int KEYSIZE = 1024;

    private X509ExtendedTrustManager baseTrustManager;
    private Map<String, X509Certificate[]> certificates;
    private int numberOfClusters;
    private int numberOfNodes;

    @Before
    public void generateCertificates() throws GeneralSecurityException, IOException, OperatorException {
        KeyPair caPair = CertUtils.generateKeyPair(KEYSIZE);
        X500Principal ca = new X500Principal("cn=CertAuth");
        X509Certificate caCert = CertUtils.generateCACertificate(ca, caPair, 30);
        baseTrustManager = CertUtils.trustManager(new Certificate[] { caCert });

        certificates = new HashMap<>();
        numberOfClusters = scaledRandomIntBetween(2, 8);
        numberOfNodes = scaledRandomIntBetween(2, 8);
        for (int cluster = 1; cluster <= numberOfClusters; cluster++) {
            for (int node = 1; node <= numberOfNodes; node++) {
                KeyPair nodePair = CertUtils.generateKeyPair(KEYSIZE);
                final String cn = "n" + node + ".c" + cluster;
                final X500Principal principal = new X500Principal("cn=" + cn);
                final String san = "node" + node + ".cluster" + cluster + ".elasticsearch";
                final GeneralNames altNames = new GeneralNames(CertUtils.createCommonName(san));
                final X509Certificate signed = generateSignedCertificate(principal, altNames, nodePair, caCert, caPair.getPrivate(), 30);
                final X509Certificate self = generateSignedCertificate(principal, altNames, nodePair, null, null, 30);
                certificates.put(cn + "/ca", new X509Certificate[] { signed });
                certificates.put(cn + "/self", new X509Certificate[] { self });
            }
        }
    }

    public void testTrustsExplicitCertificateName() throws Exception {
        final int trustedCluster = randomIntBetween(1, numberOfClusters);
        final List<String> trustedNames = new ArrayList<>(numberOfNodes);
        for (int node = 1; node <= numberOfNodes; node++) {
            trustedNames.add("node" + node + ".cluster" + trustedCluster + ".elasticsearch");
        }
        final CertificateTrustRestrictions restrictions = new CertificateTrustRestrictions(trustedNames);
        final RestrictedTrustManager trustManager = new RestrictedTrustManager(Settings.EMPTY, baseTrustManager, restrictions);
        assertSingleClusterIsTrusted(trustedCluster, trustManager, trustedNames);
    }

    public void testTrustsWildcardCertificateName() throws Exception {
        final int trustedCluster = randomIntBetween(1, numberOfClusters);
        final List<String> trustedNames = Collections.singletonList("*.cluster" + trustedCluster + ".elasticsearch");
        final CertificateTrustRestrictions restrictions = new CertificateTrustRestrictions(trustedNames);
        final RestrictedTrustManager trustManager = new RestrictedTrustManager(Settings.EMPTY, baseTrustManager, restrictions);
        assertSingleClusterIsTrusted(trustedCluster, trustManager, trustedNames);
    }

    public void testTrustWithRegexCertificateName() throws Exception {
        final int trustedNode = randomIntBetween(1, numberOfNodes);
        final List<String> trustedNames = Collections.singletonList("/node" + trustedNode + ".cluster[0-9].elasticsearch/");
        final CertificateTrustRestrictions restrictions = new CertificateTrustRestrictions(
                trustedNames
        );
        final RestrictedTrustManager trustManager = new RestrictedTrustManager(Settings.EMPTY, baseTrustManager, restrictions);
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
        final RestrictedTrustManager trustManager = new RestrictedTrustManager(Settings.EMPTY, baseTrustManager, restrictions);
        for (String cert : certificates.keySet()) {
            if (cert.endsWith("/ca")) {
                assertTrusted(trustManager, cert);
            } else {
                assertNotValid(trustManager, cert, "PKIX path building failed.*");
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
            assertThat(e.getMessage(), new TypeSafeMatcher<String>() {
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
        return Arrays.stream(cert).map(c -> c.getSubjectDN().getName()).collect(Collectors.joining(", "));
    }

}
