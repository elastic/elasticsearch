/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.ssl.CertUtils;
import org.elasticsearch.xpack.core.ssl.RestrictedTrustManager;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.x500.X500Principal;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.ssl.CertUtils.generateSignedCertificate;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for SSL trust restrictions
 *
 * @see RestrictedTrustManager
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@TestLogging("org.elasticsearch.xpack.ssl.RestrictedTrustManager:DEBUG")
public class SSLTrustRestrictionsTests extends SecurityIntegTestCase {

    /**
     * Use a small keysize for performance, since the keys are only used in this test, but a large enough keysize
     * to get past the SSL algorithm checker
     */
    private static final int KEYSIZE = 1024;

    private static final int RESOURCE_RELOAD_MILLIS = 3;
    private static final TimeValue MAX_WAIT_RELOAD = TimeValue.timeValueSeconds(1);

    private static Path configPath;
    private static Settings nodeSSL;

    private static CertificateInfo ca;
    private static CertificateInfo trustedCert;
    private static CertificateInfo untrustedCert;
    private static Path restrictionsPath;

    @Override
    protected int maxNumberOfNodes() {
        // We are trying to test the SSL configuration for which clients/nodes may join a cluster
        // We prefer the cluster to only have 1 node, so that the SSL checking doesn't happen until the test methods run
        // (That's not _quite_ true, because the base setup code checks the cluster using transport client, but it's the best we can do)
        return 1;
    }

    @BeforeClass
    public static void setupCertificates() throws Exception {
        configPath = createTempDir();

        final KeyPair caPair = CertUtils.generateKeyPair(KEYSIZE);
        final X509Certificate caCert = CertUtils.generateCACertificate(new X500Principal("cn=CertAuth"), caPair, 30);
        ca = writeCertificates("ca", caPair.getPrivate(), caCert);

        trustedCert = generateCertificate("trusted", "node.trusted");
        untrustedCert = generateCertificate("untrusted", "someone.else");

        nodeSSL = Settings.builder()
                .put("xpack.security.transport.ssl.enabled", true)
                .put("xpack.security.transport.ssl.verification_mode", "certificate")
                .putList("xpack.ssl.certificate_authorities", ca.getCertPath().toString())
                .put("xpack.ssl.key", trustedCert.getKeyPath())
                .put("xpack.ssl.certificate", trustedCert.getCertPath())
                .build();
    }

    @AfterClass
    public static void cleanup() {
        configPath = null;
        nodeSSL = null;
        ca = null;
        trustedCert = null;
        untrustedCert = null;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {

        Settings parentSettings = super.nodeSettings(nodeOrdinal);
        Settings.Builder builder = Settings.builder()
                .put(parentSettings.filter((s) -> s.startsWith("xpack.ssl.") == false))
                .put(nodeSSL);

        restrictionsPath = configPath.resolve("trust_restrictions.yml");
        writeRestrictions("*.trusted");
        builder.put("xpack.ssl.trust_restrictions.path", restrictionsPath);
        builder.put("resource.reload.interval.high", RESOURCE_RELOAD_MILLIS + "ms");

        return builder.build();
    }

    private void writeRestrictions(String trustedPattern) {
        try {
            Files.write(restrictionsPath, Collections.singleton("trust.subject_name: \"" + trustedPattern + "\""));
        } catch (IOException e) {
            throw new ElasticsearchException("failed to write restrictions", e);
        }
    }

    @Override
    protected Settings transportClientSettings() {
        Settings parentSettings = super.transportClientSettings();
        Settings.Builder builder = Settings.builder()
                .put(parentSettings.filter((s) -> s.startsWith("xpack.ssl.") == false))
                .put(nodeSSL);
        return builder.build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    public void testCertificateWithTrustedNameIsAccepted() throws Exception {
        writeRestrictions("*.trusted");
        try {
            tryConnect(trustedCert);
        } catch (SSLHandshakeException | SocketException ex) {
            logger.warn(new ParameterizedMessage("unexpected handshake failure with certificate [{}] [{}]",
                    trustedCert.certificate.getSubjectDN(), trustedCert.certificate.getSubjectAlternativeNames()), ex);
            fail("handshake should have been successful, but failed with " + ex);
        }
    }

    public void testCertificateWithUntrustedNameFails() throws Exception {
        writeRestrictions("*.trusted");
        try {
            tryConnect(untrustedCert);
            fail("handshake should have failed, but was successful");
        } catch (SSLHandshakeException | SocketException ex) {
            // expected
        }
    }

    public void testRestrictionsAreReloaded() throws Exception {
        writeRestrictions("*");
        assertBusy(() -> {
            try {
                tryConnect(untrustedCert);
            } catch (SSLHandshakeException | SocketException ex) {
                fail("handshake should have been successful, but failed with " + ex);
            }
        }, MAX_WAIT_RELOAD.millis(), TimeUnit.MILLISECONDS);

        writeRestrictions("*.trusted");
        assertBusy(() -> {
            try {
                tryConnect(untrustedCert);
                fail("handshake should have failed, but was successful");
            } catch (SSLHandshakeException | SocketException ex) {
                // expected
            }
        }, MAX_WAIT_RELOAD.millis(), TimeUnit.MILLISECONDS);
    }

    private void tryConnect(CertificateInfo certificate) throws Exception {
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.ssl.key", certificate.getKeyPath())
                .put("xpack.ssl.certificate", certificate.getCertPath())
                .putList("xpack.ssl.certificate_authorities", ca.getCertPath().toString())
                .put("xpack.ssl.verification_mode", "certificate")
                .build();

        String node = randomFrom(internalCluster().getNodeNames());
        SSLService sslService = new SSLService(settings, TestEnvironment.newEnvironment(settings));
        SSLSocketFactory sslSocketFactory = sslService.sslSocketFactory(settings);
        TransportAddress address = internalCluster().getInstance(Transport.class, node).boundAddress().publishAddress();
        try (SSLSocket socket = (SSLSocket) sslSocketFactory.createSocket(address.getAddress(), address.getPort())) {
            assertThat(socket.isConnected(), is(true));
            // The test simply relies on this (synchronously) connecting (or not), so we don't need a handshake handler
            socket.startHandshake();
        }
    }


    private static CertificateInfo generateCertificate(String name, String san) throws Exception {
        final KeyPair keyPair = CertUtils.generateKeyPair(KEYSIZE);
        final X500Principal principal = new X500Principal("cn=" + name);
        final GeneralNames altNames = new GeneralNames(CertUtils.createCommonName(san));
        final X509Certificate cert = generateSignedCertificate(principal, altNames, keyPair, ca.getCertificate(), ca.getKey(), 30);
        return writeCertificates(name, keyPair.getPrivate(), cert);
    }

    private static CertificateInfo writeCertificates(String name, PrivateKey key, X509Certificate cert) throws IOException {
        final Path keyPath = writePem(key, name + ".key");
        final Path certPath = writePem(cert, name + ".crt");
        return new CertificateInfo(key, keyPath, cert, certPath);
    }

    private static Path writePem(Object obj, String filename) throws IOException {
        Path path = configPath.resolve(filename);
        Files.deleteIfExists(path);
        try (BufferedWriter out = Files.newBufferedWriter(path);
             JcaPEMWriter pemWriter = new JcaPEMWriter(out)) {
            pemWriter.writeObject(obj);
        }
        return path;
    }

    private static class CertificateInfo {
        private final PrivateKey key;
        private final Path keyPath;
        private final X509Certificate certificate;
        private final Path certPath;

        private CertificateInfo(PrivateKey key, Path keyPath, X509Certificate certificate, Path certPath) {
            this.key = key;
            this.keyPath = keyPath;
            this.certificate = certificate;
            this.certPath = certPath;
        }

        private PrivateKey getKey() {
            return key;
        }

        private Path getKeyPath() {
            return keyPath;
        }

        private X509Certificate getCertificate() {
            return certificate;
        }

        private Path getCertPath() {
            return certPath;
        }
    }
}
