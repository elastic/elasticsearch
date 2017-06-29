/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Time;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.network.InetAddressHelper;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.transport.Transport;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for SSL reloading support
 */
public class SSLReloadIntegTests extends SecurityIntegTestCase {

    private Path nodeStorePath;

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        if (nodeStorePath == null) {
            Path origPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
            Path tempDir = createTempDir();
            nodeStorePath = tempDir.resolve("testnode.jks");
            try {
                Files.copy(origPath, nodeStorePath);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to copy keystore");
            }
        }
        Settings settings = super.nodeSettings(nodeOrdinal);
        Settings.Builder builder = Settings.builder()
                .put(settings.filter((s) -> s.startsWith("xpack.ssl.") == false));


        SecuritySettingsSource.addSSLSettingsForStore(builder,
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks", "testnode");
        builder.put("resource.reload.interval.high", "1s")
                .put("xpack.ssl.keystore.path", nodeStorePath);

        if (builder.get("xpack.ssl.truststore.path") != null) {
            builder.put("xpack.ssl.truststore.path", nodeStorePath);
        }

        return builder.build();
    }

    @Override
    protected boolean useGeneratedSSLConfig() {
        return false;
    }

    public void testThatSSLConfigurationReloadsOnModification() throws Exception {
        KeyPair keyPair = CertUtils.generateKeyPair(randomFrom(1024, 2048));
        X509Certificate certificate = getCertificate(keyPair);
        KeyStore keyStore = KeyStore.getInstance("jks");
        keyStore.load(null, null);
        keyStore.setKeyEntry("key", keyPair.getPrivate(), SecuritySettingsSource.TEST_PASSWORD.toCharArray(),
                new Certificate[] { certificate });
        Path keystorePath = createTempDir().resolve("newcert.jks");
        try (OutputStream out = Files.newOutputStream(keystorePath)) {
            keyStore.store(out, SecuritySettingsSource.TEST_PASSWORD.toCharArray());
        }
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", SecuritySettingsSource.TEST_PASSWORD);
        secureSettings.setString("xpack.ssl.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.ssl.keystore.path", keystorePath)
                .put("xpack.ssl.truststore.path", nodeStorePath)
                .setSecureSettings(secureSettings)
                .build();
        String node = randomFrom(internalCluster().getNodeNames());
        SSLService sslService = new SSLService(settings, new Environment(settings));
        SSLSocketFactory sslSocketFactory = sslService.sslSocketFactory(settings);
        TransportAddress address = internalCluster()
                .getInstance(Transport.class, node).boundAddress().publishAddress();
        try (SSLSocket socket = (SSLSocket) sslSocketFactory.createSocket(address.getAddress(), address.getPort())) {
            assertThat(socket.isConnected(), is(true));
            socket.startHandshake();
            fail("handshake should not have been successful!");
        } catch (SSLHandshakeException | SocketException expected) {
            logger.trace("expected exception", expected);
        }

        KeyStore nodeStore = KeyStore.getInstance("jks");
        try (InputStream in = Files.newInputStream(nodeStorePath)) {
            nodeStore.load(in, "testnode".toCharArray());
        }
        nodeStore.setCertificateEntry("newcert", certificate);
        Path path = nodeStorePath.getParent().resolve("updated.jks");
        try (OutputStream out = Files.newOutputStream(path)) {
            nodeStore.store(out, "testnode".toCharArray());
        }
        try {
            Files.move(path, nodeStorePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(path, nodeStorePath, StandardCopyOption.REPLACE_EXISTING);
        }

        CountDownLatch latch = new CountDownLatch(1);
        assertBusy(() -> {
            try (SSLSocket socket = (SSLSocket) sslSocketFactory.createSocket(address.getAddress(), address.getPort())) {
                logger.info("opened socket for reloading [{}]", socket);
                socket.addHandshakeCompletedListener(event -> {
                    try {
                        assertThat(event.getPeerPrincipal().getName(), containsString("Test Node"));
                        logger.info("ssl handshake completed on port [{}]", event.getSocket().getLocalPort());
                        latch.countDown();
                    } catch (Exception e) {
                        fail("caught exception in listener " + e.getMessage());
                    }
                });
                socket.startHandshake();

            } catch (Exception e) {
                fail("caught exception " + e.getMessage());
            }
        });
        latch.await();
    }

    private X509Certificate getCertificate(KeyPair keyPair) throws Exception {
        final DateTime notBefore = new DateTime(DateTimeZone.UTC);
        final DateTime notAfter = notBefore.plusYears(1);
        X500Name subject = new X500Name("CN=random cert");
        JcaX509v3CertificateBuilder builder =
                new JcaX509v3CertificateBuilder(subject, CertUtils.getSerial(),
                        new Time(notBefore.toDate(), Locale.ROOT), new Time(notAfter.toDate(), Locale.ROOT), subject, keyPair.getPublic());

        JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();
        builder.addExtension(Extension.subjectKeyIdentifier, false, extUtils.createSubjectKeyIdentifier(keyPair.getPublic()));
        builder.addExtension(Extension.authorityKeyIdentifier, false, extUtils.createAuthorityKeyIdentifier(keyPair.getPublic()));
        builder.addExtension(Extension.subjectAlternativeName, false,
                CertUtils.getSubjectAlternativeNames(true, Sets.newHashSet(InetAddressHelper.getAllAddresses())));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate());
        X509CertificateHolder certificateHolder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(certificateHolder);
    }
}
