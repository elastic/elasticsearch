/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.shield.Security;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.transport.Transport;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests for SSL reloading support
 */
public class SSLReloadIntegTests extends ShieldIntegTestCase {

    private Path nodeStorePath;

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        if (nodeStorePath == null) {
            Path origPath = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks");
            Path tempDir = createTempDir();
            nodeStorePath = tempDir.resolve("testnode.jks");
            try {
                Files.copy(origPath, nodeStorePath);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to copy keystore");
            }
        }
        Settings settings = super.nodeSettings(nodeOrdinal);
        Settings.Builder builder = Settings.builder();
        for (Entry<String, String> entry : settings.getAsMap().entrySet()) {
            if (entry.getKey().startsWith(Security.setting("ssl.")) == false) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }

        builder.put("resource.reload.interval.high", "1s")
                .put(ShieldSettingsSource.getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks",
                        "testnode"))
                .put("xpack.security.ssl.keystore.path", nodeStorePath);

        if (builder.get("xpack.security.ssl.truststore.path") != null) {
            builder.put("xpack.security.ssl.truststore.path", nodeStorePath);
        }

        return builder.build();
    }

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    @Override
    protected boolean autoSSLEnabled() {
        return false;
    }

    public void testThatSSLConfigurationReloadsOnModification() throws Exception {
        KeyPair keyPair = CertUtils.generateKeyPair();
        X509Certificate certificate = getCertificate(keyPair);
        KeyStore keyStore = KeyStore.getInstance("jks");
        keyStore.load(null, null);
        keyStore.setKeyEntry("key", keyPair.getPrivate(), "changeme".toCharArray(), new Certificate[] { certificate });
        Path keystorePath = createTempDir().resolve("newcert.jks");
        try (OutputStream out = Files.newOutputStream(keystorePath)) {
            keyStore.store(out, "changeme".toCharArray());
        }

        Settings settings = Settings.builder()
                .put("keystore.path", keystorePath)
                .put("keystore.password", "changeme")
                .put("truststore.path", nodeStorePath)
                .put("truststore.password", "testnode")
                .build();
        String node = randomFrom(internalCluster().getNodeNames());
        ServerSSLService sslService = internalCluster().getInstance(ServerSSLService.class, node);
        SSLSocketFactory sslSocketFactory = sslService.sslSocketFactory(settings);
        InetSocketTransportAddress address = (InetSocketTransportAddress) internalCluster()
                .getInstance(Transport.class, node).boundAddress().publishAddress();
        try (SSLSocket socket = (SSLSocket) sslSocketFactory.createSocket(address.getAddress(), address.getPort())) {
            socket.startHandshake();
            fail("handshake should not have been successful!");
        } catch (SSLHandshakeException expected) {
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
