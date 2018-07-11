/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

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
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Collections;
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
        //Node starts with testnode.jks
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
    protected boolean transportSSLEnabled() {
        return true;
    }

    public void testThatSSLConfigurationReloadsOnModification() throws Exception {
        Path keystorePath = createTempDir().resolve("testnode_updated.jks");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.jks"), keystorePath);
        X509Certificate certificate = CertParsingUtils.readX509Certificates(Collections.singletonList(getDataPath
                ("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.crt")))[0];
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        secureSettings.setString("xpack.ssl.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.ssl.keystore.path", keystorePath)
                .put("xpack.ssl.truststore.path", nodeStorePath)
                .setSecureSettings(secureSettings)
                .build();
        String node = randomFrom(internalCluster().getNodeNames());
        SSLService sslService = new SSLService(settings, TestEnvironment.newEnvironment(settings));
        SSLConfiguration sslConfiguration = sslService.getSSLConfiguration("xpack.ssl");
        SSLSocketFactory sslSocketFactory = sslService.sslSocketFactory(sslConfiguration);
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
}
