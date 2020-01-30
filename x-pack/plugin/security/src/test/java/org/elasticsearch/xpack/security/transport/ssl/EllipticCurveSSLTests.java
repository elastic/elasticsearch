/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.ssl;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.junit.BeforeClass;

import javax.net.ssl.HandshakeCompletedEvent;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivateKey;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class EllipticCurveSSLTests extends SecurityIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Path keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/prime256v1-key.pem");
        final Path certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/prime256v1-cert.pem");
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal).filter(s -> s.startsWith("xpack.security.transport.ssl") == false))
                .put("xpack.security.transport.ssl.enabled", true)
                .put("xpack.security.transport.ssl.key", keyPath)
                .put("xpack.security.transport.ssl.certificate", certPath)
                .put("xpack.security.transport.ssl.certificate_authorities", certPath)
                // disable hostname verificate since these certs aren't setup for that
                .put("xpack.security.transport.ssl.verification_mode", "certificate")
                .build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    public void testConnection() throws Exception {
        assumeFalse("Fails on BCTLS with 'Closed engine without receiving the close alert message.'", inFipsJvm());
        final Path keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/prime256v1-key.pem");
        final Path certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/prime256v1-cert.pem");
        PrivateKey privateKey = PemUtils.readPrivateKey(keyPath, () -> null);
        Certificate[] certs = CertParsingUtils.readCertificates(Collections.singletonList(certPath.toString()), newEnvironment());
        X509ExtendedKeyManager x509ExtendedKeyManager = CertParsingUtils.keyManager(certs, privateKey, new char[0]);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(new X509ExtendedKeyManager[] { x509ExtendedKeyManager },
            new TrustManager[]{CertParsingUtils.trustManager(CertParsingUtils.readCertificates(Collections.singletonList(certPath)))},
            new SecureRandom());
        SSLSocketFactory socketFactory = sslContext.getSocketFactory();
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().setTransport(true).get();
        TransportAddress address = randomFrom(response.getNodes()).getTransport().getAddress().publishAddress();

        final CountDownLatch latch = new CountDownLatch(1);
        try (SSLSocket sslSocket = AccessController.doPrivileged(new PrivilegedExceptionAction<SSLSocket>() {
              @Override
              public SSLSocket run() throws Exception {
                  return (SSLSocket) socketFactory.createSocket(address.address().getAddress(), address.address().getPort());
              }})) {
            final AtomicReference<HandshakeCompletedEvent> reference = new AtomicReference<>();
            sslSocket.addHandshakeCompletedListener((event) -> {
                reference.set(event);
                latch.countDown();
            });
            sslSocket.startHandshake();
            latch.await();

            HandshakeCompletedEvent event = reference.get();
            assertNotNull(event);
            SSLSession session = event.getSession();
            Certificate[] peerChain = session.getPeerCertificates();
            assertEquals(1, peerChain.length);
            assertEquals(certs[0], peerChain[0]);
            assertThat(session.getCipherSuite(),
                anyOf(containsString("ECDSA"), is("TLS_AES_256_GCM_SHA384"), is("TLS_AES_128_GCM_SHA256")));
        }
    }

    @BeforeClass
    public static void assumeECDSACiphersSupported() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(null, null, null);
        SSLEngine sslEngine = sslContext.createSSLEngine();
        assumeTrue("ECDSA ciphers must be supported for this test to run. Enabled ciphers: " +
                        Arrays.toString(sslEngine.getEnabledCipherSuites()) + ", supported ciphers: " +
                        Arrays.toString(sslEngine.getSupportedCipherSuites()),
                Arrays.stream(sslEngine.getEnabledCipherSuites()).anyMatch(s -> s.contains("ECDSA")));
    }
}
