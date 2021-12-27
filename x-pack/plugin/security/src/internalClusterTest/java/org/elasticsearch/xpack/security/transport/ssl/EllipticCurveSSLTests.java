/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.ssl;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.HandshakeCompletedEvent;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class EllipticCurveSSLTests extends SecurityIntegTestCase {
    private static String CURVE;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Path keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/private_" + CURVE + ".pem");
        final Path certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/certificate_" + CURVE + ".pem");
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings).filter(s -> s.startsWith("xpack.security.transport.ssl") == false))
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
        final Path keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/private_" + CURVE + ".pem");
        final Path certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/certificate_" + CURVE + ".pem");
        final X509ExtendedKeyManager x509ExtendedKeyManager = CertParsingUtils.getKeyManagerFromPEM(certPath, keyPath, new char[0]);
        final X509ExtendedTrustManager trustManager = CertParsingUtils.getTrustManagerFromPEM(List.of(certPath));
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(new X509ExtendedKeyManager[] { x509ExtendedKeyManager }, new TrustManager[] { trustManager }, new SecureRandom());
        SSLSocketFactory socketFactory = sslContext.getSocketFactory();
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().setTransport(true).get();
        TransportAddress address = randomFrom(response.getNodes()).getInfo(TransportInfo.class).getAddress().publishAddress();

        final CountDownLatch latch = new CountDownLatch(1);
        try (SSLSocket sslSocket = AccessController.doPrivileged(new PrivilegedExceptionAction<SSLSocket>() {
            @Override
            public SSLSocket run() throws Exception {
                return (SSLSocket) socketFactory.createSocket(address.address().getAddress(), address.address().getPort());
            }
        })) {
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
            assertEquals(CertParsingUtils.readX509Certificate(certPath), peerChain[0]);
            assertThat(
                session.getCipherSuite(),
                anyOf(containsString("ECDSA"), is("TLS_AES_256_GCM_SHA384"), is("TLS_AES_128_GCM_SHA256"))
            );
        }
    }

    @BeforeClass
    public static void assumeECDSACiphersSupported() throws Exception {
        CURVE = randomFrom("secp256r1", "secp384r1", "secp521r1");
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(null, null, null);
        SSLEngine sslEngine = sslContext.createSSLEngine();
        assumeTrue(
            "ECDSA ciphers must be supported for this test to run. Enabled ciphers: "
                + Arrays.toString(sslEngine.getEnabledCipherSuites())
                + ", supported ciphers: "
                + Arrays.toString(sslEngine.getSupportedCipherSuites()),
            Arrays.stream(sslEngine.getEnabledCipherSuites()).anyMatch(s -> s.contains("ECDSA"))
        );
    }
}
