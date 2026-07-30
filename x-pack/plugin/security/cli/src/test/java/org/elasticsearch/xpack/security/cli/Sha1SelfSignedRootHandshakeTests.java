/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli;

import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Reproduces the TLS handshake failure reported against Elasticsearch 9.3.x (bundled JDK 26):
 * a certificate chain whose only SHA-1-signed member is a self-signed root CA is rejected by
 * the default {@code SunX509} key manager, even though such a root is never itself
 * cryptographically verified during normal chain validation.
 * <p>
 * This is caused by <a href="https://bugs.openjdk.org/browse/JDK-8359956">JDK-8359956</a>, which
 * made {@code SunX509} (the key manager {@link KeyManagerFactory#getDefaultAlgorithm()} resolves
 * to, and the one Elasticsearch uses for its keystores) check every certificate it is about to
 * present -- including self-signed roots -- against the signature algorithms the peer advertises.
 * A modern TLS client (OpenSSL 3.x / Node 18+, and hence current Kibana) no longer advertises
 * SHA-1, so Elasticsearch can no longer find a certificate alias it is allowed to present and the
 * handshake aborts.
 */
public class Sha1SelfSignedRootHandshakeTests extends ESTestCase {

    private static final char[] PASSWORD = "keystore-password".toCharArray();

    // Simulates a modern TLS client (OpenSSL 3.x / Node.js 18+) that no longer offers SHA-1
    // as an acceptable certificate signature algorithm.
    private static final String MODERN_CLIENT_SIGNATURE_SCHEMES = String.join(
        ",",
        "rsa_pkcs1_sha256",
        "rsa_pkcs1_sha384",
        "rsa_pkcs1_sha512",
        "rsa_pss_rsae_sha256",
        "rsa_pss_rsae_sha384",
        "rsa_pss_rsae_sha512"
    );

    public void testHandshakeFailsWhenChainContainsSha1SignedSelfSignedRoot() throws Exception {
        assumeTrue(
            "SunX509 only checks the presented chain against peer signature algorithms as of JDK 26 (JDK-8359956)",
            Runtime.version().feature() >= 26
        );

        var certs = generateChain();
        var serverKeyStore = buildKeyStore(new X509Certificate[] { certs.leaf(), certs.root() }, certs.leafKeyPair());

        SSLHandshakeException failure = attemptHandshake(serverKeyStore, certs.root());
        assertThat(failure, notNullValue());
    }

    public void testHandshakeSucceedsWhenCertCheckingIsDisabled() throws Exception {
        assumeTrue("jdk.tls.SunX509KeyManager.certChecking only exists as of JDK 26 (JDK-8359956)", Runtime.version().feature() >= 26);

        var certs = generateChain();
        var serverKeyStore = buildKeyStore(new X509Certificate[] { certs.leaf(), certs.root() }, certs.leafKeyPair());

        String previous = System.getProperty("jdk.tls.SunX509KeyManager.certChecking");
        System.setProperty("jdk.tls.SunX509KeyManager.certChecking", "false");
        try {
            SSLHandshakeException failure = attemptHandshake(serverKeyStore, certs.root());
            assertThat(failure, nullValue());
        } finally {
            if (previous == null) {
                System.clearProperty("jdk.tls.SunX509KeyManager.certChecking");
            } else {
                System.setProperty("jdk.tls.SunX509KeyManager.certChecking", previous);
            }
        }
    }

    public void testHandshakeSucceedsWhenRootIsNotPresented() throws Exception {
        // A TLS server has no need to present its own trust anchor -- the peer already trusts it
        // out of band. Leaving the root out of the configured chain avoids the SunX509 check
        // entirely, on any JDK version.
        var certs = generateChain();
        var serverKeyStore = buildKeyStore(new X509Certificate[] { certs.leaf() }, certs.leafKeyPair());

        SSLHandshakeException failure = attemptHandshake(serverKeyStore, certs.root());
        assertThat(failure, nullValue());
    }

    /**
     * Attempts a loopback TLS handshake where the server presents {@code serverKeyStore} (built
     * via {@link KeyManagerFactory#getDefaultAlgorithm()}, exactly as Elasticsearch does) and the
     * client trusts {@code trustedRoot} but restricts itself to modern signature algorithms.
     *
     * @return the client-side handshake failure, or {@code null} if the handshake succeeded.
     */
    private SSLHandshakeException attemptHandshake(KeyStore serverKeyStore, X509Certificate trustedRoot) throws Exception {
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(serverKeyStore, PASSWORD);

        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(keyManagerFactory.getKeyManagers(), null, null);

        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("root", trustedRoot);
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        SSLContext clientContext = SSLContext.getInstance("TLS");
        clientContext.init(null, trustManagerFactory.getTrustManagers(), null);

        String previousClientSchemes = System.getProperty("jdk.tls.client.SignatureSchemes");
        System.setProperty("jdk.tls.client.SignatureSchemes", MODERN_CLIENT_SIGNATURE_SCHEMES);
        try (
            SSLServerSocket serverSocket = (SSLServerSocket) serverContext.getServerSocketFactory()
                .createServerSocket(0, 1, InetAddress.getLoopbackAddress())
        ) {

            AtomicReference<Exception> serverFailure = new AtomicReference<>();
            Thread serverThread = new Thread(() -> {
                try (SSLSocket accepted = (SSLSocket) serverSocket.accept()) {
                    accepted.startHandshake();
                } catch (Exception e) {
                    serverFailure.set(e);
                }
            });
            serverThread.start();

            try (
                SSLSocket clientSocket = (SSLSocket) clientContext.getSocketFactory()
                    .createSocket(InetAddress.getLoopbackAddress(), serverSocket.getLocalPort())
            ) {
                clientSocket.startHandshake();
                return null;
            } catch (SSLHandshakeException e) {
                return e;
            } finally {
                serverThread.join(TimeUnit.SECONDS.toMillis(10));
            }
        } finally {
            if (previousClientSchemes == null) {
                System.clearProperty("jdk.tls.client.SignatureSchemes");
            } else {
                System.setProperty("jdk.tls.client.SignatureSchemes", previousClientSchemes);
            }
        }
    }

    private KeyStore buildKeyStore(X509Certificate[] chain, KeyPair leafKeyPair) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("node", leafKeyPair.getPrivate(), PASSWORD, chain);
        return keyStore;
    }

    private CertChain generateChain() throws Exception {
        KeyPair rootKeyPair = CertGenUtils.generateKeyPair(2048);
        KeyPair leafKeyPair = CertGenUtils.generateKeyPair(2048);

        // Self-signed root, signed with SHA-1 -- e.g. an old enterprise root CA that predates the
        // move away from SHA-1. Its own signature is never verified during normal chain
        // validation (it is the trust anchor), but SunX509 checks it anyway as of JDK 26.
        X509Certificate root = CertGenUtils.generateSignedCertificate(
            new X500Principal("CN=Test Self-Signed Root CA"),
            null,
            rootKeyPair,
            null,
            null,
            true,
            3650,
            "SHA1withRSA",
            null,
            Set.of()
        );

        // Leaf certificate, signed by the root with SHA-256, matching modern issuance practice.
        X509Certificate leaf = CertGenUtils.generateSignedCertificate(
            new X500Principal("CN=node.example.com"),
            null,
            leafKeyPair,
            root,
            rootKeyPair.getPrivate(),
            false,
            3650,
            "SHA256withRSA",
            null,
            Set.of()
        );

        return new CertChain(root, leaf, leafKeyPair);
    }

    private record CertChain(X509Certificate root, X509Certificate leaf, KeyPair leafKeyPair) {}
}
