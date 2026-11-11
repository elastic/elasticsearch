/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslDiagnostics;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

/**
 * Builds an HTTP client specific SSL object from an {@link SslConfiguration}. Plugins using an HTTP client library that x-pack-core does
 * not depend on can extend this to get the same protocol, cipher and hostname verification handling as {@link SslProfile}.
 */
public abstract class AbstractSslBuilder<T> {

    public T build(SslConfiguration config, SSLContext sslContext) {
        String[] ciphers = supportedCiphers(sslParameters(sslContext).getCipherSuites(), config.getCipherSuites(), false);
        String[] supportedProtocols = config.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        HostnameVerifier verifier;

        if (config.verificationMode().isHostnameVerificationEnabled()) {
            verifier = HttpsSupport.getDefaultHostnameVerifier();
        } else {
            verifier = NoopHostnameVerifier.INSTANCE;
        }

        return build(sslContext, supportedProtocols, ciphers, verifier);
    }

    /**
     * Verifies {@code host} against the peer certificate of {@code session}. On failure, the exception lists the names the certificate
     * is actually valid for, which the default messages of HTTP client libraries do not.
     */
    // TODO: move to monitoring once inference is on HC5 (#157778), remaining callers are HC4 SSLIOSessionStrategy overrides
    public static void verifyHostname(HostnameVerifier verifier, String host, SSLSession session) throws SSLPeerUnverifiedException {
        if (verifier.verify(host, session) == false) {
            final Certificate[] certs = session.getPeerCertificates();
            final X509Certificate x509 = (X509Certificate) certs[0];
            final X500Principal x500Principal = x509.getSubjectX500Principal();
            final String altNames = Strings.collectionToCommaDelimitedString(SslDiagnostics.describeValidHostnames(x509));
            throw new SSLPeerUnverifiedException(
                LoggerMessageFormat.format(
                    "Expected SSL certificate to be valid for host [{}],"
                        + " but it is only valid for subject alternative names [{}] and subject [{}]",
                    new Object[] { host, altNames, x500Principal.toString() }
                )
            );
        }
    }

    /**
     * This method exists to simplify testing
     */
    String[] supportedCiphers(String[] supportedCiphers, List<String> requestedCiphers, boolean log) {
        return SSLService.supportedCiphers(supportedCiphers, requestedCiphers, log);
    }

    /**
     * The {@link SSLParameters} that are associated with the {@code sslContext}.
     * <p>
     * This method exists to simplify testing since {@link SSLContext#getSupportedSSLParameters()} is {@code final}.
     *
     * @param sslContext The SSL context for the current SSL settings
     * @return Never {@code null}.
     */
    SSLParameters sslParameters(SSLContext sslContext) {
        return sslContext.getSupportedSSLParameters();
    }

    protected abstract T build(SSLContext sslContext, String[] protocols, String[] ciphers, HostnameVerifier verifier);
}
