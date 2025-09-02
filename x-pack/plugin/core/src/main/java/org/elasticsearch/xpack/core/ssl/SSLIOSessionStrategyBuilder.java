/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOSession;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslDiagnostics;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

public class SSLIOSessionStrategyBuilder {

    public static final SSLIOSessionStrategyBuilder INSTANCE = new SSLIOSessionStrategyBuilder();

    public SSLIOSessionStrategy sslIOSessionStrategy(SslConfiguration config, SSLContext sslContext) {
        String[] ciphers = supportedCiphers(sslParameters(sslContext).getCipherSuites(), config.getCipherSuites(), false);
        String[] supportedProtocols = config.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        HostnameVerifier verifier;

        if (config.verificationMode().isHostnameVerificationEnabled()) {
            verifier = SSLIOSessionStrategy.getDefaultHostnameVerifier();
        } else {
            verifier = NoopHostnameVerifier.INSTANCE;
        }

        return sslIOSessionStrategy(sslContext, supportedProtocols, ciphers, verifier);
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

    /**
     * This method only exists to simplify testing because {@link SSLIOSessionStrategy} does
     * not expose any of the parameters that you give it.
     */
    SSLIOSessionStrategy sslIOSessionStrategy(SSLContext sslContext, String[] protocols, String[] ciphers, HostnameVerifier verifier) {
        return new SSLIOSessionStrategy(sslContext, protocols, ciphers, verifier) {
            @Override
            protected void verifySession(HttpHost host, IOSession iosession, SSLSession session) throws SSLException {
                if (verifier.verify(host.getHostName(), session) == false) {
                    final Certificate[] certs = session.getPeerCertificates();
                    final X509Certificate x509 = (X509Certificate) certs[0];
                    final X500Principal x500Principal = x509.getSubjectX500Principal();
                    final String altNames = Strings.collectionToCommaDelimitedString(SslDiagnostics.describeValidHostnames(x509));
                    throw new SSLPeerUnverifiedException(
                        LoggerMessageFormat.format(
                            "Expected SSL certificate to be valid for host [{}],"
                                + " but it is only valid for subject alternative names [{}] and subject [{}]",
                            new Object[] { host.getHostName(), altNames, x500Principal.toString() }
                        )
                    );
                }
            }
        };
    }
}
