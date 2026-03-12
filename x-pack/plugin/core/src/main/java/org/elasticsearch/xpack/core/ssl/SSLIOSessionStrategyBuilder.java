/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.apache.http.HttpHost;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOSession;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.ssl.SslDiagnostics;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

class SSLIOSessionStrategyBuilder extends AbstractSslBuilder<SSLIOSessionStrategy> {

    public static final SSLIOSessionStrategyBuilder INSTANCE = new SSLIOSessionStrategyBuilder();

    /**
     * This method only exists to simplify testing because {@link SSLIOSessionStrategy} does
     * not expose any of the parameters that you give it.
     */
    @Override
    SSLIOSessionStrategy build(SSLContext sslContext, String[] protocols, String[] ciphers, HostnameVerifier verifier) {
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
