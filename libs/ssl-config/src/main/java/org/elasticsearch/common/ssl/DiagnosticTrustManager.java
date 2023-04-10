/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509ExtendedTrustManager;

public final class DiagnosticTrustManager extends X509ExtendedTrustManager {

    /**
     * This interface exists because the ssl-config library does not depend on log4j, however the whole purpose of this class is to log
     * diagnostic messages, so it must be provided with a function by which it can do that.
     */
    @FunctionalInterface
    public interface DiagnosticLogger {
        void warning(String message, GeneralSecurityException cause);
    }

    private final X509ExtendedTrustManager delegate;
    private final Supplier<String> contextName;
    private final DiagnosticLogger logger;
    private final Map<String, List<X509Certificate>> issuers;

    /**
     * @param contextName The descriptive name of the context that this trust manager is operating in (e.g "xpack.security.http.ssl")
     * @param logger      For uses that depend on log4j, it is recommended that this parameter be equivalent to
     *                    {@code LogManager.getLogger(DiagnosticTrustManager.class)::warn}
     */
    public DiagnosticTrustManager(X509ExtendedTrustManager delegate, Supplier<String> contextName, DiagnosticLogger logger) {
        this.delegate = delegate;
        this.contextName = contextName;
        this.logger = logger;
        this.issuers = Stream.of(delegate.getAcceptedIssuers())
            .collect(
                Collectors.toMap(
                    cert -> cert.getSubjectX500Principal().getName(),
                    List::of,
                    (List<X509Certificate> a, List<X509Certificate> b) -> {
                        final ArrayList<X509Certificate> list = new ArrayList<>(a.size() + b.size());
                        list.addAll(a);
                        list.addAll(b);
                        return list;
                    }
                )
            );
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        try {
            delegate.checkClientTrusted(chain, authType, socket);
        } catch (CertificateException e) {
            diagnose(e, chain, SslDiagnostics.PeerType.CLIENT, session(socket));
            throw e;
        }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        try {
            delegate.checkServerTrusted(chain, authType, socket);
        } catch (CertificateException e) {
            diagnose(e, chain, SslDiagnostics.PeerType.SERVER, session(socket));
            throw e;
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        try {
            delegate.checkClientTrusted(chain, authType, engine);
        } catch (CertificateException e) {
            diagnose(e, chain, SslDiagnostics.PeerType.CLIENT, session(engine));
            throw e;
        }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        try {
            delegate.checkServerTrusted(chain, authType, engine);
        } catch (CertificateException e) {
            diagnose(e, chain, SslDiagnostics.PeerType.SERVER, session(engine));
            throw e;
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        try {
            delegate.checkClientTrusted(chain, authType);
        } catch (CertificateException e) {
            diagnose(e, chain, SslDiagnostics.PeerType.CLIENT, null);
            throw e;
        }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        try {
            delegate.checkServerTrusted(chain, authType);
        } catch (CertificateException e) {
            diagnose(e, chain, SslDiagnostics.PeerType.SERVER, null);
            throw e;
        }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return delegate.getAcceptedIssuers();
    }

    private void diagnose(CertificateException cause, X509Certificate[] chain, SslDiagnostics.PeerType peerType, SSLSession session) {
        final String diagnostic = SslDiagnostics.INSTANCE.getTrustDiagnosticFailure(
            chain,
            peerType,
            session,
            this.contextName.get(),
            this.issuers
        );
        logger.warning(diagnostic, cause);
    }

    private static SSLSession session(Socket socket) {
        if (socket instanceof final SSLSocket ssl) {
            final SSLSession handshakeSession = ssl.getHandshakeSession();
            if (handshakeSession == null) {
                return ssl.getSession();
            } else {
                return handshakeSession;
            }
        } else {
            return null;
        }
    }

    private static SSLSession session(SSLEngine engine) {
        return engine.getHandshakeSession();
    }
}
