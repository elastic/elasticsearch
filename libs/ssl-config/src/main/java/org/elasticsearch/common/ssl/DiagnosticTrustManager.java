/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.ssl;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509ExtendedTrustManager;
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

import static org.elasticsearch.common.ssl.SslDiagnostics.getTrustDiagnosticFailure;

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
            .collect(Collectors.toMap(cert -> cert.getSubjectX500Principal().getName(), List::of,
                (List<X509Certificate> a, List<X509Certificate> b) -> {
                    final ArrayList<X509Certificate> list = new ArrayList<>(a.size() + b.size());
                    list.addAll(a);
                    list.addAll(b);
                    return list;
                }));
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
        final String diagnostic = getTrustDiagnosticFailure(chain, peerType, session, this.contextName.get(), this.issuers);
        logger.warning(diagnostic, cause);
    }

    private SSLSession session(Socket socket) {
        if (socket instanceof SSLSocket) {
            final SSLSocket ssl = (SSLSocket) socket;
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

    private SSLSession session(SSLEngine engine) {
        return engine.getHandshakeSession();
    }
}
