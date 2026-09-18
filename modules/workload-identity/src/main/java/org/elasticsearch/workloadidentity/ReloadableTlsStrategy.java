/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.net.NamedEndpoint;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.reactor.ssl.TransportSecurityLayer;
import org.apache.hc.core5.util.Timeout;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link TlsStrategy} whose underlying {@link DefaultClientTlsStrategy} can be swapped in place
 * by {@link #setDelegate(DefaultClientTlsStrategy)} without rebuilding the surrounding Apache HC
 * stack. A rotation-epoch counter is maintained so callers can observe that a swap occurred.
 *
 * <p>After each TLS handshake this strategy stamps the delegate instance that established the
 * connection on the {@link javax.net.ssl.SSLSession} under {@link #SESSION_KEY}. The stamp is
 * read by {@link RotationAwareReuseStrategy} to detect connections established before a cert
 * rotation and retire them after their next response.
 */
final class ReloadableTlsStrategy implements TlsStrategy {

    static final String SESSION_KEY = "workload-identity.tls-strategy";

    private static final Logger logger = LogManager.getLogger(ReloadableTlsStrategy.class);

    /**
     * Atomic snapshot pairing the published delegate with the epoch at which it was published.
     * The delegate is {@code null} until the first {@link #setDelegate(DefaultClientTlsStrategy)}.
     */
    private final AtomicReference<State> state;

    ReloadableTlsStrategy() {
        this.state = new AtomicReference<>(new State(null, 0));
    }

    /**
     * Publish a new underlying strategy and advance the rotation epoch.
     */
    void setDelegate(DefaultClientTlsStrategy next) {
        state.updateAndGet(prev -> new State(next, prev.epoch + 1));
    }

    /**
     * @return the currently-published delegate; {@code null} before the first {@link #setDelegate}.
     */
    DefaultClientTlsStrategy getDelegate() {
        return state.get().delegate;
    }

    /**
     * @return the current rotation epoch.
     */
    int currentEpoch() {
        return state.get().epoch;
    }

    @Deprecated
    @Override
    public boolean upgrade(
        TransportSecurityLayer tlsSession,
        HttpHost host,
        SocketAddress localAddress,
        SocketAddress remoteAddress,
        Object attachment,
        Timeout handshakeTimeout
    ) {
        upgrade(tlsSession, host, attachment, handshakeTimeout, null);
        return true;
    }

    @Override
    public void upgrade(
        TransportSecurityLayer tlsSession,
        NamedEndpoint endpoint,
        Object attachment,
        Timeout handshakeTimeout,
        FutureCallback<TransportSecurityLayer> callback
    ) {
        final State captured = state.get();
        if (captured.delegate == null) {
            throw new IllegalStateException("ReloadableTlsStrategy upgrade() called before initial setDelegate()");
        }
        logger.debug("new workload-identity TLS connection to [{}] at rotation epoch [{}]", endpoint, captured.epoch);
        captured.delegate.upgrade(tlsSession, endpoint, attachment, handshakeTimeout, new FutureCallback<>() {
            @Override
            public void completed(TransportSecurityLayer result) {
                final TlsDetails tlsDetails = result.getTlsDetails();
                if (tlsDetails != null) {
                    tlsDetails.getSSLSession().putValue(SESSION_KEY, captured.epoch);
                }
                if (callback != null) {
                    callback.completed(result);
                }
            }

            @Override
            public void failed(Exception ex) {
                if (callback != null) {
                    callback.failed(ex);
                }
            }

            @Override
            public void cancelled() {
                if (callback != null) {
                    callback.cancelled();
                }
            }
        });
    }

    private record State(DefaultClientTlsStrategy delegate, int epoch) {}
}
