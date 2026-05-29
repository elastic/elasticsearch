/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.http.HttpHost;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOSession;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link SchemeIOSessionStrategy} whose underlying {@link SSLIOSessionStrategy} can be swapped
 * in place by {@link #setDelegate(SSLIOSessionStrategy)} without rebuilding the surrounding
 * Apache HC stack, plus a rotation-epoch counter so connections established under a previous
 * delegate can be identified and retired by {@link RotationAwareReuseStrategy}.
 */
final class ReloadableSchemeIoSessionStrategy implements SchemeIOSessionStrategy {

    private static final Logger logger = LogManager.getLogger(ReloadableSchemeIoSessionStrategy.class);

    /**
     * {@link IOSession} attribute carrying the rotation epoch at which a given connection was
     * upgraded to TLS. Read by {@link RotationAwareReuseStrategy} to decide whether the
     * connection is still cryptographically current. Package-private so the reuse strategy can
     * reference the same key.
     */
    static final String ROTATION_EPOCH_ATTR = "es.workload_identity.tls.rotation_epoch";

    /**
     * Atomic snapshot pairing the published delegate with the epoch at which it was published.
     * The delegate is {@code null} until the first {@link #setDelegate(SSLIOSessionStrategy)},
     * which the plugin sequences via {@link WorkloadIdentitySslConfig#start()} before any TLS
     * upgrade can be dispatched. Epoch wraparound is irrelevant: the reuse strategy compares
     * with {@code !=}, not {@code <}.
     */
    private final AtomicReference<State> state;

    ReloadableSchemeIoSessionStrategy() {
        this.state = new AtomicReference<>(new State(null, 0));
    }

    /**
     * Publish a new underlying strategy and advance the rotation epoch so existing connections
     * stamped at the previous epoch can be drained by {@link RotationAwareReuseStrategy}.
     */
    void setDelegate(SSLIOSessionStrategy next) {
        state.updateAndGet(prev -> new State(next, prev.epoch + 1));
    }

    /**
     * @return the currently-published delegate. Exposed for tests that need to observe the swap;
     *         not used by Apache HC internals.
     */
    SSLIOSessionStrategy getDelegate() {
        return state.get().delegate;
    }

    /**
     * @return the current rotation epoch, i.e., the epoch at which any new TLS handshake going
     *         through this strategy would be stamped.
     */
    int currentEpoch() {
        return state.get().epoch;
    }

    @Override
    public boolean isLayeringRequired() {
        return true;
    }

    @Override
    public IOSession upgrade(HttpHost host, IOSession ioSession) throws IOException {
        final State captured = state.get();
        if (captured.delegate == null) {
            throw new IllegalStateException("ReloadableSchemeIoSessionStrategy upgrade() called before initial setDelegate()");
        }
        final IOSession upgraded = captured.delegate.upgrade(host, ioSession);
        upgraded.setAttribute(ROTATION_EPOCH_ATTR, captured.epoch);
        logger.debug("stamped new workload-identity TLS connection to [{}] at rotation epoch [{}]", host, captured.epoch);
        return upgraded;
    }

    private record State(SSLIOSessionStrategy delegate, int epoch) {}
}
