/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.hc.client5.http.impl.DefaultClientConnectionReuseStrategy;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ConnectionReuseStrategy;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import javax.net.ssl.SSLSession;

/**
 * A {@link ConnectionReuseStrategy} that closes connections whose TLS session was established
 * under a previous {@link ReloadableTlsStrategy} delegate (i.e. before a cert rotation), so
 * a continuous request stream cannot pin a connection on stale cryptographic material.
 *
 * <p>The flow at the end of each request-response exchange is:
 * <ol>
 *   <li>Apache HC consults this strategy via {@code keepAlive(request, response, context)}.</li>
 *   <li>We defer to {@link DefaultClientConnectionReuseStrategy} first; if HC's own heuristics
 *       already say "do not reuse" (e.g. {@code Connection: close}, HTTP/1.0 without explicit
 *       keep-alive, framing problems), we honor that and return {@code false}.</li>
 *   <li>Otherwise we read the {@link SSLSession} from the context and check the rotation epoch
 *       stamped on it by {@link ReloadableTlsStrategy} at handshake time.</li>
 *   <li>If the stamped epoch does not match {@link ReloadableTlsStrategy#currentEpoch() the
 *       current epoch}, we return {@code false} so HC closes the connection instead of returning
 *       it to the pool.</li>
 * </ol>
 *
 * <p>Connections that were not stamped (e.g. plain HTTP, although the workload-identity issuer
 * is always HTTPS) or whose session carries no stamp are conservatively kept alive: this strategy
 * must not be the one that breaks reuse for connections it does not own.
 *
 * <p>Reload races are bounded by one RTT: a request whose response races a swap may see "stale"
 * and close eagerly even though its connection's stamp matched the strategy state at request
 * dispatch; the next response on a stamp-current connection catches it normally.
 */
final class RotationAwareReuseStrategy implements ConnectionReuseStrategy {

    private static final Logger logger = LogManager.getLogger(RotationAwareReuseStrategy.class);

    private final ReloadableTlsStrategy tlsStrategy;
    private final ConnectionReuseStrategy fallback;

    RotationAwareReuseStrategy(ReloadableTlsStrategy tlsStrategy) {
        this(tlsStrategy, DefaultClientConnectionReuseStrategy.INSTANCE);
    }

    // Visible for testing
    RotationAwareReuseStrategy(ReloadableTlsStrategy tlsStrategy, ConnectionReuseStrategy fallback) {
        this.tlsStrategy = tlsStrategy;
        this.fallback = fallback;
    }

    @Override
    public boolean keepAlive(HttpRequest request, HttpResponse response, HttpContext context) {
        if (fallback.keepAlive(request, response, context) == false) {
            return false;
        }
        final SSLSession sslSession = HttpClientContext.castOrCreate(context).getSSLSession();
        if (sslSession == null) {
            return true;
        }
        final Object stamped = sslSession.getValue(ReloadableTlsStrategy.SESSION_KEY);
        if (stamped instanceof Integer stampedEpoch) {
            final int currentEpoch = tlsStrategy.currentEpoch();
            if (stampedEpoch != currentEpoch) {
                logger.debug(
                    "retiring workload-identity HTTP connection: stamped epoch [{}] differs from current [{}]",
                    stampedEpoch,
                    currentEpoch
                );
                return false;
            }
        }
        return true;
    }
}
