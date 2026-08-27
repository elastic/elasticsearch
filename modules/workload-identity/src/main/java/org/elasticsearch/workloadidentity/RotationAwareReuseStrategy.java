/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpResponse;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.nio.conn.ManagedNHttpClientConnection;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

/**
 * A {@link ConnectionReuseStrategy} that closes connections whose TLS session was established
 * under a previous {@link ReloadableSchemeIoSessionStrategy#currentEpoch() rotation epoch}, so
 * a continuous request stream cannot pin a connection on stale cryptographic material.
 *
 * <p>The flow at the end of each request-response exchange is:
 * <ol>
 *   <li>Apache HC consults this strategy via {@code keepAlive(response, context)} (see
 *       {@code AbstractClientExchangeHandler#responseCompleted}).</li>
 *   <li>We defer to {@link DefaultConnectionReuseStrategy} first; if HC's own heuristics
 *       already say "do not reuse" (e.g. {@code Connection: close}, HTTP/1.0 without explicit
 *       keep-alive, framing problems), we honor that and return {@code false}.</li>
 *   <li>Otherwise we look up the underlying {@link ManagedNHttpClientConnection} via the
 *       {@link HttpCoreContext#HTTP_CONNECTION} context attribute, then read the rotation
 *       epoch stamped on its {@link org.apache.http.nio.reactor.IOSession} at
 *       {@link ReloadableSchemeIoSessionStrategy#upgrade upgrade()} time.</li>
 *   <li>If the connection's stamped epoch does not match
 *       {@link ReloadableSchemeIoSessionStrategy#currentEpoch() the current epoch}, we return
 *       {@code false} so HC closes the connection instead of returning it to the pool.</li>
 * </ol>
 *
 * <p>Connections that were not stamped (e.g. plain HTTP, although the workload-identity issuer
 * is always HTTPS) or whose attribute value is not an {@code Integer} are conservatively kept
 * alive: this strategy must not be the one that breaks reuse for connections it does not own.
 *
 * <p>Reload races are bounded by one RTT: a request whose response races a swap may see "stale"
 * and close eagerly even though its connection's stamp matched the strategy state at request
 * dispatch; the next response on a stamp-current connection catches it normally.
 */
final class RotationAwareReuseStrategy implements ConnectionReuseStrategy {

    private static final Logger logger = LogManager.getLogger(RotationAwareReuseStrategy.class);

    private final ConnectionReuseStrategy fallback;
    private final ReloadableSchemeIoSessionStrategy strategy;

    RotationAwareReuseStrategy(ReloadableSchemeIoSessionStrategy strategy) {
        this(strategy, DefaultConnectionReuseStrategy.INSTANCE);
    }

    // Visible-for-testing constructor: allows substituting the fallback so tests can pin its
    // behavior deterministically without spinning up a real HttpResponse with valid framing.
    RotationAwareReuseStrategy(ReloadableSchemeIoSessionStrategy strategy, ConnectionReuseStrategy fallback) {
        this.strategy = strategy;
        this.fallback = fallback;
    }

    @Override
    public boolean keepAlive(HttpResponse response, HttpContext context) {
        if (!fallback.keepAlive(response, context)) {
            return false;
        }
        final Object connObj = context.getAttribute(HttpCoreContext.HTTP_CONNECTION);
        if (connObj instanceof ManagedNHttpClientConnection conn) {
            final Object stamped = conn.getIOSession().getAttribute(ReloadableSchemeIoSessionStrategy.ROTATION_EPOCH_ATTR);
            if (stamped instanceof Integer stampedEpoch) {
                final int currentEpoch = strategy.currentEpoch();
                if (stampedEpoch == currentEpoch) {
                    return true;
                }
                logger.debug(
                    "retiring workload-identity HTTP connection: stamped epoch [{}] differs from current [{}]",
                    stampedEpoch,
                    currentEpoch
                );
                return false;
            }
            // Unstamped connection — not ours to manage, leave it alone (defer to fallback).
            return true;
        }
        // No managed connection in context (defensive) — defer to fallback's "keep".
        return true;
    }
}
