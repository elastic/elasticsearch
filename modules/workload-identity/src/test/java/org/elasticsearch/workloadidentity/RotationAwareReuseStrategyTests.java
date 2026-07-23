/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.http.HttpResponse;
import org.apache.http.nio.conn.ManagedNHttpClientConnection;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOSession;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RotationAwareReuseStrategyTests extends ESTestCase {

    public void testFallbackVetoesReuseRegardlessOfEpoch() {
        final ReloadableSchemeIoSessionStrategy wrapper = new ReloadableSchemeIoSessionStrategy();
        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(wrapper, (response, context) -> false);

        // Even with a context that would otherwise pass (matching epoch), the fallback's veto
        // wins. This protects HC's framing-level guarantees: Connection: close, HTTP/1.0
        // without explicit keep-alive, etc.
        final HttpContext context = contextWithStampedConnection(wrapper.currentEpoch());

        assertFalse(strategy.keepAlive(mock(HttpResponse.class), context));
    }

    public void testMatchingEpochKeepsConnectionAlive() {
        final ReloadableSchemeIoSessionStrategy wrapper = new ReloadableSchemeIoSessionStrategy();
        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(wrapper, (response, context) -> true);

        wrapper.setDelegate(mock(SSLIOSessionStrategy.class)); // epoch -> 1
        final HttpContext context = contextWithStampedConnection(wrapper.currentEpoch());

        assertTrue("connection stamped at current epoch must be reused", strategy.keepAlive(mock(HttpResponse.class), context));
    }

    public void testStaleEpochClosesConnection() {
        final ReloadableSchemeIoSessionStrategy wrapper = new ReloadableSchemeIoSessionStrategy();
        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(wrapper, (response, context) -> true);

        // Stamp the connection at the construction-time epoch (0), then advance the wrapper to
        // simulate a rotation that happened while the request was in flight.
        final HttpContext context = contextWithStampedConnection(wrapper.currentEpoch());
        wrapper.setDelegate(mock(SSLIOSessionStrategy.class)); // epoch -> 1

        assertFalse(
            "connection stamped at a pre-rotation epoch must be closed even when the fallback would reuse it",
            strategy.keepAlive(mock(HttpResponse.class), context)
        );
    }

    public void testMissingManagedConnectionAttributeKeepsAliveByDefault() {
        final ReloadableSchemeIoSessionStrategy wrapper = new ReloadableSchemeIoSessionStrategy();
        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(wrapper, (response, context) -> true);

        // A context without HTTP_CONNECTION should never happen for a real exchange, but the
        // strategy must not be the one that breaks reuse when HC's contract is unmet.
        assertTrue(strategy.keepAlive(mock(HttpResponse.class), new BasicHttpContext()));
    }

    public void testUnstampedConnectionKeepsAliveByDefault() {
        final ReloadableSchemeIoSessionStrategy wrapper = new ReloadableSchemeIoSessionStrategy();
        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(wrapper, (response, context) -> true);

        // A connection that we did not stamp (e.g. plain HTTP, or somehow upgraded via a path
        // that bypassed our wrapper) must be left to the fallback's judgment, which here says
        // "reuse". This strategy's job is rotation-aware retirement, not connection policing.
        final ManagedNHttpClientConnection conn = mock(ManagedNHttpClientConnection.class);
        final IOSession session = mock(IOSession.class);
        when(conn.getIOSession()).thenReturn(session);
        when(session.getAttribute(ReloadableSchemeIoSessionStrategy.ROTATION_EPOCH_ATTR)).thenReturn(null);

        final HttpContext context = new BasicHttpContext();
        context.setAttribute(HttpCoreContext.HTTP_CONNECTION, conn);

        assertTrue(strategy.keepAlive(mock(HttpResponse.class), context));
    }

    public void testStampAttributeOfWrongTypeKeepsAliveByDefault() {
        final ReloadableSchemeIoSessionStrategy wrapper = new ReloadableSchemeIoSessionStrategy();
        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(wrapper, (response, context) -> true);

        final ManagedNHttpClientConnection conn = mock(ManagedNHttpClientConnection.class);
        final IOSession session = mock(IOSession.class);
        when(conn.getIOSession()).thenReturn(session);
        // Defensive: the attribute key is package-shared, but in principle some other component
        // could write a non-Integer there. We must not throw and must not disrupt reuse.
        when(session.getAttribute(ReloadableSchemeIoSessionStrategy.ROTATION_EPOCH_ATTR)).thenReturn("not-an-integer");

        final HttpContext context = new BasicHttpContext();
        context.setAttribute(HttpCoreContext.HTTP_CONNECTION, conn);

        assertTrue(strategy.keepAlive(mock(HttpResponse.class), context));
    }

    /**
     * Build an {@link HttpContext} carrying a mocked {@link ManagedNHttpClientConnection} whose
     * {@link IOSession} reports {@code stampedEpoch} for
     * {@link ReloadableSchemeIoSessionStrategy#ROTATION_EPOCH_ATTR}.
     */
    private static HttpContext contextWithStampedConnection(int stampedEpoch) {
        final ManagedNHttpClientConnection conn = mock(ManagedNHttpClientConnection.class);
        final IOSession session = mock(IOSession.class);
        when(conn.getIOSession()).thenReturn(session);
        when(session.getAttribute(ReloadableSchemeIoSessionStrategy.ROTATION_EPOCH_ATTR)).thenReturn(stampedEpoch);
        final HttpContext context = new BasicHttpContext();
        context.setAttribute(HttpCoreContext.HTTP_CONNECTION, conn);
        return context;
    }
}
