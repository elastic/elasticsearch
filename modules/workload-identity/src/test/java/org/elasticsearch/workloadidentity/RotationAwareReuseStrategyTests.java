/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.core5.http.ConnectionReuseStrategy;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;

import javax.net.ssl.SSLSession;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RotationAwareReuseStrategyTests extends ESTestCase {

    private static HttpClientContext contextWithSession(SSLSession session) {
        final HttpClientContext ctx = HttpClientContext.create();
        ctx.setSSLSession(session);
        return ctx;
    }

    private static ConnectionReuseStrategy allowingFallback() {
        final ConnectionReuseStrategy fallback = mock(ConnectionReuseStrategy.class);
        when(fallback.keepAlive(any(), any(), any())).thenReturn(true);
        return fallback;
    }

    public void testFallbackDecisionRespected() {
        final ConnectionReuseStrategy fallback = mock(ConnectionReuseStrategy.class);
        when(fallback.keepAlive(any(), any(), any())).thenReturn(false);
        final ReloadableTlsStrategy tlsStrategy = new ReloadableTlsStrategy();
        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(tlsStrategy, fallback);

        final HttpClientContext context = HttpClientContext.create();
        assertFalse(strategy.keepAlive(mock(HttpRequest.class), mock(HttpResponse.class), context));
        // Must not read SSL state when fallback already rejected
        verify(fallback).keepAlive(any(), any(), any());
    }

    public void testKeepsAliveWhenNoSSLSession() {
        final ReloadableTlsStrategy tlsStrategy = new ReloadableTlsStrategy();
        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(tlsStrategy, allowingFallback());

        final HttpClientContext context = HttpClientContext.create();
        assertTrue(strategy.keepAlive(mock(HttpRequest.class), mock(HttpResponse.class), context));
    }

    public void testKeepsAliveWhenNoStamp() {
        final ReloadableTlsStrategy tlsStrategy = new ReloadableTlsStrategy();
        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(tlsStrategy, allowingFallback());

        final SSLSession session = mock(SSLSession.class);
        when(session.getValue(ReloadableTlsStrategy.SESSION_KEY)).thenReturn(null);

        assertTrue(strategy.keepAlive(mock(HttpRequest.class), mock(HttpResponse.class), contextWithSession(session)));
    }

    public void testKeepsAliveWhenStampMatchesCurrentEpoch() {
        final ReloadableTlsStrategy tlsStrategy = new ReloadableTlsStrategy();
        tlsStrategy.setDelegate(mock(DefaultClientTlsStrategy.class));
        final int epoch = tlsStrategy.currentEpoch();

        final SSLSession session = mock(SSLSession.class);
        when(session.getValue(ReloadableTlsStrategy.SESSION_KEY)).thenReturn(epoch);

        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(tlsStrategy, allowingFallback());
        assertTrue(strategy.keepAlive(mock(HttpRequest.class), mock(HttpResponse.class), contextWithSession(session)));
    }

    public void testRetiresWhenStampIsStale() {
        final ReloadableTlsStrategy tlsStrategy = new ReloadableTlsStrategy();
        tlsStrategy.setDelegate(mock(DefaultClientTlsStrategy.class));
        final int staleEpoch = tlsStrategy.currentEpoch();
        tlsStrategy.setDelegate(mock(DefaultClientTlsStrategy.class));

        final SSLSession session = mock(SSLSession.class);
        when(session.getValue(ReloadableTlsStrategy.SESSION_KEY)).thenReturn(staleEpoch);

        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(tlsStrategy, allowingFallback());
        assertFalse(strategy.keepAlive(mock(HttpRequest.class), mock(HttpResponse.class), contextWithSession(session)));
    }

    public void testRetiresAfterDelegateSwap() {
        final ReloadableTlsStrategy tlsStrategy = new ReloadableTlsStrategy();
        tlsStrategy.setDelegate(mock(DefaultClientTlsStrategy.class));
        final int epochA = tlsStrategy.currentEpoch();

        final SSLSession session = mock(SSLSession.class);
        when(session.getValue(ReloadableTlsStrategy.SESSION_KEY)).thenReturn(epochA);

        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(tlsStrategy, allowingFallback());

        // Before rotation: connection is alive
        assertTrue(strategy.keepAlive(mock(HttpRequest.class), mock(HttpResponse.class), contextWithSession(session)));

        // Rotate — epoch advances
        tlsStrategy.setDelegate(mock(DefaultClientTlsStrategy.class));

        // After rotation: connection stamped with old epoch is stale
        assertFalse(strategy.keepAlive(mock(HttpRequest.class), mock(HttpResponse.class), contextWithSession(session)));
    }

    public void testNonIntegerStampAtSessionKeyKeepsAlive() {
        final ReloadableTlsStrategy tlsStrategy = new ReloadableTlsStrategy();
        tlsStrategy.setDelegate(mock(DefaultClientTlsStrategy.class));
        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(tlsStrategy, allowingFallback());

        final SSLSession session = mock(SSLSession.class);
        when(session.getValue(ReloadableTlsStrategy.SESSION_KEY)).thenReturn("not-an-integer");

        assertTrue(strategy.keepAlive(mock(HttpRequest.class), mock(HttpResponse.class), contextWithSession(session)));
    }

    public void testNoSSLSessionVerification() {
        final ConnectionReuseStrategy fallback = mock(ConnectionReuseStrategy.class);
        when(fallback.keepAlive(any(), any(), any())).thenReturn(false);
        final ReloadableTlsStrategy tlsStrategy = new ReloadableTlsStrategy();
        final RotationAwareReuseStrategy strategy = new RotationAwareReuseStrategy(tlsStrategy, fallback);

        final SSLSession session = mock(SSLSession.class);
        strategy.keepAlive(mock(HttpRequest.class), mock(HttpResponse.class), contextWithSession(session));

        // SSLSession must not be queried when fallback already returns false
        verify(session, never()).getValue(any());
    }
}
