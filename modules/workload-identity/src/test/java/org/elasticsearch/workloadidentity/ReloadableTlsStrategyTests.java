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
import org.apache.hc.core5.net.NamedEndpoint;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.reactor.ssl.TransportSecurityLayer;
import org.elasticsearch.test.ESTestCase;

import javax.net.ssl.SSLSession;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReloadableTlsStrategyTests extends ESTestCase {

    /**
     * Returns a delegate whose {@code upgrade()} immediately fires {@code callback.completed(result)}
     * synchronously, letting unit tests exercise the wrapping callback without a real IO reactor.
     */
    private static DefaultClientTlsStrategy completingDelegate(TransportSecurityLayer result) {
        final DefaultClientTlsStrategy delegate = mock(DefaultClientTlsStrategy.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final FutureCallback<TransportSecurityLayer> cb = invocation.getArgument(4);
            if (cb != null) {
                cb.completed(result);
            }
            return null;
        }).when(delegate).upgrade(any(), any(), any(), any(), any());
        return delegate;
    }

    public void testEpochStartsAtZeroAndAdvancesOnSetDelegate() {
        final ReloadableTlsStrategy wrapper = new ReloadableTlsStrategy();

        assertThat("initial rotation epoch", wrapper.currentEpoch(), equalTo(0));

        wrapper.setDelegate(mock(DefaultClientTlsStrategy.class));
        assertThat("first rotation advances epoch", wrapper.currentEpoch(), equalTo(1));

        wrapper.setDelegate(mock(DefaultClientTlsStrategy.class));
        assertThat("each rotation advances epoch", wrapper.currentEpoch(), equalTo(2));
    }

    public void testSetDelegatePublishesAndReplaces() {
        final ReloadableTlsStrategy wrapper = new ReloadableTlsStrategy();

        assertNull("delegate must be null before setDelegate", wrapper.getDelegate());

        final DefaultClientTlsStrategy first = mock(DefaultClientTlsStrategy.class);
        wrapper.setDelegate(first);
        assertSame("getDelegate must return the published delegate", first, wrapper.getDelegate());

        final DefaultClientTlsStrategy second = mock(DefaultClientTlsStrategy.class);
        wrapper.setDelegate(second);
        assertSame("second setDelegate must replace the first", second, wrapper.getDelegate());
    }

    public void testUpgradeStampsSessionWithCurrentEpoch() {
        final ReloadableTlsStrategy wrapper = new ReloadableTlsStrategy();

        final SSLSession session = mock(SSLSession.class);
        final TransportSecurityLayer result = mock(TransportSecurityLayer.class);
        when(result.getTlsDetails()).thenReturn(new TlsDetails(session, null));

        wrapper.setDelegate(completingDelegate(result));
        final int epoch = wrapper.currentEpoch();

        wrapper.upgrade(mock(TransportSecurityLayer.class), mock(NamedEndpoint.class), null, null, null);

        verify(session).putValue(ReloadableTlsStrategy.SESSION_KEY, epoch);
    }

    public void testUpgradeWithNullTlsDetailsDoesNotThrow() {
        final ReloadableTlsStrategy wrapper = new ReloadableTlsStrategy();
        final TransportSecurityLayer result = mock(TransportSecurityLayer.class);
        when(result.getTlsDetails()).thenReturn(null);
        wrapper.setDelegate(completingDelegate(result));

        wrapper.upgrade(mock(TransportSecurityLayer.class), mock(NamedEndpoint.class), null, null, null);
    }

    public void testUpgradeForwardsCompletedToOuterCallback() {
        final ReloadableTlsStrategy wrapper = new ReloadableTlsStrategy();
        final TransportSecurityLayer result = mock(TransportSecurityLayer.class);
        when(result.getTlsDetails()).thenReturn(null);
        wrapper.setDelegate(completingDelegate(result));

        @SuppressWarnings("unchecked")
        final FutureCallback<TransportSecurityLayer> outer = mock(FutureCallback.class);
        wrapper.upgrade(mock(TransportSecurityLayer.class), mock(NamedEndpoint.class), null, null, outer);

        verify(outer).completed(result);
    }

    public void testUpgradeForwardsFailedToOuterCallback() {
        final ReloadableTlsStrategy wrapper = new ReloadableTlsStrategy();
        final Exception failure = new Exception("handshake failed");
        final DefaultClientTlsStrategy delegate = mock(DefaultClientTlsStrategy.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final FutureCallback<TransportSecurityLayer> cb = invocation.getArgument(4);
            if (cb != null) {
                cb.failed(failure);
            }
            return null;
        }).when(delegate).upgrade(any(), any(), any(), any(), any());
        wrapper.setDelegate(delegate);

        @SuppressWarnings("unchecked")
        final FutureCallback<TransportSecurityLayer> outer = mock(FutureCallback.class);
        wrapper.upgrade(mock(TransportSecurityLayer.class), mock(NamedEndpoint.class), null, null, outer);

        verify(outer).failed(failure);
    }

    public void testUpgradeForwardsCancelledToOuterCallback() {
        final ReloadableTlsStrategy wrapper = new ReloadableTlsStrategy();
        final DefaultClientTlsStrategy delegate = mock(DefaultClientTlsStrategy.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final FutureCallback<TransportSecurityLayer> cb = invocation.getArgument(4);
            if (cb != null) {
                cb.cancelled();
            }
            return null;
        }).when(delegate).upgrade(any(), any(), any(), any(), any());
        wrapper.setDelegate(delegate);

        @SuppressWarnings("unchecked")
        final FutureCallback<TransportSecurityLayer> outer = mock(FutureCallback.class);
        wrapper.upgrade(mock(TransportSecurityLayer.class), mock(NamedEndpoint.class), null, null, outer);

        verify(outer).cancelled();
    }

    @SuppressWarnings("deprecation")
    public void testDeprecatedUpgradeReturnsTrue() {
        final ReloadableTlsStrategy wrapper = new ReloadableTlsStrategy();
        wrapper.setDelegate(mock(DefaultClientTlsStrategy.class));

        final boolean result = wrapper.upgrade(mock(TransportSecurityLayer.class), new HttpHost("localhost"), null, null, null, null);
        assertTrue("deprecated upgrade() must always return true", result);
    }

    /**
     * Guards against a future wiring regression: {@link ReloadableTlsStrategy#upgrade} must
     * reject dispatch before {@link ReloadableTlsStrategy#setDelegate} has published a delegate.
     * Unreachable in production because {@link WorkloadIdentitySslConfig#start()} sequences the
     * initial publish ahead of {@link WorkloadIdentityHttpClientManager#start()}.
     */
    public void testUpgradeBeforeSetDelegateFails() {
        final ReloadableTlsStrategy wrapper = new ReloadableTlsStrategy();
        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> wrapper.upgrade(null, null, null, null, null, null)
        );
        assertThat(ex.getMessage(), containsString("setDelegate"));
    }
}
