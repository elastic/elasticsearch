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
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOSession;
import org.apache.http.nio.reactor.ssl.SSLIOSession;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReloadableSchemeIoSessionStrategyTests extends ESTestCase {

    public void testEpochStartsAtZeroAndAdvancesOnSetDelegate() {
        final ReloadableSchemeIoSessionStrategy wrapper = new ReloadableSchemeIoSessionStrategy();

        assertThat("initial rotation epoch", wrapper.currentEpoch(), equalTo(0));

        wrapper.setDelegate(mock(SSLIOSessionStrategy.class));
        assertThat("first rotation advances epoch", wrapper.currentEpoch(), equalTo(1));

        wrapper.setDelegate(mock(SSLIOSessionStrategy.class));
        assertThat("each rotation advances epoch", wrapper.currentEpoch(), equalTo(2));
    }

    public void testUpgradeStampsReturnedIOSessionWithCurrentEpoch() throws Exception {
        final SSLIOSessionStrategy delegate = mock(SSLIOSessionStrategy.class);
        // SSLIOSessionStrategy#upgrade returns SSLIOSession (a subtype of IOSession). Match the
        // real signature in the stub so the wrapper sees the same shape as in production.
        final SSLIOSession upgraded = mock(SSLIOSession.class);
        final IOSession raw = mock(IOSession.class);
        final HttpHost host = new HttpHost("issuer.example", 8443, "https");
        when(delegate.upgrade(any(), any())).thenReturn(upgraded);

        final ReloadableSchemeIoSessionStrategy wrapper = new ReloadableSchemeIoSessionStrategy();
        // Publish the initial delegate (mirrors the production WorkloadIdentitySslConfig.start()
        // → listener edge) so upgrade() has something to dispatch to.
        wrapper.setDelegate(delegate);
        assertThat(wrapper.currentEpoch(), equalTo(1));

        final IOSession returned = wrapper.upgrade(host, raw);

        assertSame("upgrade must return whatever the delegate returned", upgraded, returned);
        // The attribute name and value together are the contract observed by RotationAwareReuseStrategy.
        verify(upgraded).setAttribute(ReloadableSchemeIoSessionStrategy.ROTATION_EPOCH_ATTR, 1);
    }

    /**
     * Guards against a future wiring regression: {@link ReloadableSchemeIoSessionStrategy#upgrade upgrade()}
     * must reject dispatch before {@link ReloadableSchemeIoSessionStrategy#setDelegate setDelegate()}
     * has published a delegate. Unreachable in production because
     * {@link WorkloadIdentitySslConfig#start()} sequences the initial publish ahead of
     * {@link WorkloadIdentityHttpClientManager#start()}.
     */
    public void testUpgradeBeforeSetDelegateFails() {
        final ReloadableSchemeIoSessionStrategy wrapper = new ReloadableSchemeIoSessionStrategy();
        final HttpHost host = new HttpHost("issuer.example", 8443, "https");
        final IOSession raw = mock(IOSession.class);
        final IllegalStateException ex = expectThrows(IllegalStateException.class, () -> wrapper.upgrade(host, raw));
        assertThat(ex.getMessage(), containsString("setDelegate"));
    }

}
