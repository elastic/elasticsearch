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

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReloadableSchemeIoSessionStrategyTests extends ESTestCase {

    public void testEpochStartsAtZeroAndAdvancesOnSetDelegate() {
        final SSLIOSessionStrategy initial = mock(SSLIOSessionStrategy.class);
        final ReloadableSchemeIoSessionStrategy wrapper = new ReloadableSchemeIoSessionStrategy(initial);

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

        final ReloadableSchemeIoSessionStrategy wrapper = new ReloadableSchemeIoSessionStrategy(delegate);
        // Rotate once before exercising upgrade() so the assertion is unambiguous (epoch != 0).
        wrapper.setDelegate(delegate);
        assertThat(wrapper.currentEpoch(), equalTo(1));

        final IOSession returned = wrapper.upgrade(host, raw);

        assertSame("upgrade must return whatever the delegate returned", upgraded, returned);
        // The attribute name and value together are the contract observed by RotationAwareReuseStrategy.
        verify(upgraded).setAttribute(ReloadableSchemeIoSessionStrategy.ROTATION_EPOCH_ATTR, 1);
    }

}
