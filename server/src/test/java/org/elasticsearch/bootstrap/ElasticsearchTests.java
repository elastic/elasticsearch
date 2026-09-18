/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.Scope;
import org.elasticsearch.entitlement.runtime.policy.entitlements.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;
import org.elasticsearch.readiness.ReadinessService;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class ElasticsearchTests extends ESTestCase {
    public void testFindPluginsWithNativeAccess() {

        var policies = Map.ofEntries(
            entry(
                "plugin-with-native",
                new Policy(
                    "policy",
                    List.of(
                        new Scope("module.a", List.of(new LoadNativeLibrariesEntitlement())),
                        new Scope("module.b", List.of(new InboundNetworkEntitlement()))
                    )
                )
            ),
            entry(
                "another-plugin-with-native",
                new Policy(
                    "policy",
                    List.of(
                        new Scope("module.a2", List.of(new LoadNativeLibrariesEntitlement())),
                        new Scope("module.b2", List.of(new LoadNativeLibrariesEntitlement())),
                        new Scope("module.c2", List.of(new InboundNetworkEntitlement()))

                    )
                )
            ),
            entry(
                "plugin-without-native",
                new Policy(
                    "policy",
                    List.of(
                        new Scope("module.a3", List.of(new InboundNetworkEntitlement())),
                        new Scope("module.b3", List.of(new OutboundNetworkEntitlement()))
                    )
                )
            )
        );

        var pluginsWithNativeAccess = Elasticsearch.findPluginsWithNativeAccess(policies);

        assertThat(pluginsWithNativeAccess.keySet(), containsInAnyOrder("plugin-with-native", "another-plugin-with-native"));
        assertThat(pluginsWithNativeAccess.get("plugin-with-native"), containsInAnyOrder("module.a"));
        assertThat(pluginsWithNativeAccess.get("another-plugin-with-native"), containsInAnyOrder("module.a2", "module.b2"));
    }

    public void testWaitForNodeReadyBlocksUntilReady() throws Exception {
        CountDownLatch listenerRegistered = new CountDownLatch(1);
        AtomicReference<ReadinessService.BoundAddressListener> capturedListener = new AtomicReference<>();

        ReadinessService service = new ReadinessService(null, null) {
            @Override
            public synchronized void addBoundAddressListener(BoundAddressListener listener) {
                capturedListener.set(listener);
                listenerRegistered.countDown();
            }
        };

        Thread waiter = new Thread(() -> Elasticsearch.waitForNodeReady(service), "test-waiter");
        waiter.start();
        try {
            assertTrue("listener should be registered", listenerRegistered.await(10, java.util.concurrent.TimeUnit.SECONDS));
            assertTrue("waitForNodeReady should be blocking", waiter.isAlive());

            capturedListener.get().addressBound(mockBoundAddress());

            waiter.join(10_000);
            assertFalse("waitForNodeReady should have returned after readiness fired", waiter.isAlive());
        } finally {
            // ensure the thread is not left stuck if the test fails partway through
            if (capturedListener.get() != null) {
                capturedListener.get().addressBound(mockBoundAddress());
            }
            waiter.join(5_000);
        }
    }

    public void testWaitForNodeReadyWhenAlreadyReady() throws Exception {
        BoundTransportAddress address = mockBoundAddress();

        // Simulates a service that is already ready: fires the listener immediately on registration
        ReadinessService service = new ReadinessService(null, null) {
            @Override
            public synchronized void addBoundAddressListener(BoundAddressListener listener) {
                listener.addressBound(address);
            }
        };

        // Should return without blocking; if it hangs the test will time out
        Elasticsearch.waitForNodeReady(service);
    }

    private static BoundTransportAddress mockBoundAddress() throws UnknownHostException {
        TransportAddress ta = new TransportAddress(InetAddress.getLoopbackAddress(), 9200);
        return new BoundTransportAddress(new TransportAddress[] { ta }, ta);
    }
}
