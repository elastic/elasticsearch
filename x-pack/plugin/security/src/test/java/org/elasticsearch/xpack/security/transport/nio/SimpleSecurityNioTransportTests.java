/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.security.transport.AbstractSimpleSecurityTransportTestCase;

import java.io.IOException;
import java.util.Collections;

public class SimpleSecurityNioTransportTests extends AbstractSimpleSecurityTransportTestCase {

    public MockTransportService nioFromThreadPool(Settings settings, ThreadPool threadPool, final Version version,
                                                  ClusterSettings clusterSettings, boolean doHandshake) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        NetworkService networkService = new NetworkService(Collections.emptyList());
        Settings settings1 = Settings.builder()
                .put(settings)
                .put("xpack.security.transport.ssl.enabled", true).build();
        Transport transport = new SecurityNioTransport(settings1, threadPool,
                networkService, BigArrays.NON_RECYCLING_INSTANCE, new MockPageCacheRecycler(settings), namedWriteableRegistry,
                new NoneCircuitBreakerService(), null, createSSLService(settings1)) {

            @Override
            public Version executeHandshake(DiscoveryNode node, TcpChannel channel, TimeValue timeout) throws IOException,
                    InterruptedException {
                if (doHandshake) {
                    return super.executeHandshake(node, channel, timeout);
                } else {
                    return version.minimumCompatibilityVersion();
                }
            }

            @Override
            protected Version getCurrentVersion() {
                return version;
            }

        };
        MockTransportService mockTransportService =
                MockTransportService.createNewService(Settings.EMPTY, transport, version, threadPool, clusterSettings,
                        Collections.emptySet());
        mockTransportService.start();
        return mockTransportService;
    }

    @Override
    protected MockTransportService build(Settings settings, Version version, ClusterSettings clusterSettings, boolean doHandshake) {
        if (TcpTransport.PORT.exists(settings) == false) {
            settings = Settings.builder().put(settings)
                .put(TcpTransport.PORT.getKey(), "0")
                .build();
        }
        MockTransportService transportService = nioFromThreadPool(settings, threadPool, version, clusterSettings, doHandshake);
        transportService.start();
        return transportService;
    }
}
