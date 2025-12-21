/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;
import static org.elasticsearch.transport.AbstractSimpleTransportTestCase.IGNORE_DESERIALIZATION_ERRORS_SETTING;

public class ServerlessTransportHandshakeTests extends ESTestCase {

    private static ThreadPool threadPool;

    @BeforeClass
    public static void startThreadPool() {
        threadPool = new TestThreadPool(ServerlessTransportHandshakeTests.class.getSimpleName());
    }

    private final List<TransportService> transportServices = new ArrayList<>();

    private TransportService startServices(String nodeNameAndId, Settings settings, TransportInterceptor transportInterceptor) {
        TcpTransport transport = new Netty4Transport(
            settings,
            TransportVersion.current(),
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(settings)
        );
        TransportService transportService = new MockTransportService(
            settings,
            transport,
            threadPool,
            transportInterceptor,
            (boundAddress) -> DiscoveryNodeUtils.builder(nodeNameAndId)
                .name(nodeNameAndId)
                .address(boundAddress.publishAddress())
                .roles(emptySet())
                .version(VersionInformation.CURRENT)
                .build(),
            null,
            Collections.emptySet(),
            nodeNameAndId
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        transportServices.add(transportService);
        return transportService;
    }

    @After
    public void tearDown() throws Exception {
        for (TransportService transportService : transportServices) {
            transportService.close();
        }
        super.tearDown();
    }

    @AfterClass
    public static void terminateThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        threadPool = null;
    }

    public void testAcceptsMismatchedServerlessBuildHashWithoutWarning() {
        assumeTrue("Current build needs to be a snapshot", Build.current().isSnapshot());
        final var transportInterceptorA = new BuildHashModifyingTransportInterceptor();
        final var transportInterceptorB = new BuildHashModifyingTransportInterceptor();
        final Settings settings = Settings.builder()
            .put("cluster.name", "a")
            .put(IGNORE_DESERIALIZATION_ERRORS_SETTING.getKey(), true) // suppress assertions to test production error-handling
            .build();
        final TransportService transportServiceA = startServices("TS_A", settings, transportInterceptorA);
        final TransportService transportServiceB = startServices("TS_B", settings, transportInterceptorB);
        MockLog.assertThatLogger(() -> {
            AbstractSimpleTransportTestCase.connectToNode(transportServiceA, transportServiceB.getLocalNode(), TestProfiles.LIGHT_PROFILE);
            assertTrue(transportServiceA.nodeConnected(transportServiceB.getLocalNode()));
        },
            TransportService.class,
            new MockLog.UnseenEventExpectation("incompatible wire format log", TransportService.class.getCanonicalName(), Level.WARN, "*")
        );

    }

}
