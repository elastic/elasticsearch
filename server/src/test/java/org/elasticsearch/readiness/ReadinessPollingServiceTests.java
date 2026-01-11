/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.readiness;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

public class ReadinessPollingServiceTests extends ESTestCase {
    /**
     * This is for tests that are not expecting a timeout to occur.
     * We use a large value to support single-step debugging.
     */
    private static final int LONG_TIMEOUT_MILLIS = 20 * 60 * 1000;

    /**
     * We're not actually waiting for nodes to boot, so use a short value to avoid wasting time.
     */
    private static final int QUICK_RETRY_MILLIS = 1;

    private ThreadPool threadPool;
    private MockTransportService transport;
    private ClusterService clusterService;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getClass().getName());
        transport = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        transport.start();
        transport.acceptIncomingRequests();
        DiscoveryNode node = transport.getLocalNode();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().add(node);
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster")).nodes(nodesBuilder).build();
        clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool);
    }

    @After
    public void teardown() {
        clusterService.close();
        transport.close();
        terminate(threadPool);
    }

    public void testReadinessSucceedsImmediately() throws Exception {
        registerReadinessAction(() -> true);

        ReadinessPollingService poll = new ReadinessPollingService(
            clusterService,
            transport,
            threadPool,
            LONG_TIMEOUT_MILLIS,
            QUICK_RETRY_MILLIS
        );
        AtomicBoolean readiness = new AtomicBoolean(false);

        poll.execute(n -> true, ActionListener.wrap(readiness::set, e -> fail(e.toString())));

        assertBusy(() -> assertTrue("Node was ready", readiness.get()));
    }

    public void testReadinessRetriesThenSucceeds() throws Exception {
        AtomicInteger remainingFailures = new AtomicInteger(2); // fails twice before success
        registerReadinessAction(() -> remainingFailures.getAndDecrement() <= 0);

        ReadinessPollingService poll = new ReadinessPollingService(
            clusterService,
            transport,
            threadPool,
            LONG_TIMEOUT_MILLIS,
            QUICK_RETRY_MILLIS
        );
        AtomicBoolean readiness = new AtomicBoolean(false);

        poll.execute(n -> true, ActionListener.wrap(readiness::set, e -> fail(e.toString())));

        assertBusy(() -> assertTrue("Node was ready", readiness.get()));
    }

    private void registerReadinessAction(BooleanSupplier readinessSupplier) {
        TransportReadinessAction action = new TransportReadinessAction(new ActionFilters(Set.of()), null, Runnable::run, readinessSupplier);
        transport.registerRequestHandler(
            TransportReadinessAction.TYPE.name(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            false,
            false,
            ReadinessRequest::new,
            (request, channel, task) -> action.execute(task, request, ActionListener.wrap(channel::sendResponse, channel::sendResponse))
        );
    }

}
