/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class TransportClusterAllocationExplainActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(TransportClusterAllocationExplainActionTests.class.getName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        transportService = new CapturingTransport().createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            address -> clusterService.localNode(),
            clusterService.getClusterSettings(),
            Set.of()
        );
        new TransportClusterAllocationExplainAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Set.of()),
            null,
            () -> ClusterInfo.EMPTY,
            EmptySnapshotsInfoService.INSTANCE,
            new AllocationDeciders(List.of()),
            null
        );
    }

    public void testCanNotTripCircuitBreaker() {
        assertThat(
            transportService.getRequestHandler(TransportClusterAllocationExplainAction.TYPE.name()).canTripCircuitBreaker(),
            is(false)
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        clusterService.close();
        transportService.close();
    }
}
