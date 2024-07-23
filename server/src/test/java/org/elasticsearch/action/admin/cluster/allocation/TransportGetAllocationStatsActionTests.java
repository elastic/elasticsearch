/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.AllocationStatsService;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStatsTests;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetAllocationStatsActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private AllocationStatsService allocationStatsService;
    private FeatureService featureService;

    private TransportGetAllocationStatsAction action;

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
        allocationStatsService = mock(AllocationStatsService.class);
        featureService = mock(FeatureService.class);
        action = new TransportGetAllocationStatsAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Set.of()),
            null,
            allocationStatsService,
            featureService
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

    public void testReturnsOnlyRequestedStats() throws Exception {

        var metrics = EnumSet.copyOf(randomSubsetOf(Metric.values().length, Metric.values()));

        var request = new TransportGetAllocationStatsAction.Request(
            TimeValue.ONE_MINUTE,
            new TaskId(randomIdentifier(), randomNonNegativeLong()),
            metrics
        );

        when(allocationStatsService.stats()).thenReturn(Map.of(randomIdentifier(), NodeAllocationStatsTests.randomNodeAllocationStats()));
        when(featureService.clusterHasFeature(any(ClusterState.class), eq(AllocationStatsFeatures.INCLUDE_DISK_THRESHOLD_SETTINGS)))
            .thenReturn(true);

        var future = new PlainActionFuture<TransportGetAllocationStatsAction.Response>();
        action.masterOperation(mock(Task.class), request, ClusterState.EMPTY_STATE, future);
        var response = future.get();

        if (metrics.contains(Metric.ALLOCATIONS)) {
            assertThat(response.getNodeAllocationStats(), not(anEmptyMap()));
            verify(allocationStatsService).stats();
        } else {
            assertThat(response.getNodeAllocationStats(), anEmptyMap());
            verify(allocationStatsService, never()).stats();
        }

        if (metrics.contains(Metric.FS)) {
            assertNotNull(response.getDiskThresholdSettings());
        } else {
            assertNull(response.getDiskThresholdSettings());
        }
    }
}
