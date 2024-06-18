/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.mockito.Mockito.mock;

public class UpdateEventIngestedRangeTransportActionTests extends ESTestCase {

    // TODO: add this to a setUp method if this works
    private DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();

    public void testMasterOperation() {
        ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        ThreadPool mockThreadPool = mock(ThreadPool.class);

        // TODO: this doesn't work with a non-mock thread pool
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(mockThreadPool);

        // TODO: is this going to work with a mock TransportService?
        String indexName = "blogs";
        ClusterState clusterState1 = state(1, new String[] { indexName }, 1);
        Index blogsIndex = clusterState1.metadata().index(indexName).getIndex();
        ClusterService clusterService = createClusterService(Settings.builder(), clusterState1);
        UpdateEventIngestedRangeTransportAction transportAction = new UpdateEventIngestedRangeTransportAction(
            transportService,
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class)
        );
        // TODO: how do I set this up to run?
        Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap = new HashMap<>();
        EventIngestedRangeClusterStateService.ShardRangeInfo shardRangeInfo = new EventIngestedRangeClusterStateService.ShardRangeInfo(
            new ShardId(blogsIndex, 0),
            ShardLongFieldRange.of(1000, 2000)
        );
        eventIngestedRangeMap.put(blogsIndex, List.of(shardRangeInfo));

        UpdateEventIngestedRangeRequest updateRangeRequest = new UpdateEventIngestedRangeRequest(eventIngestedRangeMap);

        AtomicReference<AcknowledgedResponse> ackedResponseFromCallback = new AtomicReference<>(null);
        ActionListener<AcknowledgedResponse> responseListener = new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                ackedResponseFromCallback.set(acknowledgedResponse);
            }

            @Override
            public void onFailure(Exception e) {
                fail("onFailure should not have been called but received: " + e);
            }
        };

        // TODO: this test runs "successfully" (no errors), but the task never runs - how do I create a real task system?
        transportAction.masterOperation(null, updateRangeRequest, clusterState1, responseListener);
    }

    public void testOnFailureIsCalled() {
        ThreadPool mockThreadPool = mock(ThreadPool.class);

        // TODO: this doesn't work with a non-mock thread pool
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(mockThreadPool);

        // TODO: is this going to work with a mock TransportService?
        String indexName = "blogs";
        ClusterState clusterState1 = state(1, new String[] { indexName }, 1);
        Index blogsIndex = clusterState1.metadata().index(indexName).getIndex();
        UpdateEventIngestedRangeTransportAction transportAction = new UpdateEventIngestedRangeTransportAction(
            transportService,
            mock(ClusterService.class),
            mockThreadPool,
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class)
        );
        // TODO: how do I set this up to run?
        Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap = new HashMap<>();
        EventIngestedRangeClusterStateService.ShardRangeInfo shardRangeInfo = new EventIngestedRangeClusterStateService.ShardRangeInfo(
            new ShardId(blogsIndex, 0),
            ShardLongFieldRange.of(1000, 2000)
        );
        eventIngestedRangeMap.put(blogsIndex, List.of(shardRangeInfo));

        UpdateEventIngestedRangeRequest updateRangeRequest = new UpdateEventIngestedRangeRequest(eventIngestedRangeMap);

        AtomicBoolean onFailureCalled = new AtomicBoolean(false);
        ActionListener<AcknowledgedResponse> responseListener = new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                fail("onResponse should not have been called");
            }

            @Override
            public void onFailure(Exception e) {
                onFailureCalled.set(true);
            }
        };

        transportAction.masterOperation(null, updateRangeRequest, clusterState1, responseListener);
        // TODO: this test passes (onFailure is called) but for uninteresting reasons - namely queue.submitTask throws NPE
        // TODO: need a better onFailure test that ideally fails while the task is running - how do that?
        assertTrue("onFailure should have been called", onFailureCalled.get());
    }

    // copied from GatewayServiceTests
    private ClusterService createClusterService(Settings.Builder settingsBuilder, ClusterState initialState) {
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var settings = settingsBuilder.build();
        final var clusterSettings = createBuiltInClusterSettings(settings);

        final var clusterService = new ClusterService(
            settings,
            clusterSettings,
            new FakeThreadPoolMasterService(initialState.nodes().getLocalNodeId(), threadPool, deterministicTaskQueue::scheduleNow),
            new ClusterApplierService(initialState.nodes().getLocalNodeId(), settings, clusterSettings, threadPool) {
                @Override
                protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                    return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
                }
            }
        );

        clusterService.getClusterApplierService().setInitialState(initialState);
        clusterService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        clusterService.getMasterService()
            .setClusterStatePublisher(ClusterServiceUtils.createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getMasterService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        return clusterService;
    }
}
