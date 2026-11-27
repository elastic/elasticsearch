/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Set;

import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportPublishIndexingOperationsHeapMemoryRequirementsTests extends ESTestCase {

    public void testMasterOperationStoresHeapAndRawSizeSeparately() throws Exception {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(
                StatelessMemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_SETTING,
                StatelessMemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING,
                StatelessMemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING,
                StatelessMemoryMetricsService.MERGE_MEMORY_ESTIMATE_ENABLED_SETTING,
                StatelessMemoryMetricsService.ADAPTIVE_EXTRA_OVERHEAD_SETTING,
                StatelessMemoryMetricsService.SELF_REPORTED_SHARD_MEMORY_OVERHEAD_ENABLED_SETTING,
                StatelessMemoryMetricsService.ADAPTIVE_SHARD_MEMORY_ESTIMATION_MIN_THRESHOLD_ENABLED_SETTING,
                SETTING_CLUSTER_MAX_SHARDS_PER_NODE
            )
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(ThreadPool.Names.MANAGEMENT)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        StatelessMemoryMetricsService service = new StatelessMemoryMetricsService(System::nanoTime, clusterService);
        var action = new TransportPublishIndexingOperationsHeapMemoryRequirements(
            mock(TransportService.class),
            clusterService,
            threadPool,
            ActionFilters.EMPTY,
            service
        );

        final long rawSize = randomLongBetween(1L, 10_000_000L);
        final long heap = randomValueOtherThan(rawSize, () -> randomLongBetween(1L, 10_000_000L));
        final PlainActionFuture<ActionResponse.Empty> future = new PlainActionFuture<>();
        action.masterOperation(
            null,
            new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(heap, rawSize),
            ClusterState.EMPTY_STATE,
            future
        );
        future.actionGet();

        assertThat(service.minimumRequiredHeapForAcceptingLargeIndexingOps(), equalTo(heap));
        assertThat(service.largestRecentIndexingOpSize(), equalTo(rawSize));
    }
}
