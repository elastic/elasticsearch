/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.Index;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.datastreams.DataStreamFeatureSetUsage;

import java.util.LongSummaryStatistics;
import java.util.Map;

public class DataStreamUsageTransportAction extends XPackUsageFeatureTransportAction {

    @Inject
    public DataStreamUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            XPackUsageFeatureAction.DATA_STREAMS.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        final Map<String, DataStream> dataStreams = state.metadata().dataStreams();
        Map<String, IndexMetadata> indices = state.metadata().indices();
        var backingIndicesCount = 0;
        var dataStreamsWithLifecycle = 0;
        var backingIndicesManagedByDataStreamLifecycle = 0;
        LongSummaryStatistics retentionStats = new LongSummaryStatistics();
        for (DataStream dataStream : state.metadata().dataStreams().values()) {
            backingIndicesCount += dataStream.getIndices().size();
            if (DataStreamLifecycle.isEnabled() && dataStream.getLifecycle() != null) {
                dataStreamsWithLifecycle++;
                if (dataStream.getLifecycle().getEffectiveDataRetention() != null) {
                    retentionStats.accept(dataStream.getLifecycle().getEffectiveDataRetention().getMillis());
                }
                for (Index backingIndex : dataStream.getIndices()) {
                    if (dataStream.isIndexManagedByDataStreamLifecycle(backingIndex, indices::get)) {
                        backingIndicesManagedByDataStreamLifecycle++;
                    }
                }
            }
        }
        final DataStreamFeatureSetUsage.DataStreamStats stats = new DataStreamFeatureSetUsage.DataStreamStats(
            dataStreams.size(),
            backingIndicesCount
        );

        final DataStreamFeatureSetUsage.LifecycleStats lifecycleStats = DataStreamLifecycle.isEnabled()
            ? calculateLifecycleStats(dataStreamsWithLifecycle, backingIndicesManagedByDataStreamLifecycle, retentionStats)
            : null;
        final DataStreamFeatureSetUsage usage = new DataStreamFeatureSetUsage(stats, lifecycleStats);
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }

    private DataStreamFeatureSetUsage.LifecycleStats calculateLifecycleStats(
        int dataStreamsWithLifecycle,
        int backingIndicesManagedByDataStreamLifecycle,
        LongSummaryStatistics retentionStats
    ) {
        long minRetention = dataStreamsWithLifecycle == 0 ? 0 : retentionStats.getMin();
        long maxRetention = dataStreamsWithLifecycle == 0 ? 0 : retentionStats.getMax();
        double averageRetention = retentionStats.getAverage();
        RolloverConfiguration rolloverConfiguration = clusterService.getClusterSettings()
            .get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING);
        String rolloverConfigString = rolloverConfiguration.toString();
        final DataStreamFeatureSetUsage.LifecycleStats lifecycleStats = new DataStreamFeatureSetUsage.LifecycleStats(
            dataStreamsWithLifecycle,
            backingIndicesManagedByDataStreamLifecycle,
            minRetention,
            maxRetention,
            averageRetention,
            DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getDefault(null).toString().equals(rolloverConfigString)
        );
        return lifecycleStats;
    }
}
