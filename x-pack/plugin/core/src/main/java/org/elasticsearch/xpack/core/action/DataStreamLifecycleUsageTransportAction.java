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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.datastreams.DataStreamLifecycleFeatureSetUsage;

import java.util.Collection;
import java.util.LongSummaryStatistics;

public class DataStreamLifecycleUsageTransportAction extends XPackUsageFeatureTransportAction {

    @Inject
    public DataStreamLifecycleUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            XPackUsageFeatureAction.DATA_STREAM_LIFECYCLE.name(),
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
        final Collection<DataStream> dataStreams = state.metadata().dataStreams().values();
        Tuple<Long, LongSummaryStatistics> stats = calculateStats(dataStreams);

        long minRetention = stats.v2().getCount() == 0 ? 0 : stats.v2().getMin();
        long maxRetention = stats.v2().getCount() == 0 ? 0 : stats.v2().getMax();
        double averageRetention = stats.v2().getAverage();
        RolloverConfiguration rolloverConfiguration = clusterService.getClusterSettings()
            .get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING);
        String rolloverConfigString = rolloverConfiguration.toString();
        final DataStreamLifecycleFeatureSetUsage.LifecycleStats lifecycleStats = new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
            stats.v1(),
            minRetention,
            maxRetention,
            averageRetention,
            DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getDefault(null).toString().equals(rolloverConfigString)
        );

        final DataStreamLifecycleFeatureSetUsage usage = new DataStreamLifecycleFeatureSetUsage(lifecycleStats);
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }

    /**
     * Counts the number of data streams that have a lifecycle configured (and enabled) and for
     * the data streams that have a lifecycle it computes the min/max/average summary of the effective
     * configured retention.
     */
    public static Tuple<Long, LongSummaryStatistics> calculateStats(Collection<DataStream> dataStreams) {
        long dataStreamsWithLifecycles = 0;
        LongSummaryStatistics retentionStats = new LongSummaryStatistics();
        for (DataStream dataStream : dataStreams) {
            if (dataStream.getLifecycle() != null && dataStream.getLifecycle().isEnabled()) {
                dataStreamsWithLifecycles++;
                if (dataStream.getLifecycle().getDataStreamRetention() != null) {
                    retentionStats.accept(dataStream.getLifecycle().getDataStreamRetention().getMillis());
                }
            }
        }
        return new Tuple<>(dataStreamsWithLifecycles, retentionStats);
    }
}
