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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.datastreams.DataStreamLifecycleFeatureSetUsage;

import java.util.Collection;
import java.util.LongSummaryStatistics;
import java.util.stream.Collectors;

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
        if (DataStreamLifecycle.isEnabled() == false) {
            listener.onResponse(new XPackUsageFeatureResponse(DataStreamLifecycleFeatureSetUsage.DISABLED));
            return;
        }

        final Collection<DataStream> dataStreams = state.metadata().dataStreams().values();
        LongSummaryStatistics retentionStats = dataStreams.stream()
            .filter(ds -> ds.getLifecycle() != null)
            .collect(Collectors.summarizingLong(ds -> ds.getLifecycle().getEffectiveDataRetention().getMillis()));
        long dataStreamsWithLifecycles = retentionStats.getCount();
        long minRetention = dataStreamsWithLifecycles == 0 ? 0 : retentionStats.getMin();
        long maxRetention = dataStreamsWithLifecycles == 0 ? 0 : retentionStats.getMax();
        double averageRetention = retentionStats.getAverage();
        RolloverConfiguration rolloverConfiguration = clusterService.getClusterSettings()
            .get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING);
        String rolloverConfigString = rolloverConfiguration.toString();
        final DataStreamLifecycleFeatureSetUsage.LifecycleStats stats = new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
            dataStreamsWithLifecycles,
            minRetention,
            maxRetention,
            averageRetention,
            DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getDefault(null).toString().equals(rolloverConfigString)
        );

        final DataStreamLifecycleFeatureSetUsage usage = new DataStreamLifecycleFeatureSetUsage(stats);
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }
}
