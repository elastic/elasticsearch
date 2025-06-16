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
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.datastreams.DataStreamLifecycleFeatureSetUsage;

import java.util.Collection;
import java.util.LongSummaryStatistics;
import java.util.Map;

public class DataStreamLifecycleUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final DataStreamGlobalRetentionSettings globalRetentionSettings;

    @Inject
    public DataStreamLifecycleUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        super(XPackUsageFeatureAction.DATA_STREAM_LIFECYCLE.name(), transportService, clusterService, threadPool, actionFilters);
        this.globalRetentionSettings = globalRetentionSettings;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        final Collection<DataStream> dataStreams = state.metadata().dataStreams().values();
        DataStreamLifecycleFeatureSetUsage.LifecycleStats lifecycleStats = calculateStats(
            dataStreams,
            clusterService.getClusterSettings().get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING),
            globalRetentionSettings.get()
        );
        listener.onResponse(new XPackUsageFeatureResponse(new DataStreamLifecycleFeatureSetUsage(lifecycleStats)));
    }

    /**
     * Counts the number of data streams that have a lifecycle configured (and enabled),
     * computes the min/max/average summary of the data and effective retention and tracks the usage of global retention.
     */
    public static DataStreamLifecycleFeatureSetUsage.LifecycleStats calculateStats(
        Collection<DataStream> dataStreams,
        RolloverConfiguration rolloverConfiguration,
        DataStreamGlobalRetention globalRetention
    ) {
        // Initialise counters of associated data streams
        long dataStreamsWithLifecycles = 0;
        long dataStreamsWithDefaultRetention = 0;
        long dataStreamsWithMaxRetention = 0;

        LongSummaryStatistics dataRetentionStats = new LongSummaryStatistics();
        LongSummaryStatistics effectiveRetentionStats = new LongSummaryStatistics();

        for (DataStream dataStream : dataStreams) {
            if (dataStream.getDataLifecycle() != null && dataStream.getDataLifecycle().enabled()) {
                dataStreamsWithLifecycles++;
                // Track data retention
                if (dataStream.getDataLifecycle().dataRetention() != null) {
                    dataRetentionStats.accept(dataStream.getDataLifecycle().dataRetention().getMillis());
                }
                // Track effective retention
                Tuple<TimeValue, DataStreamLifecycle.RetentionSource> effectiveDataRetentionWithSource = dataStream.getDataLifecycle()
                    .getEffectiveDataRetentionWithSource(globalRetention, dataStream.isInternal());

                // Track global retention usage
                if (effectiveDataRetentionWithSource.v1() != null) {
                    effectiveRetentionStats.accept(effectiveDataRetentionWithSource.v1().getMillis());
                    if (effectiveDataRetentionWithSource.v2().equals(DataStreamLifecycle.RetentionSource.DEFAULT_GLOBAL_RETENTION)) {
                        dataStreamsWithDefaultRetention++;
                    }
                    if (effectiveDataRetentionWithSource.v2().equals(DataStreamLifecycle.RetentionSource.MAX_GLOBAL_RETENTION)) {
                        dataStreamsWithMaxRetention++;
                    }
                }
            }
        }
        Map<String, DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats> globalRetentionStats =
            DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats.getGlobalRetentionStats(
                globalRetention,
                dataStreamsWithDefaultRetention,
                dataStreamsWithMaxRetention
            );
        return new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
            dataStreamsWithLifecycles,
            DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getDefault(null).equals(rolloverConfiguration),
            DataStreamLifecycleFeatureSetUsage.RetentionStats.create(dataRetentionStats),
            DataStreamLifecycleFeatureSetUsage.RetentionStats.create(effectiveRetentionStats),
            globalRetentionStats
        );
    }
}
