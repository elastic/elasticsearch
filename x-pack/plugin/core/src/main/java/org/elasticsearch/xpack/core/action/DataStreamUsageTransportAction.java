/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
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
import org.elasticsearch.xpack.core.datastreams.DataStreamFeatureSetUsage;
import org.elasticsearch.xpack.core.datastreams.DataStreamLifecycleFeatureSetUsage;

import java.util.LongSummaryStatistics;
import java.util.Map;

public class DataStreamUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final DataStreamFailureStoreSettings dataStreamFailureStoreSettings;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;

    @Inject
    public DataStreamUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DataStreamFailureStoreSettings dataStreamFailureStoreSettings,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        super(XPackUsageFeatureAction.DATA_STREAMS.name(), transportService, clusterService, threadPool, actionFilters);
        this.dataStreamFailureStoreSettings = dataStreamFailureStoreSettings;
        this.globalRetentionSettings = globalRetentionSettings;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        final Map<String, DataStream> dataStreams = state.metadata().dataStreams();
        long backingIndicesCounter = 0;
        long failureStoreExplicitlyEnabledCounter = 0;
        long failureStoreEffectivelyEnabledCounter = 0;
        long failureIndicesCounter = 0;
        long failuresLifecycleExplicitlyEnabledCounter = 0;
        long failuresLifecycleEffectivelyEnabledCounter = 0;
        LongSummaryStatistics dataRetentionStats = new LongSummaryStatistics();
        LongSummaryStatistics effectiveRetentionStats = new LongSummaryStatistics();
        long affectedByMaxRetentionCounter = 0;
        long affectedByDefaultRetentionCounter = 0;
        DataStreamGlobalRetention globalRetention = globalRetentionSettings.get();
        for (DataStream ds : dataStreams.values()) {
            backingIndicesCounter += ds.getIndices().size();
            if (ds.isFailureStoreExplicitlyEnabled()) {
                failureStoreExplicitlyEnabledCounter++;
            }
            boolean failureStoreEffectivelyEnabled = ds.isFailureStoreEffectivelyEnabled(dataStreamFailureStoreSettings);
            if (failureStoreEffectivelyEnabled) {
                failureStoreEffectivelyEnabledCounter++;
            }
            if (ds.getFailureIndices().isEmpty() == false) {
                failureIndicesCounter += ds.getFailureIndices().size();
            }

            // Track explicitly enabled failures lifecycle configuration
            DataStreamLifecycle configuredFailuresLifecycle = ds.getDataStreamOptions() != null
                && ds.getDataStreamOptions().failureStore() != null
                && ds.getDataStreamOptions().failureStore().lifecycle() != null
                    ? ds.getDataStreamOptions().failureStore().lifecycle()
                    : null;
            if (configuredFailuresLifecycle != null && configuredFailuresLifecycle.enabled()) {
                failuresLifecycleExplicitlyEnabledCounter++;
                if (configuredFailuresLifecycle.dataRetention() != null) {
                    dataRetentionStats.accept(configuredFailuresLifecycle.dataRetention().getMillis());
                }
            }

            // Track effective failure lifecycle
            DataStreamLifecycle effectiveFailuresLifecycle = ds.getFailuresLifecycle(failureStoreEffectivelyEnabled);
            if (effectiveFailuresLifecycle != null && effectiveFailuresLifecycle.enabled()) {
                failuresLifecycleEffectivelyEnabledCounter++;
                // Track effective retention
                Tuple<TimeValue, DataStreamLifecycle.RetentionSource> effectiveDataRetentionWithSource = effectiveFailuresLifecycle
                    .getEffectiveDataRetentionWithSource(globalRetention, ds.isInternal());

                // Track global retention usage
                if (effectiveDataRetentionWithSource.v1() != null) {
                    effectiveRetentionStats.accept(effectiveDataRetentionWithSource.v1().getMillis());
                    if (effectiveDataRetentionWithSource.v2().equals(DataStreamLifecycle.RetentionSource.MAX_GLOBAL_RETENTION)) {
                        affectedByMaxRetentionCounter++;
                    }
                    if (effectiveDataRetentionWithSource.v2().equals(DataStreamLifecycle.RetentionSource.DEFAULT_FAILURES_RETENTION)) {
                        affectedByDefaultRetentionCounter++;
                    }
                }
            }
        }
        final DataStreamFeatureSetUsage.DataStreamStats stats = new DataStreamFeatureSetUsage.DataStreamStats(
            dataStreams.size(),
            backingIndicesCounter,
            failureStoreExplicitlyEnabledCounter,
            failureStoreEffectivelyEnabledCounter,
            failureIndicesCounter,
            failuresLifecycleExplicitlyEnabledCounter,
            failuresLifecycleEffectivelyEnabledCounter,
            DataStreamLifecycleFeatureSetUsage.RetentionStats.create(dataRetentionStats),
            DataStreamLifecycleFeatureSetUsage.RetentionStats.create(effectiveRetentionStats),
            DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats.getGlobalRetentionStats(
                globalRetention,
                affectedByDefaultRetentionCounter,
                affectedByMaxRetentionCounter
            )
        );
        final DataStreamFeatureSetUsage usage = new DataStreamFeatureSetUsage(stats);
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }
}
