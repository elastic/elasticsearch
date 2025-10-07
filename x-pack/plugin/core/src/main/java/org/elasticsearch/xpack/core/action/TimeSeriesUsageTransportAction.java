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
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.datastreams.TimeSeriesFeatureSetUsage;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.Phase;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;

/**
 * Exposes the time series telemetry via the xpack usage API. We track the following only for time series data streams:
 * - time series data stream count
 * - time series backing indices of these time series data streams
 * - the feature that downsamples the time series data streams, we use the write index to avoid resolving templates,
 * this might cause a small delay in the counters (backing indices, downsampling rounds).
 * - For ILM specifically, we count the phases that have configured downsampling in the policies used in the time series data streams.
 * - When elasticsearch is running in DLM only mode, we skip all the ILM metrics.
 */
public class TimeSeriesUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final ProjectResolver projectResolver;
    private final boolean ilmAvailable;

    @Inject
    public TimeSeriesUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(XPackUsageFeatureAction.TIME_SERIES_DATA_STREAMS.name(), transportService, clusterService, threadPool, actionFilters);
        this.projectResolver = projectResolver;
        this.ilmAvailable = DataStreamLifecycle.isDataStreamsLifecycleOnlyMode(clusterService.getSettings()) == false;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(state);
        IndexLifecycleMetadata ilmMetadata = projectMetadata.custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        final Map<String, DataStream> dataStreams = projectMetadata.dataStreams();

        long tsDataStreamCount = 0;
        long tsIndexCount = 0;
        IlmDownsamplingStatsTracker ilmStats = ilmAvailable ? new IlmDownsamplingStatsTracker() : null;
        DownsamplingStatsTracker dlmStats = new DownsamplingStatsTracker();
        Map<String, Long> indicesByInterval = new HashMap<>();

        for (DataStream ds : dataStreams.values()) {
            // We choose to not count time series backing indices that do not belong to a time series data stream.
            if (ds.getIndexMode() != IndexMode.TIME_SERIES) {
                continue;
            }
            tsDataStreamCount++;
            Integer dlmRounds = ds.getDataLifecycle() == null || ds.getDataLifecycle().downsampling() == null
                ? null
                : ds.getDataLifecycle().downsampling().size();

            for (Index backingIndex : ds.getIndices()) {
                IndexMetadata indexMetadata = projectMetadata.index(backingIndex);
                if (indexMetadata.getIndexMode() != IndexMode.TIME_SERIES) {
                    continue;
                }
                tsIndexCount++;
                if (ds.isIndexManagedByDataStreamLifecycle(indexMetadata.getIndex(), ignored -> indexMetadata) && dlmRounds != null) {
                    dlmStats.trackIndex(ds, indexMetadata);
                    dlmStats.trackRounds(dlmRounds, ds, indexMetadata);
                } else if (ilmAvailable && projectMetadata.isIndexManagedByILM(indexMetadata)) {
                    LifecyclePolicyMetadata policyMetadata = ilmMetadata.getPolicyMetadatas().get(indexMetadata.getLifecyclePolicyName());
                    if (policyMetadata == null) {
                        continue;
                    }
                    int rounds = 0;
                    for (Phase phase : policyMetadata.getPolicy().getPhases().values()) {
                        if (phase.getActions().containsKey(DownsampleAction.NAME)) {
                            rounds++;
                        }
                    }
                    if (rounds > 0) {
                        ilmStats.trackPolicy(policyMetadata.getPolicy());
                        ilmStats.trackIndex(ds, indexMetadata);
                        ilmStats.trackRounds(rounds, ds, indexMetadata);
                    }
                }
                String interval = indexMetadata.getSettings().get(IndexMetadata.INDEX_DOWNSAMPLE_INTERVAL.getKey());
                if (interval != null) {
                    Long count = indicesByInterval.computeIfAbsent(interval, ignored -> 0L);
                    indicesByInterval.put(interval, count + 1);
                }
            }
        }

        final TimeSeriesFeatureSetUsage usage = ilmAvailable
            ? new TimeSeriesFeatureSetUsage(
                tsDataStreamCount,
                tsIndexCount,
                ilmStats.getDownsamplingStats(),
                ilmStats.getIlmPolicyStats(),
                dlmStats.getDownsamplingStats(),
                indicesByInterval
            )
            : new TimeSeriesFeatureSetUsage(tsDataStreamCount, tsIndexCount, dlmStats.getDownsamplingStats(), indicesByInterval);
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }

    private static class DownsamplingStatsTracker {
        private long downsampledDataStreams = 0;
        private long downsampledIndices = 0;
        private final LongSummaryStatistics rounds = new LongSummaryStatistics();

        void trackIndex(DataStream ds, IndexMetadata indexMetadata) {
            if (Objects.equals(indexMetadata.getIndex(), ds.getWriteIndex())) {
                downsampledDataStreams++;
            }
            downsampledIndices++;
        }

        void trackRounds(int rounds, DataStream ds, IndexMetadata indexMetadata) {
            // We want to track rounds per data stream, so we use the write index to determine the
            // rounds applicable for this data stream
            if (Objects.equals(indexMetadata.getIndex(), ds.getWriteIndex())) {
                this.rounds.accept(rounds);
            }
        }

        TimeSeriesFeatureSetUsage.DownsamplingFeatureStats getDownsamplingStats() {
            return new TimeSeriesFeatureSetUsage.DownsamplingFeatureStats(
                downsampledDataStreams,
                downsampledIndices,
                rounds.getMin(),
                rounds.getAverage(),
                rounds.getMax()
            );
        }
    }

    static class IlmDownsamplingStatsTracker extends DownsamplingStatsTracker {
        private final Map<String, Map<String, Phase>> policies = new HashMap<>();

        void trackPolicy(LifecyclePolicy ilmPolicy) {
            policies.putIfAbsent(ilmPolicy.getName(), ilmPolicy.getPhases());
        }

        Map<String, Long> getIlmPolicyStats() {
            if (policies.isEmpty()) {
                return Map.of();
            }
            Map<String, Long> downsamplingPhases = new HashMap<>();
            for (String ilmPolicy : policies.keySet()) {
                for (Phase phase : policies.get(ilmPolicy).values()) {
                    if (phase.getActions().containsKey(DownsampleAction.NAME)) {
                        Long current = downsamplingPhases.computeIfAbsent(phase.getName(), ignored -> 0L);
                        downsamplingPhases.put(phase.getName(), current + 1);
                    }
                }
            }
            return downsamplingPhases;
        }
    }
}
