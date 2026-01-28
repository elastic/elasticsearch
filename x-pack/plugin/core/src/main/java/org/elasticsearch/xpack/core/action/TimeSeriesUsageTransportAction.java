/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.downsample.DownsampleConfig;
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
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

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
            Integer dlmRounds = ds.getDataLifecycle() == null || ds.getDataLifecycle().downsamplingRounds() == null
                ? null
                : ds.getDataLifecycle().downsamplingRounds().size();

            for (Index backingIndex : ds.getIndices()) {
                IndexMetadata indexMetadata = projectMetadata.index(backingIndex);
                if (indexMetadata.getIndexMode() != IndexMode.TIME_SERIES) {
                    continue;
                }
                tsIndexCount++;
                if (ds.isIndexManagedByDataStreamLifecycle(indexMetadata.getIndex(), ignored -> indexMetadata) && dlmRounds != null) {
                    dlmStats.trackIndex(ds, indexMetadata);
                    dlmStats.trackRounds(dlmRounds, ds, indexMetadata);
                    dlmStats.trackSamplingMethod(ds.getDataLifecycle().downsamplingMethod(), ds, indexMetadata);
                } else if (ilmAvailable && projectMetadata.isIndexManagedByILM(indexMetadata)) {
                    LifecyclePolicyMetadata policyMetadata = ilmMetadata.getPolicyMetadatas().get(indexMetadata.getLifecyclePolicyName());
                    if (policyMetadata == null) {
                        continue;
                    }
                    int rounds = 0;
                    DownsampleConfig.SamplingMethod samplingMethod = null;
                    for (Phase phase : policyMetadata.getPolicy().getPhases().values()) {
                        if (phase.getActions().containsKey(DownsampleAction.NAME)) {
                            rounds++;
                            samplingMethod = ((DownsampleAction) phase.getActions().get(DownsampleAction.NAME)).samplingMethod();
                        }
                    }
                    if (rounds > 0) {
                        ilmStats.trackPolicy(policyMetadata.getPolicy());
                        ilmStats.trackIndex(ds, indexMetadata);
                        ilmStats.trackRounds(rounds, ds, indexMetadata);
                        ilmStats.trackSamplingMethod(samplingMethod, ds, indexMetadata);
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
                ilmStats.calculateIlmPolicyStats(),
                dlmStats.getDownsamplingStats(),
                indicesByInterval
            )
            : new TimeSeriesFeatureSetUsage(tsDataStreamCount, tsIndexCount, dlmStats.getDownsamplingStats(), indicesByInterval);
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }

    private static class DownsamplingStatsTracker {
        private long downsampledDataStreams = 0;
        private long downsampledIndices = 0;
        private long aggregateSamplingMethod = 0;
        private long lastValueSamplingMethod = 0;
        private long undefinedSamplingMethod = 0;
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

        void trackSamplingMethod(DownsampleConfig.SamplingMethod samplingMethod, DataStream ds, IndexMetadata indexMetadata) {
            // We want to track the sampling method per data stream,
            // so we use the write index to determine the active lifecycle configuration
            if (Objects.equals(indexMetadata.getIndex(), ds.getWriteIndex())) {
                if (samplingMethod == null) {
                    undefinedSamplingMethod++;
                    return;
                }
                switch (samplingMethod) {
                    case DownsampleConfig.SamplingMethod.AGGREGATE -> aggregateSamplingMethod++;
                    case DownsampleConfig.SamplingMethod.LAST_VALUE -> lastValueSamplingMethod++;
                }
            }
        }

        TimeSeriesFeatureSetUsage.DownsamplingFeatureStats getDownsamplingStats() {
            return new TimeSeriesFeatureSetUsage.DownsamplingFeatureStats(
                downsampledDataStreams,
                downsampledIndices,
                rounds.getMin(),
                rounds.getAverage(),
                rounds.getMax(),
                aggregateSamplingMethod,
                lastValueSamplingMethod,
                undefinedSamplingMethod
            );
        }
    }

    /**
     * Tracks the ILM policies currently in use by time series data streams.
     */
    static class IlmDownsamplingStatsTracker extends DownsamplingStatsTracker {
        private final Map<String, Map<String, Phase>> policies = new HashMap<>();

        void trackPolicy(LifecyclePolicy ilmPolicy) {
            policies.putIfAbsent(ilmPolicy.getName(), ilmPolicy.getPhases());
        }

        /**
         * Calculates ILM-policy-specific statistics that help us get a better understanding on the phases that use downsampling and on
         * how the force merge step in the downsample action is used. More specifically, for downsampling we are tracking:
         * - if users explicitly enabled or disabled force-merge after downsampling.
         * - if the force merge could be skipped with minimal impact, when the force merge flag is undefined.
         * @return a IlmPolicyStats record that contains these counters.
         */
        TimeSeriesFeatureSetUsage.IlmPolicyStats calculateIlmPolicyStats() {
            if (policies.isEmpty()) {
                return TimeSeriesFeatureSetUsage.IlmPolicyStats.EMPTY;
            }
            long forceMergeExplicitlyEnabledCounter = 0;
            long forceMergeExplicitlyDisabledCounter = 0;
            long forceMergeDefaultCounter = 0;
            long downsampledForceMergeNeededCounter = 0; // Meaning it's followed by a searchable snapshot with force-merge index false
            Map<String, Long> downsamplingPhases = new HashMap<>();
            for (String ilmPolicy : policies.keySet()) {
                Map<String, Phase> phases = policies.get(ilmPolicy);
                boolean downsampledForceMergeNeeded = false;
                for (String phase : TimeseriesLifecycleType.ORDERED_VALID_PHASES) {
                    if (phases.containsKey(phase) == false) {
                        continue;
                    }
                    Map<String, LifecycleAction> actions = phases.get(phase).getActions();
                    if (actions.containsKey(DownsampleAction.NAME)) {
                        // count the phase used
                        Long current = downsamplingPhases.computeIfAbsent(phase, ignored -> 0L);
                        downsamplingPhases.put(phase, current + 1);
                        // Count force merge
                        DownsampleAction downsampleAction = (DownsampleAction) actions.get(DownsampleAction.NAME);
                        if (downsampleAction.forceMergeIndex() == null) {
                            forceMergeDefaultCounter++;
                            downsampledForceMergeNeeded = true; // this default force merge could be needed depending on the following steps
                        } else if (downsampleAction.forceMergeIndex()) {
                            forceMergeExplicitlyEnabledCounter++;
                        } else {
                            forceMergeExplicitlyDisabledCounter++;
                        }
                    }

                    // If there is an explicit force merge action, we could consider the downsampling force merge redundant.
                    if (actions.containsKey(ForceMergeAction.NAME)) {
                        downsampledForceMergeNeeded = false;
                    }
                    if (downsampledForceMergeNeeded && actions.containsKey(SearchableSnapshotAction.NAME)) {
                        SearchableSnapshotAction searchableSnapshotAction = (SearchableSnapshotAction) actions.get(
                            SearchableSnapshotAction.NAME
                        );
                        if (searchableSnapshotAction.isForceMergeIndex() == false) {
                            // If there was a searchable snapshot with force index false, then the downsample force merge has impact
                            downsampledForceMergeNeededCounter++;
                        }
                        downsampledForceMergeNeeded = false;
                    }
                }
            }
            return new TimeSeriesFeatureSetUsage.IlmPolicyStats(
                downsamplingPhases,
                forceMergeExplicitlyEnabledCounter,
                forceMergeExplicitlyDisabledCounter,
                forceMergeDefaultCounter,
                downsampledForceMergeNeededCounter
            );
        }
    }
}
