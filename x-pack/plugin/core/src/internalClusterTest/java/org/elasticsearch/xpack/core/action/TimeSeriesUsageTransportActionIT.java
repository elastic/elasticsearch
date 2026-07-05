/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_DOWNSAMPLE_INTERVAL_KEY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.xpack.core.action.XPackUsageFeatureAction.TIME_SERIES_DATA_STREAMS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TimeSeriesUsageTransportActionIT extends ESIntegTestCase {
    private static final String DOWNSAMPLING_IN_HOT_POLICY = "hot-downsampling-policy";
    private static final String DOWNSAMPLING_IN_WARM_COLD_POLICY = "warm-cold-downsampling-policy";
    private static final String NO_DOWNSAMPLING_POLICY = "no-downsampling-policy";
    private static final DateFormatter FORMATTER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
    private static final double LIKELIHOOD = 0.8;

    /*
     * The TimeSeriesUsageTransportAction is not exposed in the xpack core plugin, so we have a special test plugin to do this.
     * We also need to include the DataStreamsPlugin so the data streams will be properly removed by the tests' teardown.
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, TestTimeSeriesUsagePlugin.class);
    }

    public void testActionNoTimeSeriesDataStreams() throws Exception {
        // If the templates are not used, they shouldn't influence the telemetry
        if (randomBoolean()) {
            updateClusterState(clusterState -> {
                Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
                addIlmPolicies(metadataBuilder);
                ClusterState.Builder clusterStateBuilder = new ClusterState.Builder(clusterState);
                clusterStateBuilder.metadata(metadataBuilder);
                return clusterStateBuilder.build();
            });
        }
        Map<String, Object> map = getTimeSeriesUsage();
        assertThat(map.get("available"), equalTo(true));
        assertThat(map.get("enabled"), equalTo(true));
        assertThat(map.get("data_stream_count"), equalTo(0));
        assertThat(map.get("index_count"), nullValue());
        assertThat(map.get("downsampling"), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testAction() throws Exception {

        // Expected counters
        // Time series
        var timeSeriesDataStreamCount = new AtomicInteger(0);
        var timeSeriesIndexCount = new AtomicInteger(0);

        // Downsampling
        var downsampled5mIndexCount = new AtomicInteger(0);
        var downsampled1hIndexCount = new AtomicInteger(0);
        var downsampled1dIndexCount = new AtomicInteger(0);

        // ... with DLM
        var dlmDownsampledDataStreamCount = new AtomicInteger(0);
        var dlmDownsampledIndexCount = new AtomicInteger(0);
        var dlmRoundsCount = new AtomicInteger(0);
        var dlmRoundsSum = new AtomicInteger(0);
        var dlmRoundsMin = new AtomicInteger(Integer.MAX_VALUE);
        var dlmRoundsMax = new AtomicInteger(Integer.MIN_VALUE);
        var dlmAggregateSamplingMethodCount = new AtomicInteger(0);
        var dlmLastValueSamplingMethodCount = new AtomicInteger(0);
        var dlmUndefinedSamplingMethodCount = new AtomicInteger(0);

        // ... with ILM
        var ilmDownsampledDataStreamCount = new AtomicInteger(0);
        var ilmDownsampledIndexCount = new AtomicInteger(0);
        var ilmRoundsCount = new AtomicInteger(0);
        var ilmRoundsSum = new AtomicInteger(0);
        var ilmRoundsMin = new AtomicInteger(Integer.MAX_VALUE);
        var ilmRoundsMax = new AtomicInteger(Integer.MIN_VALUE);
        var ilmAggregateSamplingMethodCount = new AtomicInteger(0);
        var ilmLastValueSamplingMethodCount = new AtomicInteger(0);
        var ilmUndefinedSamplingMethodCount = new AtomicInteger(0);
        Set<String> usedPolicies = new HashSet<>();
        var ilmPolicySpecs = new AtomicReference<IlmPolicySpecs>();

        /*
         * We now add a number of simulated data streams to the cluster state. We mix different combinations of:
         * - time series and standard data streams & backing indices
         * - DLM with or without downsampling
         * - ILM with or without downsampling
         */
        updateClusterState(clusterState -> {
            Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
            ilmPolicySpecs.set(addIlmPolicies(metadataBuilder));

            Map<String, DataStream> dataStreamMap = new HashMap<>();
            for (int dataStreamCount = 0; dataStreamCount < randomIntBetween(10, 100); dataStreamCount++) {
                String dataStreamName = randomAlphaOfLength(50);
                boolean isTimeSeriesDataStream = randomBoolean();
                // this flag refers to configuration, a non tsds is also possible to have downsampling
                // configuration, but it is skipped.
                var downsamplingConfiguredBy = randomFrom(DownsampledBy.values());
                boolean isDownsampled = downsamplingConfiguredBy != DownsampledBy.NONE && isTimeSeriesDataStream;
                // An index/data stream can have both ILM & DLM configured; by default, ILM "wins"
                boolean hasLifecycle = likely() || (isDownsampled && downsamplingConfiguredBy == DownsampledBy.DLM);
                boolean hasIlm = downsamplingConfiguredBy == DownsampledBy.ILM;

                // Replicated data stream should be counted because ILM works independently.
                // DLM is not supported yet with CCR.
                boolean isReplicated = hasIlm && randomBoolean();
                DataStreamLifecycle lifecycle = maybeCreateLifecycle(isDownsampled, hasLifecycle);

                if (isTimeSeriesDataStream) {
                    timeSeriesDataStreamCount.incrementAndGet();
                    if (downsamplingConfiguredBy == DownsampledBy.DLM) {
                        dlmDownsampledDataStreamCount.incrementAndGet();
                        updateRounds(lifecycle.downsamplingRounds().size(), dlmRoundsCount, dlmRoundsSum, dlmRoundsMin, dlmRoundsMax);
                        DownsampleConfig.SamplingMethod samplingMethod = lifecycle.downsamplingMethod();
                        if (samplingMethod == null) {
                            dlmUndefinedSamplingMethodCount.incrementAndGet();
                        } else if (samplingMethod == DownsampleConfig.SamplingMethod.AGGREGATE) {
                            dlmAggregateSamplingMethodCount.incrementAndGet();
                        } else if (samplingMethod == DownsampleConfig.SamplingMethod.LAST_VALUE) {
                            dlmLastValueSamplingMethodCount.incrementAndGet();
                        }
                    } else if (downsamplingConfiguredBy == DownsampledBy.ILM) {
                        ilmDownsampledDataStreamCount.incrementAndGet();
                    }
                }

                int backingIndexCount = randomIntBetween(1, 10);
                List<Index> backingIndices = new ArrayList<>(backingIndexCount);
                Instant startTime = Instant.now().minus(randomInt(10), ChronoUnit.DAYS);
                for (int i = 0; i < backingIndexCount; i++) {
                    boolean isWriteIndex = i == backingIndexCount - 1;
                    Instant endTime = startTime.plus(randomIntBetween(1, 100), ChronoUnit.HOURS);

                    // Prepare settings
                    Settings.Builder settingsBuilder = settings(IndexVersion.current()).put("index.hidden", true)
                        .put(SETTING_INDEX_UUID, randomUUID());

                    // We do not override DLM if this is the write index
                    boolean ovewrittenDlm = isWriteIndex == false && rarely() && downsamplingConfiguredBy == DownsampledBy.DLM;
                    String policy = randomIlmPolicy(downsamplingConfiguredBy, ovewrittenDlm);
                    if (policy != null) {
                        settingsBuilder.put("index.lifecycle.name", policy);
                    }

                    // We capture that usually time series data streams have time series indices
                    // and non-time series data streams have non-time-series indices, but it can
                    // happen the other way around too
                    if (isTimeSeriesDataStream && (isWriteIndex || likely()) || isTimeSeriesDataStream == false && rarely()) {
                        settingsBuilder.put("index.mode", "time_series")
                            .put("index.time_series.start_time", FORMATTER.format(startTime))
                            .put("index.time_series.end_time", FORMATTER.format(endTime))
                            .put("index.routing_path", "uid");

                        // The write index cannot be downsampled
                        // only time series indices can be downsampled
                        if (isWriteIndex == false && isTimeSeriesDataStream) {
                            switch (randomIntBetween(0, 3)) {
                                case 0 -> {
                                    settingsBuilder.put(INDEX_DOWNSAMPLE_INTERVAL_KEY, "5m");
                                    downsampled5mIndexCount.incrementAndGet();
                                }
                                case 1 -> {
                                    settingsBuilder.put(INDEX_DOWNSAMPLE_INTERVAL_KEY, "1h");
                                    downsampled1hIndexCount.incrementAndGet();
                                }
                                case 2 -> {
                                    settingsBuilder.put(INDEX_DOWNSAMPLE_INTERVAL_KEY, "1d");
                                    downsampled1dIndexCount.incrementAndGet();
                                }
                                default -> {
                                }
                            }
                        }
                        // If the data stream is not time series we do not count the backing indices.
                        if (isTimeSeriesDataStream) {
                            timeSeriesIndexCount.incrementAndGet();
                            if (downsamplingConfiguredBy == DownsampledBy.ILM || ovewrittenDlm) {
                                ilmDownsampledIndexCount.incrementAndGet();
                                usedPolicies.add(policy);
                                if (isWriteIndex) {
                                    updateRounds(
                                        DOWNSAMPLING_IN_HOT_POLICY.equals(policy) ? 1 : 2,
                                        ilmRoundsCount,
                                        ilmRoundsSum,
                                        ilmRoundsMin,
                                        ilmRoundsMax
                                    );

                                    DownsampleConfig.SamplingMethod samplingMethod = ilmPolicySpecs.get()
                                        .samplingMethodByPolicy()
                                        .get(policy);
                                    if (samplingMethod == null) {
                                        ilmUndefinedSamplingMethodCount.incrementAndGet();
                                    } else if (samplingMethod == DownsampleConfig.SamplingMethod.AGGREGATE) {
                                        ilmAggregateSamplingMethodCount.incrementAndGet();
                                    } else if (samplingMethod == DownsampleConfig.SamplingMethod.LAST_VALUE) {
                                        ilmLastValueSamplingMethodCount.incrementAndGet();
                                    }
                                }
                            } else if (downsamplingConfiguredBy == DownsampledBy.DLM) {
                                dlmDownsampledIndexCount.incrementAndGet();
                            }
                        }
                    }
                    IndexMetadata indexMetadata = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, i + 1))
                        .settings(settingsBuilder)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .build();
                    startTime = endTime;
                    backingIndices.add(indexMetadata.getIndex());
                    metadataBuilder.put(indexMetadata, true);
                }
                DataStream dataStream = new DataStream(
                    dataStreamName,
                    backingIndices,
                    randomLongBetween(0, 1000),
                    Map.of(),
                    randomBoolean(),
                    isReplicated,
                    false,
                    randomBoolean(),
                    isTimeSeriesDataStream ? IndexMode.TIME_SERIES : IndexMode.STANDARD,
                    lifecycle,
                    DataStreamOptions.EMPTY,
                    List.of(),
                    isReplicated == false && randomBoolean(),
                    null
                );
                dataStreamMap.put(dataStream.getName(), dataStream);
            }
            Map<String, DataStreamAlias> dataStreamAliasesMap = Map.of();
            metadataBuilder.dataStreams(dataStreamMap, dataStreamAliasesMap);
            ClusterState.Builder clusterStateBuilder = new ClusterState.Builder(clusterState);
            clusterStateBuilder.metadata(metadataBuilder);
            return clusterStateBuilder.build();
        });

        Map<String, Object> map = getTimeSeriesUsage();
        assertThat(map.get("available"), equalTo(true));
        assertThat(map.get("enabled"), equalTo(true));
        assertThat(map.get("data_stream_count"), equalTo(timeSeriesDataStreamCount.get()));
        if (timeSeriesDataStreamCount.get() == 0) {
            assertThat(map.get("index_count"), nullValue());
            assertThat(map.get("downsampling"), nullValue());
        } else {
            assertThat(map.get("index_count"), equalTo(timeSeriesIndexCount.get()));
            assertThat(map.containsKey("downsampling"), equalTo(true));
            Map<String, Object> downsamplingMap = (Map<String, Object>) map.get("downsampling");

            // Downsampled indices
            assertDownsampledIndices(
                downsamplingMap,
                downsampled5mIndexCount.get(),
                downsampled1hIndexCount.get(),
                downsampled1dIndexCount.get()
            );

            // DLM
            assertThat(downsamplingMap.containsKey("dlm"), equalTo(true));
            assertDownsamplingStats(
                (Map<String, Object>) downsamplingMap.get("dlm"),
                dlmDownsampledDataStreamCount.get(),
                dlmDownsampledIndexCount.get(),
                dlmRoundsCount.get(),
                dlmRoundsSum.get(),
                dlmRoundsMin.get(),
                dlmRoundsMax.get(),
                dlmAggregateSamplingMethodCount.get(),
                dlmLastValueSamplingMethodCount.get(),
                dlmUndefinedSamplingMethodCount.get()
            );

            // ILM
            assertThat(downsamplingMap.containsKey("ilm"), equalTo(true));
            Map<String, Object> ilmStats = (Map<String, Object>) downsamplingMap.get("ilm");
            assertDownsamplingStats(
                ilmStats,
                ilmDownsampledDataStreamCount.get(),
                ilmDownsampledIndexCount.get(),
                ilmRoundsCount.get(),
                ilmRoundsSum.get(),
                ilmRoundsMin.get(),
                ilmRoundsMax.get(),
                ilmAggregateSamplingMethodCount.get(),
                ilmLastValueSamplingMethodCount.get(),
                ilmUndefinedSamplingMethodCount.get()
            );
            var explicitlyEnabled = new AtomicInteger(0);
            var explicitlyDisabled = new AtomicInteger(0);
            var undefined = new AtomicInteger(0);
            Map<String, Object> phasesStats = (Map<String, Object>) ilmStats.get("phases_in_use");
            if (usedPolicies.contains(DOWNSAMPLING_IN_HOT_POLICY)) {
                assertThat(phasesStats.get("hot"), equalTo(1));
                updateForceMergeCounters(ilmPolicySpecs.get().enabledInHot, explicitlyEnabled, explicitlyDisabled, undefined);
            } else {
                assertThat(phasesStats.get("hot"), nullValue());
            }
            if (usedPolicies.contains(DOWNSAMPLING_IN_WARM_COLD_POLICY)) {
                assertThat(phasesStats.get("warm"), equalTo(1));
                updateForceMergeCounters(ilmPolicySpecs.get().enabledInWarm, explicitlyEnabled, explicitlyDisabled, undefined);
                assertThat(phasesStats.get("cold"), equalTo(1));
                updateForceMergeCounters(ilmPolicySpecs.get().enabledInCold, explicitlyEnabled, explicitlyDisabled, undefined);
            } else {
                assertThat(phasesStats.get("warm"), nullValue());
                assertThat(phasesStats.get("cold"), nullValue());
            }
            Map<String, Object> forceMergeStats = (Map<String, Object>) ilmStats.get("force_merge");
            assertThat((int) forceMergeStats.get("explicitly_enabled_count"), equalTo(explicitlyEnabled.get()));
            assertThat(forceMergeStats.get("explicitly_disabled_count"), equalTo(explicitlyDisabled.get()));
            assertThat(forceMergeStats.get("undefined_count"), equalTo(undefined.get()));
            assertThat(forceMergeStats.get("undefined_force_merge_needed_count"), equalTo(0));
        }
    }

    @SuppressWarnings("unchecked")
    private static void assertDownsampledIndices(
        Map<String, Object> downsamplingMap,
        int downsampled5mIndexCount,
        int downsampled1hIndexCount,
        int downsampled1dIndexCount
    ) {
        if (downsampled5mIndexCount > 0 || downsampled1hIndexCount > 0 || downsampled1dIndexCount > 0) {
            assertThat(downsamplingMap.containsKey("index_count_per_interval"), equalTo(true));
            Map<String, Object> downsampledIndicesMap = (Map<String, Object>) downsamplingMap.get("index_count_per_interval");
            if (downsampled5mIndexCount > 0) {
                assertThat(downsampledIndicesMap.get("5m"), equalTo(downsampled5mIndexCount));
            } else {
                assertThat(downsampledIndicesMap.get("5m"), nullValue());
            }
            if (downsampled1hIndexCount > 0) {
                assertThat(downsampledIndicesMap.get("1h"), equalTo(downsampled1hIndexCount));
            } else {
                assertThat(downsampledIndicesMap.get("1h"), nullValue());
            }
            if (downsampled1dIndexCount > 0) {
                assertThat(downsampledIndicesMap.get("1d"), equalTo(downsampled1dIndexCount));
            } else {
                assertThat(downsampledIndicesMap.get("1d"), nullValue());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void assertDownsamplingStats(
        Map<String, Object> stats,
        Integer downsampledDataStreamCount,
        Integer downsampledIndexCount,
        Integer roundsCount,
        Integer roundsSum,
        Integer roundsMin,
        Integer roundsMax,
        Integer aggregateSamplingMethod,
        Integer lastValueSamplingMethod,
        Integer undefinedSamplingMethod
    ) {
        assertThat(stats.get("downsampled_data_stream_count"), equalTo(downsampledDataStreamCount));
        if (downsampledDataStreamCount == 0) {
            assertThat(stats.get("downsampled_index_count"), nullValue());
            assertThat(stats.get("rounds_per_data_stream"), nullValue());
            assertThat(stats.get("sampling_method"), nullValue());
        } else {
            assertThat(stats.get("downsampled_index_count"), equalTo(downsampledIndexCount));
            assertThat(stats.containsKey("rounds_per_data_stream"), equalTo(true));
            Map<String, Object> roundsMap = (Map<String, Object>) stats.get("rounds_per_data_stream");
            assertThat(roundsMap.get("average"), equalTo((double) roundsSum / roundsCount));
            assertThat(roundsMap.get("min"), equalTo(roundsMin));
            assertThat(roundsMap.get("max"), equalTo(roundsMax));
            assertThat(stats.containsKey("sampling_method"), equalTo(true));
            Map<String, Object> samplingMethodMap = (Map<String, Object>) stats.get("sampling_method");
            assertThat(samplingMethodMap.get("aggregate"), equalTo(aggregateSamplingMethod));
            assertThat(samplingMethodMap.get("last_value"), equalTo(lastValueSamplingMethod));
            assertThat(samplingMethodMap.get("undefined"), equalTo(undefinedSamplingMethod));
        }
    }

    private void updateForceMergeCounters(Boolean value, AtomicInteger enabled, AtomicInteger disabled, AtomicInteger undefined) {
        if (value == null) {
            undefined.incrementAndGet();
        } else if (value) {
            enabled.incrementAndGet();
        } else {
            disabled.incrementAndGet();
        }
    }

    private DataStreamLifecycle maybeCreateLifecycle(boolean isDownsampled, boolean hasDlm) {
        if (hasDlm == false) {
            return null;
        }
        var builder = DataStreamLifecycle.dataLifecycleBuilder();
        if (isDownsampled) {
            builder.downsamplingRounds(randomDownsamplingRounds());
        }
        return builder.build();
    }

    private void updateRounds(
        int size,
        AtomicInteger roundsCount,
        AtomicInteger roundsSum,
        AtomicInteger roundsMin,
        AtomicInteger roundsMax
    ) {
        roundsCount.incrementAndGet();
        roundsSum.addAndGet(size);
        roundsMin.updateAndGet(v -> Math.min(v, size));
        roundsMax.updateAndGet(v -> Math.max(v, size));
    }

    private Map<String, Object> getTimeSeriesUsage() throws IOException {
        XPackUsageFeatureResponse response = safeGet(client().execute(TIME_SERIES_DATA_STREAMS, new XPackUsageRequest(SAFE_AWAIT_TIMEOUT)));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = response.getUsage().toXContent(builder, ToXContent.EMPTY_PARAMS);
        Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(
            BytesReference.bytes(builder),
            true,
            XContentType.JSON
        );
        return tuple.v2();
    }

    /*
     * Updates the cluster state in the internal cluster using the provided function
     */
    protected static void updateClusterState(final Function<ClusterState, ClusterState> updater) throws Exception {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();

        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        clusterService.submitUnbatchedStateUpdateTask("time-series-usage-test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return updater.apply(currentState);
            }

            @Override
            public void onFailure(Exception e) {
                future.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                future.onResponse(null);
            }
        });
        safeGet(future);
    }

    private List<DataStreamLifecycle.DownsamplingRound> randomDownsamplingRounds() {
        List<DataStreamLifecycle.DownsamplingRound> rounds = new ArrayList<>();
        int minutes = 5;
        int days = 1;
        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            rounds.add(new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueDays(days), new DateHistogramInterval(minutes + "m")));
            minutes *= randomIntBetween(2, 5);
            days += randomIntBetween(1, 5);
        }
        return rounds;
    }

    private IlmPolicySpecs addIlmPolicies(Metadata.Builder metadataBuilder) {
        Boolean hotForceMergeEnabled = randomBoolean() ? randomBoolean() : null;
        Boolean warmForceMergeEnabled = randomBoolean() ? randomBoolean() : null;
        Boolean coldForceMergeEnabled = randomBoolean() ? randomBoolean() : null;
        DownsampleConfig.SamplingMethod samplingMethod1 = randomeSamplingMethod();
        DownsampleConfig.SamplingMethod samplingMethod2 = randomeSamplingMethod();
        List<LifecyclePolicy> policies = List.of(
            new LifecyclePolicy(
                DOWNSAMPLING_IN_HOT_POLICY,
                Map.of(
                    "hot",
                    new Phase(
                        "hot",
                        TimeValue.ZERO,
                        Map.of(
                            "downsample",
                            new DownsampleAction(DateHistogramInterval.MINUTE, null, hotForceMergeEnabled, samplingMethod1)
                        )
                    )
                )
            ),
            new LifecyclePolicy(
                DOWNSAMPLING_IN_WARM_COLD_POLICY,
                Map.of(
                    "warm",
                    new Phase(
                        "warm",
                        TimeValue.ZERO,
                        Map.of("downsample", new DownsampleAction(DateHistogramInterval.HOUR, null, warmForceMergeEnabled, samplingMethod2))
                    ),
                    "cold",
                    new Phase(
                        "cold",
                        TimeValue.timeValueDays(3),
                        Map.of("downsample", new DownsampleAction(DateHistogramInterval.DAY, null, coldForceMergeEnabled, samplingMethod2))
                    )
                )
            ),
            new LifecyclePolicy(
                NO_DOWNSAMPLING_POLICY,
                Map.of("hot", new Phase("hot", TimeValue.ZERO, Map.of("read-only", new ReadOnlyAction())))
            )
        );
        Map<String, LifecyclePolicyMetadata> policyMetadata = policies.stream()
            .collect(
                Collectors.toMap(
                    LifecyclePolicy::getName,
                    lifecyclePolicy -> new LifecyclePolicyMetadata(lifecyclePolicy, Map.of(), 1, Instant.now().toEpochMilli())
                )
            );
        IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(policyMetadata, OperationMode.RUNNING);
        metadataBuilder.putCustom(IndexLifecycleMetadata.TYPE, newMetadata);
        var samplingMethods = new HashMap<String, DownsampleConfig.SamplingMethod>(2);
        if (samplingMethod1 != null) {
            samplingMethods.put(DOWNSAMPLING_IN_HOT_POLICY, samplingMethod1);
        }
        if (samplingMethod2 != null) {
            samplingMethods.put(DOWNSAMPLING_IN_WARM_COLD_POLICY, samplingMethod2);
        }
        return new IlmPolicySpecs(hotForceMergeEnabled, warmForceMergeEnabled, coldForceMergeEnabled, samplingMethods);
    }

    private static DownsampleConfig.SamplingMethod randomeSamplingMethod() {
        return randomBoolean() ? null : randomFrom(DownsampleConfig.SamplingMethod.values());
    }

    private record IlmPolicySpecs(
        Boolean enabledInHot,
        Boolean enabledInWarm,
        Boolean enabledInCold,
        Map<String, DownsampleConfig.SamplingMethod> samplingMethodByPolicy
    ) {}

    private static String randomIlmPolicy(DownsampledBy downsampledBy, boolean ovewrittenDlm) {
        if (downsampledBy == DownsampledBy.ILM || (downsampledBy == DownsampledBy.DLM && ovewrittenDlm)) {
            return randomFrom(DOWNSAMPLING_IN_HOT_POLICY, DOWNSAMPLING_IN_WARM_COLD_POLICY);
        }
        if (downsampledBy == DownsampledBy.NONE && likely()) {
            return NO_DOWNSAMPLING_POLICY;
        }
        return null;
    }

    private static boolean likely() {
        return randomDoubleBetween(0.0, 1.0, true) < LIKELIHOOD;
    }

    private enum DownsampledBy {
        DLM,
        ILM,
        NONE
    }

    /*
     * This plugin exposes the TimeSeriesUsageTransportAction.
     */
    public static final class TestTimeSeriesUsagePlugin extends XPackClientPlugin {

        @Override
        public List<ActionHandler> getActions() {
            return List.of(new ActionHandler(TIME_SERIES_DATA_STREAMS, TimeSeriesUsageTransportAction.class));
        }
    }
}
