/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.action.XPackUsageFeatureAction.DATA_STREAMS;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamUsageTransportActionIT extends ESIntegTestCase {
    /*
     * The DataStreamUsageTransportAction is not exposed in the xpack core plugin, so we have a special test plugin to do this
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestDataStreamUsagePlugin.class);
    }

    @After
    private void cleanup() throws Exception {
        updateClusterState(clusterState -> {
            ClusterState.Builder clusterStateBuilder = new ClusterState.Builder(clusterState);
            Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
            metadataBuilder.dataStreams(Map.of(), Map.of());
            clusterStateBuilder.metadata(metadataBuilder);
            return clusterStateBuilder.build();
        });
        updateClusterSettings(
            Settings.builder()
                .putNull(DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey())
                .putNull(DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey())
        );
    }

    @SuppressWarnings("unchecked")
    public void testAction() throws Exception {
        // test empty results
        {
            Map<String, Object> map = getDataStreamUsage();
            assertThat(map.get("available"), equalTo(true));
            assertThat(map.get("enabled"), equalTo(true));
            assertThat(map.get("data_streams"), equalTo(0));
            assertThat(map.get("indices_count"), equalTo(0));

            Map<String, Object> failureStoreMap = (Map<String, Object>) map.get("failure_store");
            assertThat(failureStoreMap.get("explicitly_enabled_count"), equalTo(0));
            assertThat(failureStoreMap.get("effectively_enabled_count"), equalTo(0));
            assertThat(failureStoreMap.get("failure_indices_count"), equalTo(0));

            Map<String, Object> failuresLifecycleMap = (Map<String, Object>) failureStoreMap.get("lifecycle");
            assertThat(failuresLifecycleMap.get("explicitly_enabled_count"), equalTo(0));
            assertThat(failuresLifecycleMap.get("effectively_enabled_count"), equalTo(0));
            Map<String, Object> dataRetentionMap = (Map<String, Object>) failuresLifecycleMap.get("data_retention");
            assertThat(dataRetentionMap.size(), equalTo(1));
            assertThat(dataRetentionMap.get("configured_data_streams"), equalTo(0));

            Map<String, Object> effectiveRetentionMap = (Map<String, Object>) failuresLifecycleMap.get("effective_retention");
            assertThat(effectiveRetentionMap.size(), equalTo(1));
            assertThat(effectiveRetentionMap.get("retained_data_streams"), equalTo(0));

            Map<String, Object> globalRetentionMap = (Map<String, Object>) failuresLifecycleMap.get("global_retention");
            assertThat(globalRetentionMap.get("max"), equalTo(Map.of("defined", false)));
            assertThat(globalRetentionMap.get("default"), equalTo(Map.of("defined", false)));
        }

        // Keep track of the data streams created
        int dataStreamsCount = randomIntBetween(1, 200);
        AtomicInteger backingIndicesCount = new AtomicInteger(0);
        AtomicInteger failureIndicesCount = new AtomicInteger(0);
        AtomicInteger explicitlyEnabledFailureStoreCount = new AtomicInteger(0);
        AtomicInteger effectivelyEnabledFailureStoreCount = new AtomicInteger(0);
        AtomicInteger explicitlyEnabledFailuresLifecycleCount = new AtomicInteger(0);
        AtomicInteger effectivelyEnabledFailuresLifecycleCount = new AtomicInteger(0);
        AtomicInteger failuresLifecycleWithRetention = new AtomicInteger(0);
        AtomicInteger failuresLifecycleWithDefaultRetention = new AtomicInteger(0);

        AtomicLong totalRetentionTimes = new AtomicLong(0);
        AtomicLong minRetention = new AtomicLong(Long.MAX_VALUE);
        AtomicLong maxRetention = new AtomicLong(Long.MIN_VALUE);

        TimeValue defaultRetention = TimeValue.timeValueDays(10);
        Settings.Builder settingsBuilder = Settings.builder()
            .put(DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), "mis-*");
        boolean useDefaultRetention = randomBoolean();
        if (useDefaultRetention) {
            settingsBuilder.put(
                DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(),
                defaultRetention.getStringRep()
            );
        }
        updateClusterSettings(settingsBuilder);

        /*
         * We now add a number of simulated data streams to the cluster state. Some have failure store, some don't. The ones with failure
         * store may or may not have lifecycle with varying retention periods. After adding them, we make sure the numbers add up.
         */
        updateClusterState(clusterState -> {
            Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
            Map<String, DataStream> dataStreamMap = new HashMap<>();
            for (int i = 0; i < dataStreamsCount; i++) {
                boolean replicated = randomBoolean();
                boolean systemDataStream = rarely();
                List<Index> backingIndices = IntStream.range(0, randomIntBetween(1, 10))
                    .mapToObj(ignore -> new Index(randomAlphaOfLength(60), randomAlphaOfLength(60)))
                    .toList();
                backingIndicesCount.addAndGet(backingIndices.size());
                List<Index> failureIndices = IntStream.range(0, randomIntBetween(0, 10))
                    .mapToObj(ignore -> new Index(randomAlphaOfLength(60), randomAlphaOfLength(60)))
                    .toList();
                failureIndicesCount.addAndGet(failureIndices.size());
                Boolean failureStoreEnabled = randomBoolean() ? null : randomBoolean();
                boolean enabledBySetting = failureStoreEnabled == null && randomBoolean() && systemDataStream == false;
                if (failureStoreEnabled == null) {
                    if (enabledBySetting) {
                        effectivelyEnabledFailureStoreCount.incrementAndGet();
                    }
                } else if (failureStoreEnabled) {
                    explicitlyEnabledFailureStoreCount.incrementAndGet();
                    effectivelyEnabledFailureStoreCount.incrementAndGet();
                }
                DataStreamLifecycle lifecycle = randomBoolean()
                    ? null
                    : DataStreamLifecycle.createFailuresLifecycle(randomBoolean(), TimeValue.timeValueDays(randomIntBetween(1, 10)));
                if (lifecycle != null && lifecycle.enabled()) {
                    explicitlyEnabledFailuresLifecycleCount.incrementAndGet();
                    effectivelyEnabledFailuresLifecycleCount.incrementAndGet();
                    if (lifecycle.dataRetention() != null) {
                        long retentionMillis = lifecycle.dataRetention().getMillis();
                        totalRetentionTimes.addAndGet(retentionMillis);
                        failuresLifecycleWithRetention.incrementAndGet();
                        if (retentionMillis < minRetention.get()) {
                            minRetention.set(retentionMillis);
                        }
                        if (retentionMillis > maxRetention.get()) {
                            maxRetention.set(retentionMillis);
                        }
                    }
                }
                if (lifecycle == null
                    && (enabledBySetting || Boolean.TRUE.equals(failureStoreEnabled) || failureIndices.isEmpty() == false)) {
                    effectivelyEnabledFailuresLifecycleCount.incrementAndGet();
                    if (systemDataStream == false && useDefaultRetention) {
                        failuresLifecycleWithDefaultRetention.incrementAndGet();
                    }
                }
                DataStream dataStream = new DataStream(
                    enabledBySetting ? "mis-" + randomAlphaOfLength(10) : randomAlphaOfLength(50),
                    backingIndices,
                    randomLongBetween(0, 1000),
                    Map.of(),
                    systemDataStream || randomBoolean(),
                    replicated,
                    systemDataStream,
                    randomBoolean(),
                    IndexMode.STANDARD,
                    null,
                    failureStoreEnabled == null && lifecycle == null
                        ? DataStreamOptions.EMPTY
                        : new DataStreamOptions(new DataStreamFailureStore(failureStoreEnabled, lifecycle)),
                    failureIndices,
                    replicated == false && randomBoolean(),
                    null
                );
                dataStreamMap.put(dataStream.getName(), dataStream);
            }
            metadataBuilder.dataStreams(dataStreamMap, Map.of());
            ClusterState.Builder clusterStateBuilder = new ClusterState.Builder(clusterState);
            clusterStateBuilder.metadata(metadataBuilder);
            return clusterStateBuilder.build();
        });

        int retainedDataStreams = failuresLifecycleWithRetention.get() + failuresLifecycleWithDefaultRetention.get();

        int expectedMinimumDataRetention = minRetention.get() == Long.MAX_VALUE ? 0 : minRetention.intValue();
        int expectedMinimumEffectiveRetention = failuresLifecycleWithDefaultRetention.get() > 0
            ? (int) Math.min(minRetention.get(), defaultRetention.getMillis())
            : expectedMinimumDataRetention;

        int expectedMaximumDataRetention = maxRetention.get() == Long.MIN_VALUE ? 0 : maxRetention.intValue();
        int expectedMaximumEffectiveRetention = failuresLifecycleWithDefaultRetention.get() > 0
            ? (int) Math.max(maxRetention.get(), defaultRetention.getMillis())
            : expectedMaximumDataRetention;

        double expectedAverageDataRetention = failuresLifecycleWithRetention.get() == 0
            ? 0.0
            : totalRetentionTimes.doubleValue() / failuresLifecycleWithRetention.get();
        double expectedAverageEffectiveRetention = failuresLifecycleWithDefaultRetention.get() > 0
            ? (totalRetentionTimes.doubleValue() + failuresLifecycleWithDefaultRetention.get() * defaultRetention.getMillis())
                / retainedDataStreams
            : expectedAverageDataRetention;

        Map<String, Object> map = getDataStreamUsage();
        assertThat(map.get("available"), equalTo(true));
        assertThat(map.get("enabled"), equalTo(true));
        assertThat(map.get("data_streams"), equalTo(dataStreamsCount));
        assertThat(map.get("indices_count"), equalTo(backingIndicesCount.get()));

        Map<String, Object> failureStoreMap = (Map<String, Object>) map.get("failure_store");
        assertThat(failureStoreMap.get("explicitly_enabled_count"), equalTo(explicitlyEnabledFailureStoreCount.get()));
        assertThat(failureStoreMap.get("effectively_enabled_count"), equalTo(effectivelyEnabledFailureStoreCount.get()));
        assertThat(failureStoreMap.get("failure_indices_count"), equalTo(failureIndicesCount.get()));

        Map<String, Object> failuresLifecycleMap = (Map<String, Object>) failureStoreMap.get("lifecycle");
        assertThat(failuresLifecycleMap.get("explicitly_enabled_count"), equalTo(explicitlyEnabledFailuresLifecycleCount.get()));
        assertThat(failuresLifecycleMap.get("effectively_enabled_count"), equalTo(effectivelyEnabledFailuresLifecycleCount.get()));

        Map<String, Object> dataRetentionMap = (Map<String, Object>) failuresLifecycleMap.get("data_retention");
        assertThat(dataRetentionMap.get("configured_data_streams"), equalTo(failuresLifecycleWithRetention.get()));
        if (failuresLifecycleWithRetention.get() > 0) {
            assertThat(dataRetentionMap.get("minimum_millis"), equalTo(expectedMinimumDataRetention));
            assertThat(dataRetentionMap.get("maximum_millis"), equalTo(expectedMaximumDataRetention));
            assertThat(dataRetentionMap.get("average_millis"), equalTo(expectedAverageDataRetention));
        }

        Map<String, Object> effectiveRetentionMap = (Map<String, Object>) failuresLifecycleMap.get("effective_retention");
        assertThat(effectiveRetentionMap.get("retained_data_streams"), equalTo(retainedDataStreams));
        if (retainedDataStreams > 0) {
            assertThat(effectiveRetentionMap.get("minimum_millis"), equalTo(expectedMinimumEffectiveRetention));
            assertThat(effectiveRetentionMap.get("maximum_millis"), equalTo(expectedMaximumEffectiveRetention));
            assertThat(effectiveRetentionMap.get("average_millis"), equalTo(expectedAverageEffectiveRetention));
        }

        Map<String, Map<String, Object>> globalRetentionMap = (Map<String, Map<String, Object>>) failuresLifecycleMap.get(
            "global_retention"
        );
        assertThat(globalRetentionMap.get("max").get("defined"), equalTo(false));
        assertThat(globalRetentionMap.get("default").get("defined"), equalTo(useDefaultRetention));
        if (useDefaultRetention) {
            assertThat(
                globalRetentionMap.get("default").get("affected_data_streams"),
                equalTo(failuresLifecycleWithDefaultRetention.get())
            );
            assertThat(globalRetentionMap.get("default").get("retention_millis"), equalTo((int) defaultRetention.getMillis()));
        }
    }

    private Map<String, Object> getDataStreamUsage() throws IOException {
        XPackUsageFeatureResponse response = safeGet(client().execute(DATA_STREAMS, new XPackUsageRequest(SAFE_AWAIT_TIMEOUT)));
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
        clusterService.submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
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
        future.get();
    }

    /*
     * This plugin exposes the DataStreamUsageTransportAction.
     */
    public static final class TestDataStreamUsagePlugin extends XPackClientPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
            actions.add(new ActionPlugin.ActionHandler<>(DATA_STREAMS, DataStreamUsageTransportAction.class));
            return actions;
        }
    }
}
