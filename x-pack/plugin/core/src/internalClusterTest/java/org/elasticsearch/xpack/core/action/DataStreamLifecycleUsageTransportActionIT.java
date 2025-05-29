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
import org.elasticsearch.cluster.metadata.DataStreamAlias;
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

import static org.elasticsearch.xpack.core.action.XPackUsageFeatureAction.DATA_STREAM_LIFECYCLE;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamLifecycleUsageTransportActionIT extends ESIntegTestCase {
    /*
     * The DataLifecycleUsageTransportAction is not exposed in the xpack core plugin, so we have a special test plugin to do this
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestDateLifecycleUsagePlugin.class);
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
                .putNull(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey())
                .putNull(DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey())
        );
    }

    @SuppressWarnings("unchecked")
    public void testAction() throws Exception {
        // test empty results
        {
            Map<String, Object> map = getLifecycleUsage();
            assertThat(map.get("available"), equalTo(true));
            assertThat(map.get("enabled"), equalTo(true));
            assertThat(map.get("count"), equalTo(0));
            assertThat(map.get("default_rollover_used"), equalTo(true));

            Map<String, Object> dataRetentionMap = (Map<String, Object>) map.get("data_retention");
            assertThat(dataRetentionMap.size(), equalTo(1));
            assertThat(dataRetentionMap.get("configured_data_streams"), equalTo(0));

            Map<String, Object> effectiveRetentionMap = (Map<String, Object>) map.get("effective_retention");
            assertThat(effectiveRetentionMap.size(), equalTo(1));
            assertThat(effectiveRetentionMap.get("retained_data_streams"), equalTo(0));

            Map<String, Object> globalRetentionMap = (Map<String, Object>) map.get("global_retention");
            assertThat(globalRetentionMap.get("max"), equalTo(Map.of("defined", false)));
            assertThat(globalRetentionMap.get("default"), equalTo(Map.of("defined", false)));
        }

        // Keep track of the data streams created
        AtomicInteger dataStreamsWithLifecycleCount = new AtomicInteger(0);
        AtomicInteger dataStreamsWithRetentionCount = new AtomicInteger(0);
        AtomicInteger dataStreamsWithDefaultRetentionCount = new AtomicInteger(0);

        AtomicLong totalRetentionTimes = new AtomicLong(0);
        AtomicLong minRetention = new AtomicLong(Long.MAX_VALUE);
        AtomicLong maxRetention = new AtomicLong(Long.MIN_VALUE);

        boolean useDefaultRolloverConfig = randomBoolean();
        if (useDefaultRolloverConfig == false) {
            updateClusterSettings(
                Settings.builder().put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=33")
            );
        }
        TimeValue defaultRetention = TimeValue.timeValueDays(10);
        boolean useDefaultRetention = randomBoolean();
        if (useDefaultRetention) {
            updateClusterSettings(
                Settings.builder()
                    .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(), defaultRetention.getStringRep())
            );
        }
        /*
         * We now add a number of simulated data streams to the cluster state. Some have lifecycles, some don't. The ones with lifecycles
         * have varying retention periods. After adding them, we make sure the numbers add up.
         */
        updateClusterState(clusterState -> {
            Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
            Map<String, DataStream> dataStreamMap = new HashMap<>();
            boolean atLeastOne = false;
            for (int dataStreamCount = 0; dataStreamCount < randomIntBetween(1, 200); dataStreamCount++) {
                boolean hasLifecycle = randomBoolean() || atLeastOne == false;
                DataStreamLifecycle lifecycle;
                boolean systemDataStream = rarely();
                if (hasLifecycle) {
                    if (randomBoolean()) {
                        lifecycle = DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE;
                        dataStreamsWithLifecycleCount.incrementAndGet();
                        if (useDefaultRetention && systemDataStream == false) {
                            dataStreamsWithDefaultRetentionCount.incrementAndGet();
                        }
                        atLeastOne = true;
                    } else {
                        long retentionMillis = randomLongBetween(1000, 100000);
                        boolean isEnabled = randomBoolean() || atLeastOne == false;
                        if (isEnabled) {
                            dataStreamsWithLifecycleCount.incrementAndGet();
                            dataStreamsWithRetentionCount.incrementAndGet();
                            totalRetentionTimes.addAndGet(retentionMillis);

                            if (retentionMillis < minRetention.get()) {
                                minRetention.set(retentionMillis);
                            }
                            if (retentionMillis > maxRetention.get()) {
                                maxRetention.set(retentionMillis);
                            }
                            atLeastOne = true;
                        }
                        lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
                            .dataRetention(TimeValue.timeValueMillis(retentionMillis))
                            .enabled(isEnabled)
                            .build();
                    }
                } else {
                    lifecycle = null;
                }
                List<Index> indices = new ArrayList<>();
                for (int indicesCount = 0; indicesCount < randomIntBetween(1, 10); indicesCount++) {
                    Index index = new Index(randomAlphaOfLength(60), randomAlphaOfLength(60));
                    indices.add(index);
                }
                boolean replicated = randomBoolean();
                DataStream dataStream = new DataStream(
                    randomAlphaOfLength(50),
                    indices,
                    randomLongBetween(0, 1000),
                    Map.of(),
                    systemDataStream || randomBoolean(),
                    replicated,
                    systemDataStream,
                    randomBoolean(),
                    IndexMode.STANDARD,
                    lifecycle,
                    DataStreamOptions.EMPTY,
                    List.of(),
                    replicated == false && randomBoolean(),
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

        int retainedDataStreams = dataStreamsWithRetentionCount.get() + dataStreamsWithDefaultRetentionCount.get();

        int expectedMinimumDataRetention = minRetention.get() == Long.MAX_VALUE ? 0 : minRetention.intValue();
        int expectedMinimumEffectiveRetention = dataStreamsWithDefaultRetentionCount.get() > 0
            ? (int) Math.min(minRetention.get(), defaultRetention.getMillis())
            : expectedMinimumDataRetention;

        int expectedMaximumDataRetention = maxRetention.get() == Long.MIN_VALUE ? 0 : maxRetention.intValue();
        int expectedMaximumEffectiveRetention = dataStreamsWithDefaultRetentionCount.get() > 0
            ? (int) Math.max(maxRetention.get(), defaultRetention.getMillis())
            : expectedMaximumDataRetention;

        double expectedAverageDataRetention = dataStreamsWithRetentionCount.get() == 0
            ? 0.0
            : totalRetentionTimes.doubleValue() / dataStreamsWithRetentionCount.get();
        double expectedAverageEffectiveRetention = dataStreamsWithDefaultRetentionCount.get() > 0
            ? (totalRetentionTimes.doubleValue() + dataStreamsWithDefaultRetentionCount.get() * defaultRetention.getMillis())
                / retainedDataStreams
            : expectedAverageDataRetention;

        Map<String, Object> map = getLifecycleUsage();
        assertThat(map.get("available"), equalTo(true));
        assertThat(map.get("enabled"), equalTo(true));
        assertThat(map.get("count"), equalTo(dataStreamsWithLifecycleCount.intValue()));
        assertThat(map.get("default_rollover_used"), equalTo(useDefaultRolloverConfig));

        Map<String, Object> dataRetentionMap = (Map<String, Object>) map.get("data_retention");
        assertThat(dataRetentionMap.get("configured_data_streams"), equalTo(dataStreamsWithRetentionCount.get()));
        if (dataStreamsWithRetentionCount.get() > 0) {
            assertThat(dataRetentionMap.get("minimum_millis"), equalTo(expectedMinimumDataRetention));
            assertThat(dataRetentionMap.get("maximum_millis"), equalTo(expectedMaximumDataRetention));
            assertThat(dataRetentionMap.get("average_millis"), equalTo(expectedAverageDataRetention));
        }

        Map<String, Object> effectieRetentionMap = (Map<String, Object>) map.get("effective_retention");
        assertThat(effectieRetentionMap.get("retained_data_streams"), equalTo(retainedDataStreams));
        if (retainedDataStreams > 0) {
            assertThat(effectieRetentionMap.get("minimum_millis"), equalTo(expectedMinimumEffectiveRetention));
            assertThat(effectieRetentionMap.get("maximum_millis"), equalTo(expectedMaximumEffectiveRetention));
            assertThat(effectieRetentionMap.get("average_millis"), equalTo(expectedAverageEffectiveRetention));
        }

        Map<String, Map<String, Object>> globalRetentionMap = (Map<String, Map<String, Object>>) map.get("global_retention");
        assertThat(globalRetentionMap.get("max").get("defined"), equalTo(false));
        assertThat(globalRetentionMap.get("default").get("defined"), equalTo(useDefaultRetention));
        if (useDefaultRetention) {
            assertThat(globalRetentionMap.get("default").get("affected_data_streams"), equalTo(dataStreamsWithDefaultRetentionCount.get()));
            assertThat(globalRetentionMap.get("default").get("retention_millis"), equalTo((int) defaultRetention.getMillis()));
        }
    }

    private Map<String, Object> getLifecycleUsage() throws IOException {
        XPackUsageFeatureResponse response = safeGet(client().execute(DATA_STREAM_LIFECYCLE, new XPackUsageRequest(SAFE_AWAIT_TIMEOUT)));
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
     * This plugin exposes the DataLifecycleUsageTransportAction.
     */
    public static final class TestDateLifecycleUsagePlugin extends XPackClientPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
            actions.add(new ActionPlugin.ActionHandler<>(DATA_STREAM_LIFECYCLE, DataStreamLifecycleUsageTransportAction.class));
            return actions;
        }
    }
}
