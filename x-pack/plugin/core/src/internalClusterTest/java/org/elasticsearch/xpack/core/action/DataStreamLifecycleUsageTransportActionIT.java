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
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            Settings.builder().put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), (String) null)
        );
    }

    @SuppressWarnings("unchecked")
    public void testAction() throws Exception {
        assertUsageResults(0, 0, 0, 0.0, true);
        AtomicLong count = new AtomicLong(0);
        AtomicLong totalRetentionTimes = new AtomicLong(0);
        AtomicLong minRetention = new AtomicLong(Long.MAX_VALUE);
        AtomicLong maxRetention = new AtomicLong(Long.MIN_VALUE);
        boolean useDefaultRolloverConfig = randomBoolean();
        if (useDefaultRolloverConfig == false) {
            updateClusterSettings(
                Settings.builder().put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=33")
            );
        }
        /*
         * We now add a number of simulated data streams to the cluster state. Some have lifecycles, some don't. The ones with lifecycles
         * have varying retention periods. After adding them, we make sure the numbers add up.
         */
        updateClusterState(clusterState -> {
            Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
            Map<String, DataStream> dataStreamMap = new HashMap<>();
            for (int dataStreamCount = 0; dataStreamCount < randomInt(200); dataStreamCount++) {
                boolean hasLifecycle = randomBoolean();
                long retentionMillis;
                if (hasLifecycle) {
                    retentionMillis = randomLongBetween(1000, 100000);
                    count.incrementAndGet();
                    totalRetentionTimes.addAndGet(retentionMillis);
                    if (retentionMillis < minRetention.get()) {
                        minRetention.set(retentionMillis);
                    }
                    if (retentionMillis > maxRetention.get()) {
                        maxRetention.set(retentionMillis);
                    }
                } else {
                    retentionMillis = 0;
                }
                List<Index> indices = new ArrayList<>();
                for (int indicesCount = 0; indicesCount < randomIntBetween(1, 10); indicesCount++) {
                    Index index = new Index(randomAlphaOfLength(60), randomAlphaOfLength(60));
                    indices.add(index);
                }
                boolean systemDataStream = randomBoolean();
                DataStream dataStream = new DataStream(
                    randomAlphaOfLength(50),
                    indices,
                    randomLongBetween(0, 1000),
                    Map.of(),
                    systemDataStream || randomBoolean(),
                    randomBoolean(),
                    systemDataStream,
                    randomBoolean(),
                    IndexMode.STANDARD,
                    hasLifecycle ? new DataStreamLifecycle(retentionMillis) : null
                );
                dataStreamMap.put(dataStream.getName(), dataStream);
            }
            Map<String, DataStreamAlias> dataStreamAliasesMap = Map.of();
            metadataBuilder.dataStreams(dataStreamMap, dataStreamAliasesMap);
            ClusterState.Builder clusterStateBuilder = new ClusterState.Builder(clusterState);
            clusterStateBuilder.metadata(metadataBuilder);
            return clusterStateBuilder.build();
        });
        int expectedMinimumRetention = minRetention.get() == Long.MAX_VALUE ? 0 : minRetention.intValue();
        int expectedMaximumRetention = maxRetention.get() == Long.MIN_VALUE ? 0 : maxRetention.intValue();
        double expectedAverageRetention = count.get() == 0 ? 0.0 : totalRetentionTimes.doubleValue() / count.get();
        assertUsageResults(
            count.intValue(),
            expectedMinimumRetention,
            expectedMaximumRetention,
            expectedAverageRetention,
            useDefaultRolloverConfig
        );
    }

    @SuppressWarnings("unchecked")
    private void assertUsageResults(
        int count,
        int minimumRetention,
        int maximumRetention,
        double averageRetention,
        boolean defaultRolloverUsed
    ) throws Exception {
        XPackUsageFeatureResponse response = client().execute(DATA_STREAM_LIFECYCLE, new XPackUsageRequest()).get();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = response.getUsage().toXContent(builder, ToXContent.EMPTY_PARAMS);
        Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(
            BytesReference.bytes(builder),
            true,
            XContentType.JSON
        );

        Map<String, Object> map = tuple.v2();
        assertThat(map.get("available"), equalTo(true));
        assertThat(map.get("enabled"), equalTo(true));
        assertThat(map.get("count"), equalTo(count));
        assertThat(map.get("default_rollover_used"), equalTo(defaultRolloverUsed));
        Map<String, Object> retentionMap = (Map<String, Object>) map.get("retention");
        assertThat(retentionMap.size(), equalTo(3));
        assertThat(retentionMap.get("minimum_millis"), equalTo(minimumRetention));
        assertThat(retentionMap.get("maximum_millis"), equalTo(maximumRetention));
        assertThat(retentionMap.get("average_millis"), equalTo(averageRetention));
    }

    /*
     * Updates the cluster state in the internal cluster using the provided function
     */
    protected static void updateClusterState(final Function<ClusterState, ClusterState> updater) throws Exception {
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
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
