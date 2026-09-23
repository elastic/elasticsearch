/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.TelemetryProvider.NoopTelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.stateless.cache.ShardWarmVolumes;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.SHARD_HAS_MOVED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SearchShardInformationIndexListenerTests extends ESTestCase {

    private final Index index = new Index("my_index", "uuid");
    private final ShardId shardId = new ShardId(index, 0);
    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
    private final TelemetryProvider telemetryProvider = new NoopTelemetryProvider() {
        @Override
        public MeterRegistry getMeterRegistry() {
            return meterRegistry;
        }
    };
    private final SearchShardInformationMetricsCollector collector = new SearchShardInformationMetricsCollector(telemetryProvider);
    private final IndexSettings indexSettings = new IndexSettings(
        IndexMetadata.builder(index.getName()).settings(indexSettings(IndexVersion.current(), 1, 1)).build(),
        Settings.EMPTY
    );
    private final ClusterSettings clusterSettings = new ClusterSettings(
        // Settings.EMPTY,
        Settings.builder().put(SearchShardInformationIndexListener.QUERY_SEARCH_SHARD_INFORMATION_SETTING.getKey(), true).build(),
        Set.of(SearchShardInformationIndexListener.QUERY_SEARCH_SHARD_INFORMATION_SETTING)
    );
    private final CountDownLatch latch = new CountDownLatch(1);
    private final LatchedActionListener<Void> latchedActionListener = new LatchedActionListener<>(ActionListener.noop(), latch);
    private final RecordingClient client = new RecordingClient();
    private final ClusterService clusterService = mock(ClusterService.class);
    private MockLog mockLog;

    @Before
    public void setupMockLogger() {
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        mockLog = MockLog.capture("org.elasticsearch.xpack.stateless.recovery.shardinfo.SearchShardInformationIndexListener");

        // ensure no error log messages have been run
        MockLog.UnseenEventExpectation noErrorLogExpectation = new MockLog.UnseenEventExpectation(
            "no ERROR",
            "org.elasticsearch.xpack.stateless.recovery.shardinfo.SearchShardInformationIndexListener",
            Level.ERROR,
            "*"
        );
        mockLog.addExpectation(noErrorLogExpectation);
    }

    @After
    public void ensureAfter() {
        // ensure listener was called
        assertThat(latch.getCount(), equalTo(0L));

        mockLog.assertAllExpectationsMatched();
        mockLog.close();
    }

    public void testShardIdIsPassed() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = newListener();

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        // ensure listener was called
        assertThat(latch.getCount(), equalTo(0L));

        assertThat(client.executionCount(), equalTo(1));
        RecordingClient.Execution<
            TransportFetchSearchShardInformationAction.Request,
            TransportFetchSearchShardInformationAction.Response> execution = client.lastExecution();
        assertThat(execution.action(), equalTo(TransportFetchSearchShardInformationAction.TYPE));
        assertThat(execution.request().getShardId(), equalTo(shardId));
        assertThat(execution.request().getNodeId(), nullValue());

        assertEmptyMetrics();
    }

    public void testRelocatingNodeIdIsSet() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1)
            .relocate("relocating_node_id", 1, ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = newListener();

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        assertThat(client.executionCount(), equalTo(1));
        RecordingClient.Execution<
            TransportFetchSearchShardInformationAction.Request,
            TransportFetchSearchShardInformationAction.Response> execution = client.lastExecution();
        assertThat(execution.action(), equalTo(TransportFetchSearchShardInformationAction.TYPE));
        assertThat(execution.request().getShardId(), equalTo(shardId));
        assertThat(execution.request().getNodeId(), equalTo("relocating_node_id"));

        assertEmptyMetrics();
    }

    public void testLastSearcherIsZero() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = newListener();

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        assertThat(client.executionCount(), equalTo(1));
        RecordingClient.Execution<
            TransportFetchSearchShardInformationAction.Request,
            TransportFetchSearchShardInformationAction.Response> execution = client.lastExecution();
        assertThat(execution.action(), equalTo(TransportFetchSearchShardInformationAction.TYPE));
        assertThat(execution.request().getShardId(), equalTo(shardId));

        reset(indexShard);
        execution.listener().onResponse(new TransportFetchSearchShardInformationAction.Response(0));
        // no interactions with the indexshard if acquired time is set to 0
        verifyNoInteractions(indexShard);

        assertMetrics(1, 0);
    }

    public void testResponseListenerFailure() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = newListener();

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        assertThat(client.executionCount(), equalTo(1));
        RecordingClient.Execution<
            TransportFetchSearchShardInformationAction.Request,
            TransportFetchSearchShardInformationAction.Response> execution = client.lastExecution();
        assertThat(execution.action(), equalTo(TransportFetchSearchShardInformationAction.TYPE));
        assertThat(execution.request().getShardId(), equalTo(shardId));

        reset(indexShard);
        execution.listener().onFailure(new RuntimeException("any error"));
        verify(indexShard, times(1)).shardId();
        verifyNoMoreInteractions(indexShard);
    }

    public void testNoSettingWhenIndexShardIsClosed() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = newListener();

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        assertThat(client.executionCount(), equalTo(1));
        RecordingClient.Execution<
            TransportFetchSearchShardInformationAction.Request,
            TransportFetchSearchShardInformationAction.Response> execution = client.lastExecution();
        assertThat(execution.action(), equalTo(TransportFetchSearchShardInformationAction.TYPE));
        assertThat(execution.request().getShardId(), equalTo(shardId));

        reset(indexShard);
        when(indexShard.state()).thenReturn(IndexShardState.CLOSED);
        execution.listener().onResponse(new TransportFetchSearchShardInformationAction.Response(123));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<Void>> voidListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        verify(indexShard).waitForEngineOrClosedShard(voidListenerCaptor.capture());
        ActionListener<Void> voidActionListener = voidListenerCaptor.getValue();
        voidActionListener.onResponse(null);

        verify(indexShard, never()).tryWithEngineOrNull(any());

        assertMetrics(1, 0);
    }

    public void testNoSettingWhenEngineListenerFails() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = newListener();

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        assertThat(client.executionCount(), equalTo(1));
        RecordingClient.Execution<
            TransportFetchSearchShardInformationAction.Request,
            TransportFetchSearchShardInformationAction.Response> execution = client.lastExecution();
        assertThat(execution.action(), equalTo(TransportFetchSearchShardInformationAction.TYPE));
        assertThat(execution.request().getShardId(), equalTo(shardId));

        reset(indexShard);
        when(indexShard.state()).thenReturn(IndexShardState.CLOSED);
        execution.listener().onResponse(new TransportFetchSearchShardInformationAction.Response(123));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<Void>> voidListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        verify(indexShard).waitForEngineOrClosedShard(voidListenerCaptor.capture());
        ActionListener<Void> voidActionListener = voidListenerCaptor.getValue();

        reset(indexShard);
        voidActionListener.onFailure(new RuntimeException("test exception"));
        verify(indexShard, times(1)).shardId();
        verifyNoMoreInteractions(indexShard);
    }

    public void testSearchEngineLastSearcherAcquiredTimeIsSet() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        AtomicLong time = new AtomicLong(0);
        LongSupplier controllableTime = time::get;

        SearchShardInformationIndexListener listener = newListener(controllableTime);

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        assertThat(client.executionCount(), equalTo(1));
        RecordingClient.Execution<
            TransportFetchSearchShardInformationAction.Request,
            TransportFetchSearchShardInformationAction.Response> execution = client.lastExecution();
        assertThat(execution.action(), equalTo(TransportFetchSearchShardInformationAction.TYPE));
        assertThat(execution.request().getShardId(), equalTo(shardId));

        reset(indexShard);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);

        // change time before execution
        time.addAndGet(20);
        execution.listener().onResponse(new TransportFetchSearchShardInformationAction.Response(123));

        // capture the `waitForEngineOrClosedShard()` argument and execute it
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<Void>> voidListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        verify(indexShard).waitForEngineOrClosedShard(voidListenerCaptor.capture());
        ActionListener<Void> voidActionListener = voidListenerCaptor.getValue();
        voidActionListener.onResponse(null);

        // capture the `tryWithEngineOrNull()` argument and execute it
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Function<Engine, Void>> functionCaptor = ArgumentCaptor.forClass(Function.class);
        verify(indexShard).tryWithEngineOrNull(functionCaptor.capture());
        Function<Engine, Void> function = functionCaptor.getValue();
        SearchEngine searchEngine = mock(SearchEngine.class);

        function.apply(searchEngine);

        verify(searchEngine).setLastSearcherAcquiredTime(eq(123L));

        assertMetrics(1, 0);
        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, SearchShardInformationMetricsCollector.REQUESTS_DURATION_HISTOGRAM);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.get(0).getLong(), equalTo(20L));
        assertThat(measurements.get(0).attributes(), hasEntry("es_search_last_searcher_acquired_greater_zero", true));
    }

    public void testSetInactive() {
        SearchShardInformationIndexListener listener = newListener();

        // simulate a settings update
        Settings newSettings = Settings.builder()
            .put(SearchShardInformationIndexListener.QUERY_SEARCH_SHARD_INFORMATION_SETTING.getKey(), false)
            .build();
        clusterSettings.applySettings(newSettings);

        IndexShard indexShard = mock(IndexShard.class);
        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        verifyNoInteractions(indexShard);
    }

    public void testSwitchBetweenActiveAndInactive() {
        SearchShardInformationIndexListener listener = newListener();
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        // simulate a settings update
        Settings settings = Settings.builder()
            .put(SearchShardInformationIndexListener.QUERY_SEARCH_SHARD_INFORMATION_SETTING.getKey(), true)
            .build();
        clusterSettings.applySettings(settings);

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        assertThat(client.executionCount(), equalTo(1));
        assertThat(client.lastExecution().action(), equalTo(TransportFetchSearchShardInformationAction.TYPE));

        // simulate a settings update
        Settings updatedSettings = Settings.builder()
            .put(SearchShardInformationIndexListener.QUERY_SEARCH_SHARD_INFORMATION_SETTING.getKey(), false)
            .build();
        clusterSettings.applySettings(updatedSettings);

        CountDownLatch updateSettingsLatch = new CountDownLatch(1);
        LatchedActionListener<Void> updateSettingsActionListener = new LatchedActionListener<>(ActionListener.noop(), latch);
        listener.beforeIndexShardRecovery(indexShard, indexSettings, updateSettingsActionListener);
        assertThat(client.executionCount(), equalTo(0));
        assertThat(updateSettingsLatch.getCount(), equalTo(1L));
    }

    public void testShardIsMovedResponse() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = newListener();

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        assertThat(client.executionCount(), equalTo(1));
        RecordingClient.Execution<
            TransportFetchSearchShardInformationAction.Request,
            TransportFetchSearchShardInformationAction.Response> execution = client.lastExecution();
        assertThat(execution.action(), equalTo(TransportFetchSearchShardInformationAction.TYPE));
        assertThat(execution.request().getShardId(), equalTo(shardId));

        reset(indexShard);
        execution.listener().onResponse(TransportFetchSearchShardInformationAction.SHARD_HAS_MOVED_RESPONSE);
        verify(indexShard, times(1)).shardId();
        verifyNoMoreInteractions(indexShard);

        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, SearchShardInformationMetricsCollector.REQUESTS_SHARD_MOVED_TOTAL);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.get(0).getLong(), equalTo(1L));
    }

    public void testPutHappensBeforeShardMovedSentinel() {
        long generation = randomNonNegativeLong();
        ClusterState state = drainState(index, "source", "target", generation);
        when(clusterService.state()).thenReturn(state);
        ShardWarmVolumes volumes = newVolumes(state);
        IndexShard indexShard = relocatingDrainShard("source");

        newListener(System::currentTimeMillis, volumes).beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        RecordingClient.Execution<
            TransportFetchSearchShardInformationAction.Request,
            TransportFetchSearchShardInformationAction.Response> execution = client.lastExecution();
        assertTrue(execution.request().wantVolumes());
        execution.listener()
            .onResponse(
                new TransportFetchSearchShardInformationAction.Response(SHARD_HAS_MOVED, "source", generation, Map.of(shardId, 11L))
            );

        assertThat(volumes.get(state, "source").volumes(), equalTo(Map.of(shardId, 11L)));
        verify(indexShard, never()).waitForEngineOrClosedShard(any());
        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, SearchShardInformationMetricsCollector.REQUESTS_SHARD_MOVED_TOTAL);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.get(0).getLong(), equalTo(1L));
    }

    public void testWrongResponderDoesNotLoop() {
        long sourceGen = 10L;
        long otherGen = 20L;
        ClusterState state = drainStateTwoSources(index, "source", sourceGen, "other", otherGen, "target");
        when(clusterService.state()).thenReturn(state);
        ShardWarmVolumes volumes = newVolumes(state);
        IndexShard indexShard = relocatingDrainShard("source");

        newListener(System::currentTimeMillis, volumes).beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);
        client.lastExecution()
            .listener()
            .onResponse(new TransportFetchSearchShardInformationAction.Response(5L, "other", otherGen, Map.of(shardId, 3L)));

        assertThat(volumes.get(state, "other").volumes(), equalTo(Map.of(shardId, 3L)));
        assertThat(volumes.get(state, "source"), nullValue());
        assertFalse(volumes.claimFetch(state, "source"));
    }

    public void testDidNotCollectClearsClaimForRetry() {
        long generation = randomNonNegativeLong();
        ClusterState state = drainState(index, "source", "target", generation);
        when(clusterService.state()).thenReturn(state);
        ShardWarmVolumes volumes = newVolumes(state);
        IndexShard indexShard = relocatingDrainShard("source");

        newListener(System::currentTimeMillis, volumes).beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);
        RecordingClient.Execution<
            TransportFetchSearchShardInformationAction.Request,
            TransportFetchSearchShardInformationAction.Response> execution = client.lastExecution();
        assertTrue(execution.request().wantVolumes());
        execution.listener().onResponse(new TransportFetchSearchShardInformationAction.Response(5L));
        assertThat(volumes.get(state, "source"), nullValue());
        assertTrue(volumes.claimFetch(state, "source"));
    }

    public void testFailureRecordsFetchMetricAndRetries() {
        long generation = randomNonNegativeLong();
        ClusterState state = drainState(index, "source", "target", generation);
        when(clusterService.state()).thenReturn(state);
        ShardWarmVolumes volumes = newVolumes(state);
        IndexShard indexShard = relocatingDrainShard("source");

        newListener(System::currentTimeMillis, volumes).beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);
        client.lastExecution().listener().onFailure(new RuntimeException("rpc failed"));
        assertMetrics(0, 1);
        assertTrue(volumes.claimFetch(state, "source"));
    }

    private SearchShardInformationIndexListener newListener() {
        return newListener(System::currentTimeMillis, ShardWarmVolumes.NOOP);
    }

    private SearchShardInformationIndexListener newListener(LongSupplier now) {
        return newListener(now, ShardWarmVolumes.NOOP);
    }

    private SearchShardInformationIndexListener newListener(LongSupplier now, ShardWarmVolumes volumes) {
        return new SearchShardInformationIndexListener(client, collector, clusterSettings, now, clusterService, volumes);
    }

    private IndexShard relocatingDrainShard(String sourceId) {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = TestShardRouting.shardRoutingBuilder(shardId, "target", false, INITIALIZING)
            .withRelocatingNodeId(sourceId)
            .withRole(ShardRouting.Role.SEARCH_ONLY)
            .build();
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);
        return indexShard;
    }

    private static ShardWarmVolumes newVolumes(ClusterState state) {
        ClusterService volumesClusterService = mock(ClusterService.class);
        when(volumesClusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                Settings.builder()
                    .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_WARM_VOLUMES_ENABLED_SETTING.getKey(), true)
                    .build(),
                Set.of(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_WARM_VOLUMES_ENABLED_SETTING)
            )
        );
        when(volumesClusterService.state()).thenReturn(state);
        return new ShardWarmVolumes(volumesClusterService);
    }

    private static ClusterState drainState(Index index, String sourceNodeId, String targetNodeId, long startedAtMillis) {
        return drainStateTwoSources(index, sourceNodeId, startedAtMillis, null, 0L, targetNodeId);
    }

    private static ClusterState drainStateTwoSources(
        Index index,
        String sourceNodeId,
        long sourceGen,
        String otherNodeId,
        long otherGen,
        String targetNodeId
    ) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), index.getUUID(), 1, 1))
            .build();
        Map<String, SingleNodeShutdownMetadata> shutdowns = new HashMap<>();
        shutdowns.put(
            sourceNodeId,
            SingleNodeShutdownMetadata.builder()
                .setNodeId(sourceNodeId)
                .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                .setReason("test")
                .setStartedAtMillis(sourceGen)
                .setNodeSeen(true)
                .build()
        );
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create(sourceNodeId))
            .add(DiscoveryNodeUtils.create(targetNodeId))
            .localNodeId(targetNodeId)
            .masterNodeId(targetNodeId);
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"))
            .putCompatibilityVersions(sourceNodeId, TransportVersion.current(), Map.of())
            .putCompatibilityVersions(targetNodeId, TransportVersion.current(), Map.of());
        if (otherNodeId != null) {
            shutdowns.put(
                otherNodeId,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(otherNodeId)
                    .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                    .setReason("test")
                    .setStartedAtMillis(otherGen)
                    .setNodeSeen(true)
                    .build()
            );
            nodes.add(DiscoveryNodeUtils.create(otherNodeId));
            state.putCompatibilityVersions(otherNodeId, TransportVersion.current(), Map.of());
        }
        return state.nodes(nodes.build())
            .metadata(
                Metadata.builder()
                    .putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdowns))
                    .put(ProjectMetadata.builder(DEFAULT_PROJECT_ID).put(indexMetadata, false))
                    .build()
            )
            .build();
    }

    private void assertMetrics(long expectedSuccessfulRequests, long expectedErrors) {
        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, SearchShardInformationMetricsCollector.REQUESTS_SUCCESS_TOTAL);
        if (expectedSuccessfulRequests > 0) {
            assertThat(measurements, hasSize(1));
            assertThat(measurements.get(0).getLong(), equalTo(expectedSuccessfulRequests));
        } else {
            assertThat(measurements, hasSize(0));
        }

        measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, SearchShardInformationMetricsCollector.REQUESTS_ERRORS_TOTAL);
        if (expectedErrors > 0) {
            assertThat(measurements, hasSize(1));
            assertThat(measurements.get(0).getLong(), equalTo(expectedErrors));
        } else {
            assertThat(measurements, hasSize(0));
        }
    }

    private void assertEmptyMetrics() {
        assertMetrics(0, 0);
        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, SearchShardInformationMetricsCollector.REQUESTS_DURATION_HISTOGRAM);
        assertThat(measurements, hasSize(0));
    }

    private ShardRouting createSearchOnlyShard(ShardId shardId, String nodeId) {
        return ShardRouting.newUnassigned(
            shardId,
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
            ShardRouting.Role.SEARCH_ONLY,
            ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
        ).initialize(nodeId, null, randomNonNegativeLong());
    }

    private static class RecordingClient extends AbstractClient {

        record Execution<Request extends ActionRequest, Response extends ActionResponse>(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {}

        private final List<Execution<?, ?>> executions = new ArrayList<>();

        RecordingClient() {
            super(Settings.EMPTY, null, null);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            executions.add(new Execution<>(action, request, listener));
        }

        int executionCount() {
            return executions.size();
        }

        @SuppressWarnings("unchecked")
        <Request extends ActionRequest, Response extends ActionResponse> Execution<Request, Response> lastExecution() {
            return (Execution<Request, Response>) executions.removeLast();
        }
    }
}
