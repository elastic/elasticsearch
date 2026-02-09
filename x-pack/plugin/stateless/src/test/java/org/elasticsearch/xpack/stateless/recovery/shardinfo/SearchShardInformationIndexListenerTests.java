/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.junit.After;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class SearchShardInformationIndexListenerTests extends ESTestCase {

    private final Index index = new Index("my_index", "uuid");
    private final ShardId shardId = new ShardId(index, 0);
    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
    private final TelemetryProvider telemetryProvider = new TelemetryProvider() {
        @Override
        public Tracer getTracer() {
            return Tracer.NOOP;
        }

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
    private final Client client = mock(Client.class);

    @After
    public void ensureLatchedListenerWasCalled() {
        assertThat(latch.getCount(), equalTo(0L));
    }

    public void testShardIdIsPassed() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = new SearchShardInformationIndexListener(
            client,
            collector,
            clusterSettings,
            System::currentTimeMillis
        );

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        // ensure listener was called
        assertThat(latch.getCount(), equalTo(0L));

        ArgumentCaptor<ActionRequest> argument = ArgumentCaptor.forClass(ActionRequest.class);
        verify(client).execute(eq(TransportFetchSearchShardInformationAction.TYPE), argument.capture(), any());
        assertThat(argument.getValue(), instanceOf(TransportFetchSearchShardInformationAction.Request.class));

        TransportFetchSearchShardInformationAction.Request request = (TransportFetchSearchShardInformationAction.Request) argument
            .getValue();
        assertThat(request.getShardId(), equalTo(shardId));
        assertThat(request.getNodeId(), nullValue());

        assertEmptyMetrics();
    }

    public void testRelocatingNodeIdIsSet() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1).relocate("relocating_node_id", 1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = new SearchShardInformationIndexListener(
            client,
            collector,
            clusterSettings,
            System::currentTimeMillis
        );

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        ArgumentCaptor<ActionRequest> argument = ArgumentCaptor.forClass(ActionRequest.class);
        verify(client).execute(eq(TransportFetchSearchShardInformationAction.TYPE), argument.capture(), any());
        assertThat(argument.getValue(), instanceOf(TransportFetchSearchShardInformationAction.Request.class));

        TransportFetchSearchShardInformationAction.Request request = (TransportFetchSearchShardInformationAction.Request) argument
            .getValue();
        assertThat(request.getShardId(), equalTo(shardId));
        assertThat(request.getNodeId(), equalTo("relocating_node_id"));

        assertEmptyMetrics();
    }

    public void testLastSearcherIsZero() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = new SearchShardInformationIndexListener(
            client,
            collector,
            clusterSettings,
            System::currentTimeMillis
        );

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        ArgumentCaptor<ActionRequest> argument = ArgumentCaptor.forClass(ActionRequest.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<TransportFetchSearchShardInformationAction.Response>> listenerCaptor = ArgumentCaptor.forClass(
            ActionListener.class
        );
        verify(client).execute(eq(TransportFetchSearchShardInformationAction.TYPE), argument.capture(), listenerCaptor.capture());
        assertThat(argument.getValue(), instanceOf(TransportFetchSearchShardInformationAction.Request.class));

        TransportFetchSearchShardInformationAction.Request request = (TransportFetchSearchShardInformationAction.Request) argument
            .getValue();
        assertThat(request.getShardId(), equalTo(shardId));

        ActionListener<TransportFetchSearchShardInformationAction.Response> responseListener = listenerCaptor.getValue();

        reset(indexShard);
        responseListener.onResponse(new TransportFetchSearchShardInformationAction.Response(0));
        // no interactions with the indexshard if acquired time is set to 0
        verifyNoInteractions(indexShard);

        assertMetrics(1, 0);
    }

    public void testNoSettingWhenIndexShardIsClosed() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = new SearchShardInformationIndexListener(
            client,
            collector,
            clusterSettings,
            System::currentTimeMillis
        );

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        ArgumentCaptor<ActionRequest> argument = ArgumentCaptor.forClass(ActionRequest.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<TransportFetchSearchShardInformationAction.Response>> listenerCaptor = ArgumentCaptor.forClass(
            ActionListener.class
        );
        verify(client).execute(eq(TransportFetchSearchShardInformationAction.TYPE), argument.capture(), listenerCaptor.capture());
        assertThat(argument.getValue(), instanceOf(TransportFetchSearchShardInformationAction.Request.class));

        TransportFetchSearchShardInformationAction.Request request = (TransportFetchSearchShardInformationAction.Request) argument
            .getValue();
        assertThat(request.getShardId(), equalTo(shardId));

        ActionListener<TransportFetchSearchShardInformationAction.Response> responseListener = listenerCaptor.getValue();

        reset(indexShard);
        when(indexShard.state()).thenReturn(IndexShardState.CLOSED);
        responseListener.onResponse(new TransportFetchSearchShardInformationAction.Response(123));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<Void>> voidListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        verify(indexShard).waitForEngineOrClosedShard(voidListenerCaptor.capture());
        ActionListener<Void> voidActionListener = voidListenerCaptor.getValue();
        voidActionListener.onResponse(null);

        verify(indexShard, never()).tryWithEngineOrNull(any());

        assertMetrics(1, 0);
    }

    public void testSearchEngineLastSearcherAcquiredTimeIsSet() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        AtomicLong time = new AtomicLong(0);
        LongSupplier controllableTime = time::get;

        SearchShardInformationIndexListener listener = new SearchShardInformationIndexListener(
            client,
            collector,
            clusterSettings,
            controllableTime
        );

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        ArgumentCaptor<ActionRequest> argument = ArgumentCaptor.forClass(ActionRequest.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<TransportFetchSearchShardInformationAction.Response>> listenerCaptor = ArgumentCaptor.forClass(
            ActionListener.class
        );
        verify(client).execute(eq(TransportFetchSearchShardInformationAction.TYPE), argument.capture(), listenerCaptor.capture());
        assertThat(argument.getValue(), instanceOf(TransportFetchSearchShardInformationAction.Request.class));

        TransportFetchSearchShardInformationAction.Request request = (TransportFetchSearchShardInformationAction.Request) argument
            .getValue();
        assertThat(request.getShardId(), equalTo(shardId));

        ActionListener<TransportFetchSearchShardInformationAction.Response> responseListener = listenerCaptor.getValue();

        reset(indexShard);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);

        // change time before execution
        time.addAndGet(20);
        responseListener.onResponse(new TransportFetchSearchShardInformationAction.Response(123));

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
        SearchShardInformationIndexListener listener = new SearchShardInformationIndexListener(
            client,
            collector,
            clusterSettings,
            System::currentTimeMillis
        );

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
        SearchShardInformationIndexListener listener = new SearchShardInformationIndexListener(
            client,
            collector,
            clusterSettings,
            System::currentTimeMillis
        );
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

        verify(client).execute(eq(TransportFetchSearchShardInformationAction.TYPE), any(), any());

        // simulate a settings update
        Settings updatedSettings = Settings.builder()
            .put(SearchShardInformationIndexListener.QUERY_SEARCH_SHARD_INFORMATION_SETTING.getKey(), false)
            .build();
        clusterSettings.applySettings(updatedSettings);

        reset(client);

        CountDownLatch updateSettingsLatch = new CountDownLatch(1);
        LatchedActionListener<Void> updateSettingsActionListener = new LatchedActionListener<>(ActionListener.noop(), latch);
        listener.beforeIndexShardRecovery(indexShard, indexSettings, updateSettingsActionListener);
        verifyNoInteractions(client);
        assertThat(updateSettingsLatch.getCount(), equalTo(1L));
    }

    public void testShardIsMovedResponse() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting shardRouting = createSearchOnlyShard(shardId, "search_node_1").moveToStarted(1);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        SearchShardInformationIndexListener listener = new SearchShardInformationIndexListener(
            client,
            collector,
            clusterSettings,
            System::currentTimeMillis
        );

        listener.beforeIndexShardRecovery(indexShard, indexSettings, latchedActionListener);

        ArgumentCaptor<ActionRequest> argument = ArgumentCaptor.forClass(ActionRequest.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<TransportFetchSearchShardInformationAction.Response>> listenerCaptor = ArgumentCaptor.forClass(
            ActionListener.class
        );
        verify(client).execute(eq(TransportFetchSearchShardInformationAction.TYPE), argument.capture(), listenerCaptor.capture());
        assertThat(argument.getValue(), instanceOf(TransportFetchSearchShardInformationAction.Request.class));

        TransportFetchSearchShardInformationAction.Request request = (TransportFetchSearchShardInformationAction.Request) argument
            .getValue();
        assertThat(request.getShardId(), equalTo(shardId));

        ActionListener<TransportFetchSearchShardInformationAction.Response> responseListener = listenerCaptor.getValue();

        reset(indexShard);
        responseListener.onFailure(new ShardNotFoundException(shardId));
        verifyNoInteractions(indexShard);

        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, SearchShardInformationMetricsCollector.REQUESTS_SHARD_MOVED_TOTAL);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.get(0).getLong(), equalTo(1L));
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
            ShardRouting.Role.SEARCH_ONLY
        ).initialize(nodeId, null, randomNonNegativeLong());
    }
}
