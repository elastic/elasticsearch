/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.StubLinkedProjectConfigService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.utils.TransformConfigVersionUtils;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimeBasedCheckpointProviderTests extends ESTestCase {

    private static final String TIMESTAMP_FIELD = "@timestamp";

    private Clock clock;
    private Client client;
    private ParentTaskAssigningClient parentTaskClient;
    private IndexBasedTransformConfigManager transformConfigManager;
    private MockTransformAuditor transformAuditor;

    @Before
    public void setUpMocks() {
        clock = mock(Clock.class);
        when(clock.millis()).thenReturn(123456789L);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        parentTaskClient = new ParentTaskAssigningClient(client, new TaskId("dummy-node:123456"));
        transformConfigManager = mock(IndexBasedTransformConfigManager.class);
        transformAuditor = MockTransformAuditor.createMockAuditor();
    }

    public void testSourceHasChanged_NotChanged() throws InterruptedException {
        testSourceHasChanged(
            0,
            false,
            TransformCheckpoint.EMPTY,
            TransformConfigVersionUtils.randomVersionBetween(TransformConfigVersion.V_7_15_0, TransformConfigVersion.CURRENT),
            TIMESTAMP_FIELD,
            TimeValue.timeValueMinutes(10),
            TimeValue.ZERO,
            tuple(0L, 123000000L)
        );
    }

    public void testSourceHasChanged_NotChanged_DoNotAlignCheckpointsBecauseOfVersion() throws InterruptedException {
        testSourceHasChanged(
            0,
            false,
            TransformCheckpoint.EMPTY,
            TransformConfigVersion.V_7_14_0,
            TIMESTAMP_FIELD,
            TimeValue.timeValueMinutes(10),
            TimeValue.ZERO,
            // Checkpoint alignment doesn't work here because the transform was created without alignment.
            tuple(0L, 123456789L)
        );
    }

    public void testSourceHasChanged_Changed() throws InterruptedException {
        testSourceHasChanged(
            1,
            true,
            TransformCheckpoint.EMPTY,
            TransformConfigVersion.CURRENT,
            TIMESTAMP_FIELD,
            TimeValue.timeValueMinutes(10),
            TimeValue.ZERO,
            tuple(0L, 123000000L)
        );
    }

    public void testSourceHasChanged_UnfinishedCheckpoint() throws InterruptedException {
        testSourceHasChanged(
            0,
            false,
            new TransformCheckpoint("", 100000000L, 7, emptyMap(), null),
            TransformConfigVersion.CURRENT,
            TIMESTAMP_FIELD,
            TimeValue.timeValueMinutes(10),
            TimeValue.ZERO,
            tuple(0L, 123000000L)
        );
    }

    public void testSourceHasChanged_SubsequentCheckpoint() throws InterruptedException {
        testSourceHasChanged(
            0,
            false,
            new TransformCheckpoint("", 100000000L, 7, emptyMap(), 120000000L),
            TransformConfigVersion.CURRENT,
            TIMESTAMP_FIELD,
            TimeValue.timeValueMinutes(10),
            TimeValue.ZERO,
            tuple(120000000L, 123000000L)
        );
    }

    public void testSourceHasChanged_WithDelay() throws InterruptedException {
        testSourceHasChanged(
            0,
            false,
            new TransformCheckpoint("", 100000000L, 7, emptyMap(), 120000000L),
            TransformConfigVersion.CURRENT,
            TIMESTAMP_FIELD,
            TimeValue.timeValueMinutes(10),
            TimeValue.timeValueMinutes(5),
            tuple(120000000L, 123000000L)
        );
    }

    public void testSourceHasChanged_UsesInitialDelayWhileNoDataProcessed() throws InterruptedException {
        // While the transform is still in its initial catch-up phase (no data processed yet), the change-detection gate widens
        // its window with initial_delay (0s) instead of the steady-state delay (60s). The gate must mirror createNextCheckpoint,
        // otherwise just-landed data that has aged less than the steady-state delay would never trigger the next checkpoint.
        TransformConfig transformConfig = new TransformConfig.Builder(
            TransformConfigTests.randomTransformConfig(getTestName(), TransformConfigVersion.CURRENT)
        ).setSettings(new SettingsConfig.Builder().setAlignCheckpoints(false).build())
            .setSyncConfig(new TimeSyncConfig(TIMESTAMP_FIELD, TimeValue.timeValueMillis(60000), TimeValue.ZERO))
            .build();

        final SearchResponse searchResponse = newSearchResponse(1);
        try {
            doAnswer(withResponse(searchResponse)).when(client).execute(eq(TransportSearchAction.TYPE), any(), any());
            TimeBasedCheckpointProvider provider = newCheckpointProvider(transformConfig, () -> false);

            SetOnce<Boolean> hasChangedHolder = new SetOnce<>();
            SetOnce<Exception> exceptionHolder = new SetOnce<>();
            CountDownLatch latch = new CountDownLatch(1);
            provider.sourceHasChanged(
                new TransformCheckpoint("", 100000000L, 1, emptyMap(), 120000000L),
                new LatchedActionListener<>(ActionListener.wrap(hasChangedHolder::set, exceptionHolder::set), latch)
            );
            assertThat(latch.await(100, TimeUnit.MILLISECONDS), is(true));

            ArgumentCaptor<SearchRequest> searchRequestArgumentCaptor = ArgumentCaptor.forClass(SearchRequest.class);
            verify(client).execute(eq(TransportSearchAction.TYPE), searchRequestArgumentCaptor.capture(), any());
            BoolQueryBuilder boolQuery = (BoolQueryBuilder) searchRequestArgumentCaptor.getValue().source().query();
            RangeQueryBuilder rangeQuery = (RangeQueryBuilder) boolQuery.filter().get(1);
            // Upper bound uses initial_delay (0) -> now (123456789), not the steady-state now - 60000.
            assertThat(rangeQuery.from(), is(equalTo(120000000L)));
            assertThat(rangeQuery.to(), is(equalTo(123456789L)));
            assertThat(hasChangedHolder.get(), is(true));
            assertThat(exceptionHolder.get(), is(nullValue()));
        } finally {
            searchResponse.decRef();
        }
    }

    private void testSourceHasChanged(
        long totalHits,
        boolean expectedHasChangedValue,
        TransformCheckpoint lastCheckpoint,
        TransformConfigVersion transformVersion,
        String dateHistogramField,
        TimeValue dateHistogramInterval,
        TimeValue delay,
        Tuple<Long, Long> expectedRangeQueryBounds
    ) throws InterruptedException {
        final SearchResponse searchResponse = newSearchResponse(totalHits);
        try {
            doAnswer(withResponse(searchResponse)).when(client).execute(eq(TransportSearchAction.TYPE), any(), any());
            String transformId = getTestName();
            TransformConfig transformConfig = newTransformConfigWithDateHistogram(
                transformId,
                transformVersion,
                dateHistogramField,
                dateHistogramInterval,
                delay
            );
            TimeBasedCheckpointProvider provider = newCheckpointProvider(transformConfig);

            SetOnce<Boolean> hasChangedHolder = new SetOnce<>();
            SetOnce<Exception> exceptionHolder = new SetOnce<>();
            CountDownLatch latch = new CountDownLatch(1);
            provider.sourceHasChanged(
                lastCheckpoint,
                new LatchedActionListener<>(ActionListener.wrap(hasChangedHolder::set, exceptionHolder::set), latch)
            );
            assertThat(latch.await(100, TimeUnit.MILLISECONDS), is(true));

            ArgumentCaptor<SearchRequest> searchRequestArgumentCaptor = ArgumentCaptor.forClass(SearchRequest.class);
            verify(client).execute(eq(TransportSearchAction.TYPE), searchRequestArgumentCaptor.capture(), any());
            SearchRequest searchRequest = searchRequestArgumentCaptor.getValue();
            BoolQueryBuilder boolQuery = (BoolQueryBuilder) searchRequest.source().query();
            RangeQueryBuilder rangeQuery = (RangeQueryBuilder) boolQuery.filter().get(1);
            assertThat(rangeQuery.from(), is(equalTo(expectedRangeQueryBounds.v1())));
            assertThat(rangeQuery.to(), is(equalTo(expectedRangeQueryBounds.v2())));

            assertThat(hasChangedHolder.get(), is(equalTo(expectedHasChangedValue)));
            assertThat(exceptionHolder.get(), is(nullValue()));
        } finally {
            searchResponse.decRef();
        }
    }

    public void testCreateNextCheckpoint_NoDelay() throws InterruptedException {
        String transformId = getTestName();
        testCreateNextCheckpoint(
            transformId,
            TIMESTAMP_FIELD,
            TimeValue.timeValueMinutes(10),
            TimeValue.ZERO,
            new TransformCheckpoint(transformId, 100000000L, 7, emptyMap(), 120000000L),
            new TransformCheckpoint(transformId, 123456789L, 8, emptyMap(), 123000000L)
        );
    }

    public void testCreateNextCheckpoint_SmallDelay() throws InterruptedException {
        String transformId = getTestName();
        testCreateNextCheckpoint(
            transformId,
            TIMESTAMP_FIELD,
            TimeValue.timeValueMinutes(10),
            TimeValue.timeValueMinutes(5),
            new TransformCheckpoint(transformId, 100000000L, 7, emptyMap(), 120000000L),
            new TransformCheckpoint(transformId, 123456789L, 8, emptyMap(), 123000000L)
        );
    }

    public void testCreateNextCheckpoint_BigDelay() throws InterruptedException {
        String transformId = getTestName();
        testCreateNextCheckpoint(
            transformId,
            TIMESTAMP_FIELD,
            TimeValue.timeValueMinutes(10),
            TimeValue.timeValueMinutes(10),
            new TransformCheckpoint(transformId, 100000000L, 7, emptyMap(), 120000000L),
            new TransformCheckpoint(transformId, 123456789L, 8, emptyMap(), 122400000L)
        );
    }

    public void testCreateNextCheckpoint_FirstCheckpointUsesInitialDelay() throws InterruptedException {
        // No previous checkpoint and no data processed yet -> the reduced initial_delay (0s) applies, so the upper bound is "now".
        testCreateNextCheckpointWithInitialDelay(
            TimeValue.timeValueMillis(60000),
            TimeValue.ZERO,
            false,
            TransformCheckpoint.EMPTY,
            1L,
            123456789L
        );
    }

    public void testCreateNextCheckpoint_KeepsInitialDelayWhileNoDataProcessed() throws InterruptedException {
        // The first (checkpoint #1) range was empty: no document has been processed yet, so the transform is still in its
        // initial catch-up phase and checkpoint #2 keeps using initial_delay (0s) -> upper bound is "now", not now - 60000.
        // This is what lets a chained/downstream transform pick up source data that lands shortly after its empty first
        // checkpoint, instead of waiting for it to age past the steady-state delay.
        testCreateNextCheckpointWithInitialDelay(
            TimeValue.timeValueMillis(60000),
            TimeValue.ZERO,
            false,
            new TransformCheckpoint("", 100000000L, 1, emptyMap(), 100000000L),
            2L,
            123456789L
        );
    }

    public void testCreateNextCheckpoint_UsesSteadyStateDelayOnceDataProcessed() throws InterruptedException {
        // Same checkpoint #2 as above, but a document has now been processed: the transform leaves its initial catch-up phase
        // and switches to the steady-state delay -> now - 60000.
        testCreateNextCheckpointWithInitialDelay(
            TimeValue.timeValueMillis(60000),
            TimeValue.ZERO,
            true,
            new TransformCheckpoint("", 100000000L, 1, emptyMap(), 100000000L),
            2L,
            123396789L
        );
    }

    public void testCreateNextCheckpoint_SubsequentCheckpointClampedToPreviousUpperBound() throws InterruptedException {
        // checkpoint #1 used a small initial_delay, leaving its upper bound (== now) later than now - delay. The next checkpoint's
        // upper bound is clamped up to the previous upper bound so the [lower, upper) range never inverts.
        testCreateNextCheckpointWithInitialDelay(
            TimeValue.timeValueMillis(60000),
            TimeValue.ZERO,
            true,
            new TransformCheckpoint("", 123456789L, 1, emptyMap(), 123456789L),
            2L,
            123456789L
        );
    }

    private void testCreateNextCheckpointWithInitialDelay(
        TimeValue delay,
        TimeValue initialDelay,
        boolean hasProcessedData,
        TransformCheckpoint lastCheckpoint,
        long expectedCheckpoint,
        long expectedTimeUpperBound
    ) throws InterruptedException {
        String transformId = getTestName();
        GetCheckpointAction.Response checkpointResponse = new GetCheckpointAction.Response(Collections.emptyMap());
        doAnswer(withResponse(checkpointResponse)).when(client).execute(eq(GetCheckpointAction.INSTANCE), any(), any());

        TransformConfig transformConfig = new TransformConfig.Builder(
            TransformConfigTests.randomTransformConfig(transformId, TransformConfigVersion.CURRENT)
        ).setSettings(new SettingsConfig.Builder().setAlignCheckpoints(false).build())
            .setSyncConfig(new TimeSyncConfig(TIMESTAMP_FIELD, delay, initialDelay))
            .build();
        TimeBasedCheckpointProvider provider = newCheckpointProvider(transformConfig, () -> hasProcessedData);

        SetOnce<TransformCheckpoint> checkpointHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        provider.createNextCheckpoint(
            lastCheckpoint,
            new LatchedActionListener<>(ActionListener.wrap(checkpointHolder::set, exceptionHolder::set), latch)
        );
        assertThat(latch.await(100, TimeUnit.MILLISECONDS), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
        TransformCheckpoint checkpoint = checkpointHolder.get();
        assertThat(checkpoint.getCheckpoint(), is(equalTo(expectedCheckpoint)));
        assertThat(checkpoint.getTimeUpperBound(), is(equalTo(expectedTimeUpperBound)));
    }

    private void testCreateNextCheckpoint(
        String transformId,
        String dateHistogramField,
        TimeValue dateHistogramInterval,
        TimeValue delay,
        TransformCheckpoint lastCheckpoint,
        TransformCheckpoint expectedNextCheckpoint
    ) throws InterruptedException {
        GetCheckpointAction.Response checkpointResponse = new GetCheckpointAction.Response(Collections.emptyMap());
        doAnswer(withResponse(checkpointResponse)).when(client).execute(eq(GetCheckpointAction.INSTANCE), any(), any());

        TransformConfig transformConfig = newTransformConfigWithDateHistogram(
            transformId,
            TransformConfigVersion.CURRENT,
            dateHistogramField,
            dateHistogramInterval,
            delay
        );
        TimeBasedCheckpointProvider provider = newCheckpointProvider(transformConfig);

        SetOnce<TransformCheckpoint> checkpointHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        provider.createNextCheckpoint(
            lastCheckpoint,
            new LatchedActionListener<>(ActionListener.wrap(checkpointHolder::set, exceptionHolder::set), latch)
        );
        assertThat(latch.await(100, TimeUnit.MILLISECONDS), is(true));
        assertThat(checkpointHolder.get(), is(equalTo(expectedNextCheckpoint)));
        assertThat(exceptionHolder.get(), is(nullValue()));
    }

    public void testSourceHasChangedIncludesRuntimeMappings() throws InterruptedException {
        // Arrange: create a config with explicit runtime_mappings
        Map<String, Object> runtimeMappings = Map.of(
            "total_price_with_tax",
            Map.of("type", "double", "script", Map.of("source", "emit(1.0)"))
        );
        SourceConfig sourceWithRuntimeMappings = new SourceConfig(
            new String[] { "source_index" },
            QueryConfig.matchAll(),
            runtimeMappings,
            null
        );
        TransformConfig transformConfig = new TransformConfig.Builder(TransformConfigTests.randomTransformConfig()).setSource(
            sourceWithRuntimeMappings
        ).setSyncConfig(new TimeSyncConfig(TIMESTAMP_FIELD, TimeValue.ZERO)).build();

        final SearchResponse searchResponse = newSearchResponse(0);
        try {
            doAnswer(withResponse(searchResponse)).when(client).execute(eq(TransportSearchAction.TYPE), any(), any());
            TimeBasedCheckpointProvider provider = newCheckpointProvider(transformConfig);

            // Act: call sourceHasChanged
            CountDownLatch latch = new CountDownLatch(1);
            provider.sourceHasChanged(TransformCheckpoint.EMPTY, new LatchedActionListener<>(ActionListener.wrap(r -> {}, e -> {}), latch));
            assertThat(latch.await(100, TimeUnit.MILLISECONDS), is(true));

            // Assert: the search request should include runtime_mappings
            ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
            verify(client).execute(eq(TransportSearchAction.TYPE), searchRequestCaptor.capture(), any());
            SearchRequest capturedRequest = searchRequestCaptor.getValue();
            assertThat(
                "sourceHasChanged search should include runtime_mappings from the source config",
                capturedRequest.source().runtimeMappings(),
                is(equalTo(runtimeMappings))
            );
        } finally {
            searchResponse.decRef();
        }
    }

    private TimeBasedCheckpointProvider newCheckpointProvider(TransformConfig transformConfig) {
        // Default to "data already processed" so that, when initial_delay == delay (the common case), behaviour matches the
        // steady state. Tests exercising the initial catch-up phase pass an explicit supplier.
        return newCheckpointProvider(transformConfig, () -> true);
    }

    private TimeBasedCheckpointProvider newCheckpointProvider(TransformConfig transformConfig, BooleanSupplier hasProcessedData) {
        return new TimeBasedCheckpointProvider(
            clock,
            parentTaskClient,
            new RemoteClusterResolver(Settings.EMPTY, StubLinkedProjectConfigService.INSTANCE),
            transformConfigManager,
            transformAuditor,
            transformConfig,
            hasProcessedData
        );
    }

    private static TransformConfig newTransformConfigWithDateHistogram(
        String transformId,
        TransformConfigVersion transformVersion,
        String dateHistogramField,
        TimeValue dateHistogramInterval,
        TimeValue delay
    ) {
        DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(
            dateHistogramField,
            null,
            false,
            new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval(dateHistogramInterval.getStringRep())),
            null,
            null
        );
        Supplier<SingleGroupSource> singleGroupSourceSupplier = new Supplier<>() {
            int groupCount = 0;

            @Override
            public SingleGroupSource get() {
                return ++groupCount == 1
                    ? dateHistogramGroupSource
                    : GroupConfigTests.randomSingleGroupSource(TransformConfigVersion.CURRENT);
            }
        };
        PivotConfig pivotConfigWithDateHistogramSource = new PivotConfig(
            GroupConfigTests.randomGroupConfig(singleGroupSourceSupplier),
            AggregationConfigTests.randomAggregationConfig(),
            null // deprecated
        );
        SettingsConfig.Builder settingsConfigBuilder = new SettingsConfig.Builder();
        if (randomBoolean()) {
            settingsConfigBuilder.setAlignCheckpoints(
                randomBoolean()
                    // Set align_checkpoints setting explicitly to "true".
                    ? true
                    // Set align_checkpoints setting explicitly to "null". This will be interpreted as "true".
                    : null
            );
        } else {
            // Leave align_checkpoints setting unset. This will be interpreted as "true".
        }
        return new TransformConfig.Builder(TransformConfigTests.randomTransformConfig(transformId, transformVersion)).setSettings(
            settingsConfigBuilder.build()
        ).setPivotConfig(pivotConfigWithDateHistogramSource).setSyncConfig(new TimeSyncConfig(TIMESTAMP_FIELD, delay)).build();
    }

    private static SearchResponse newSearchResponse(long totalHits) {
        return SearchResponseUtils.successfulResponse(SearchHits.empty(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 0));
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }
}
