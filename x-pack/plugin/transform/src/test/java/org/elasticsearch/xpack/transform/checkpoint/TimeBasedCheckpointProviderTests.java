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
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
            TransformConfigVersionUtils.randomVersionBetween(random(), TransformConfigVersion.V_7_15_0, TransformConfigVersion.CURRENT),
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

    private TimeBasedCheckpointProvider newCheckpointProvider(TransformConfig transformConfig) {
        return new TimeBasedCheckpointProvider(
            clock,
            parentTaskClient,
            new RemoteClusterResolver(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            transformConfigManager,
            transformAuditor,
            transformConfig
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
        return new SearchResponse(
            SearchHits.empty(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 0),
            null,
            null,
            false,
            false,
            null,
            0,
            null,
            1,
            1,
            0,
            0,
            ShardSearchFailure.EMPTY_ARRAY,
            null
        );
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
