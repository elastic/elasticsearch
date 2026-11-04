/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.esql.action.internal.SharedSecrets;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EsqlDataExtractorTests extends ESTestCase {

    private static final String DEFAULT_QUERY = "FROM logs";
    private static final String TIME_FIELD = "ts";
    private static final String SOURCE_TIME_FIELD = "@timestamp";
    private static final long GROUPING_INTERVAL_MILLIS = 60_000L;
    private static final String SUMMARY_COUNT_FIELD = "doc_count";
    private static final String JOB_ID = "test-job";
    private static final String DATE = "date";
    private static final String DATE_NANOS = "date_nanos";
    private static final String LONG = "long";
    private static final String KEYWORD = "keyword";

    private Client client;
    private DatafeedTimingStatsReporter timingStatsReporter;
    private TestThreadPool trackingThreadPool;

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(mock(ThreadPool.class));
        when(client.threadPool().getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        timingStatsReporter = mock(DatafeedTimingStatsReporter.class);
        trackingThreadPool = new TestThreadPool(getTestClass().getName());
    }

    @After
    public void tearDownTests() {
        ThreadPool.terminate(trackingThreadPool, 10, TimeUnit.SECONDS);
    }

    public void testCancelRacingTaskPublicationShouldCancelQueryTaskOnceAndSetParentTask() throws Exception {
        NodeClientTracking tracking = new NodeClientTracking();
        NodeClient nodeClient = mockNodeClient(tracking);
        TaskId parentTaskId = new TaskId("parent-node", 42L);
        EsqlDataExtractor extractor = new EsqlDataExtractor(
            new ParentTaskAssigningClient(nodeClient, parentTaskId),
            context(0L, 1000L),
            timingStatsReporter
        );
        EsqlQueryRequest request = mock(EsqlQueryRequest.class);
        ActionType<EsqlQueryResponse> action = mock();
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = new TestEsqlQueryRequestBuilder(request, action);
        tracking.cancelBeforeTaskPublication = extractor::cancel;

        Future<EsqlQueryResponse> result = trackingThreadPool.generic().submit(() -> extractor.execute(builder));
        assertTrue(tracking.queryStarted.await(10, TimeUnit.SECONDS));
        assertTrue(tracking.cancelRequestReceived.await(10, TimeUnit.SECONDS));
        assertThat(tracking.cancelRequests.size(), equalTo(1));
        assertThat(tracking.cancelRequests.get(0).getTargetTaskId(), equalTo(new TaskId("local-node", 7L)));
        assertThat(tracking.cancelOrigins, equalTo(List.of("tasks")));
        verify(request).setParentTask(parentTaskId);

        tracking.queryListener.onResponse(mock(EsqlQueryResponse.class));
        result.get(10, TimeUnit.SECONDS);
        extractor.destroy();
        assertThat(tracking.cancelRequests.size(), equalTo(1));
    }

    public void testNormalQueryCompletionShouldClearTrackedTaskWithoutCancel() throws Exception {
        NodeClientTracking tracking = new NodeClientTracking();
        NodeClient nodeClient = mockNodeClient(tracking);
        EsqlDataExtractor extractor = new EsqlDataExtractor(nodeClient, context(0L, 1000L), timingStatsReporter);
        EsqlQueryRequest request = mock(EsqlQueryRequest.class);
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = new TestEsqlQueryRequestBuilder(request, mock());

        Future<EsqlQueryResponse> result = trackingThreadPool.generic().submit(() -> extractor.execute(builder));
        assertTrue(tracking.queryStarted.await(10, TimeUnit.SECONDS));
        tracking.queryListener.onResponse(mock(EsqlQueryResponse.class));
        result.get(10, TimeUnit.SECONDS);

        extractor.destroy();
        assertThat(tracking.cancelRequests, equalTo(List.of()));
    }

    public void testDuplicateCancelWhileInFlightShouldCancelTaskExactlyOnce() throws Exception {
        NodeClientTracking tracking = new NodeClientTracking();
        NodeClient nodeClient = mockNodeClient(tracking);
        EsqlDataExtractor extractor = new EsqlDataExtractor(nodeClient, context(0L, 1000L), timingStatsReporter);
        EsqlQueryRequest request = mock(EsqlQueryRequest.class);
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = new TestEsqlQueryRequestBuilder(request, mock());

        Future<EsqlQueryResponse> result = trackingThreadPool.generic().submit(() -> extractor.execute(builder));
        assertTrue(tracking.queryStarted.await(10, TimeUnit.SECONDS));
        extractor.cancel();
        extractor.cancel();
        assertTrue(tracking.cancelRequestReceived.await(10, TimeUnit.SECONDS));
        assertThat(tracking.cancelRequests.size(), equalTo(1));

        tracking.queryListener.onResponse(mock(EsqlQueryResponse.class));
        result.get(10, TimeUnit.SECONDS);
        extractor.destroy();
        assertThat(tracking.cancelRequests.size(), equalTo(1));
    }

    public void testStoredDatafeedHeadersShouldReachEsqlQuery() throws Exception {
        NodeClientTracking tracking = new NodeClientTracking();
        NodeClient nodeClient = mockNodeClient(tracking);
        EsqlDataExtractor extractor = new EsqlDataExtractor(
            nodeClient,
            contextWithHeaders(0L, 1000L, Map.of("es-security-runas-user", "stored-datafeed-user")),
            timingStatsReporter
        );
        EsqlQueryRequest request = mock(EsqlQueryRequest.class);
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = new TestEsqlQueryRequestBuilder(request, mock());

        Future<EsqlQueryResponse> result = trackingThreadPool.generic().submit(() -> extractor.execute(builder));
        assertTrue(tracking.queryStarted.await(10, TimeUnit.SECONDS));
        assertThat(tracking.queryRunAsHeader, equalTo("stored-datafeed-user"));

        tracking.queryListener.onResponse(mock(EsqlQueryResponse.class));
        result.get(10, TimeUnit.SECONDS);
    }

    public void testQueryWithoutUserSortOrLimitShouldInjectTimeSortAndCap() throws IOException {
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE), column("value", LONG)), isoAtEpochMillis(1500L), 42L);

        assertThat(extractor.hasNext(), is(true));
        DataExtractor.Result result = extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(DEFAULT_QUERY + " | SORT ??timeField ASC | LIMIT 10000"));
        assertThat(extractor.capturedParams, equalTo(List.of(new EsqlQueryParam("timeField", "timestamp", IDENTIFIER))));
        assertThat(result.rowCount(), equalTo(1L));
        verify(timingStatsReporter).reportSearchDuration(any());
    }

    public void testNextPathShouldBuildEsqlRequestWithoutIndicesOptions() throws IOException {
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = mock();
        when(builder.query(any())).thenReturn(builder);
        when(builder.filter(any())).thenReturn(builder);
        when(builder.params(any())).thenReturn(builder);
        when(builder.allowPartialResults(anyBoolean())).thenReturn(builder);
        PlainActionFuture<EsqlQueryResponse> responseFuture = new PlainActionFuture<>();
        responseFuture.onResponse(wrapResponse(mockEsqlResponse(List.of(column(TIME_FIELD, DATE)), List.of())));
        when(builder.execute()).thenReturn(responseFuture);
        SharedSecrets.setEsqlQueryRequestBuilderAccess(client -> builder);
        try {
            EsqlDataExtractor extractor = new EsqlDataExtractor(client, context(1000L, 2000L), timingStatsReporter);

            extractor.next();

            verify(builder).query(DEFAULT_QUERY + " | SORT ??timeField ASC | LIMIT 10000");
            verify(builder).filter(any(RangeQueryBuilder.class));
            verify(builder).params(List.of(new EsqlQueryParam("timeField", TIME_FIELD, IDENTIFIER)));
            verify(builder).allowPartialResults(false);
            // The concrete builder lives in x-pack-esql, which is deliberately absent from the ML test classpath.
            // Its core interface has no indicesOptions API, so this production next() path cannot send one.
            assertThat(
                Arrays.stream(EsqlQueryRequestBuilder.class.getMethods())
                    .map(method -> method.getName())
                    .anyMatch("indicesOptions"::equals),
                is(false)
            );
        } finally {
            SharedSecrets.setEsqlQueryRequestBuilderAccess(null);
        }
    }

    public void testUserQueryWithSortShouldStillAppendTimeSortAndCap() throws IOException {
        String esqlQuery = "FROM logs | SORT value DESC";
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, esqlQuery, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE), column("value", LONG)), isoAtEpochMillis(1500L), 1L);

        extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(esqlQuery + " | SORT ??timeField ASC | LIMIT 10000"));
    }

    public void testUserLimitShouldKeepUserLimitAndAppendTimeSort() throws IOException {
        String query = "FROM logs-* | LIMIT 20";
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, query, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(query + " | SORT ??timeField ASC"));
    }

    public void testUserLimitEndingInLineCommentShouldAppendTimeSortOnNewLine() throws IOException {
        String query = "FROM logs-* | LIMIT 20 // explanation";
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, query, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(query + "\n | SORT ??timeField ASC"));
    }

    public void testFourQuoteTripleStringWithOuterLimitShouldAppendTimeSort() throws IOException {
        String query = "FROM logs-* | WHERE message == \"\"\"literal\"\"\"\" | LIMIT 20";
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, query, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(query + " | SORT ??timeField ASC"));
    }

    public void testTimeFilterShouldBeHalfOpenOnNext() throws IOException {
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        assertThat(extractor.capturedTimeFilter, equalTo(new RangeQueryBuilder("timestamp").gte(1000L).lt(2000L).format("epoch_millis")));
    }

    public void testMultiValuedEmittedTimeShouldFailWholeBatchOnNext() {
        RecordingSyncQuery extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), List.of(isoAtEpochMillis(100L), isoAtEpochMillis(200L)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString("multi-valued"));
    }

    public void testMissingTimeFieldShouldThrowIllegalArgumentOnNext() throws IOException {
        RecordingSyncQuery extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column("other_field", KEYWORD)), "some-value");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString("final ES|QL output"));
        assertThat(e.getMessage(), containsString("data_description.time_field [" + TIME_FIELD + "]"));
    }

    public void testMissingSummaryCountFieldShouldThrowIllegalArgumentOnNext() throws IOException {
        RecordingSyncQuery extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD, SUMMARY_COUNT_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), isoAtEpochMillis(100L));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(SUMMARY_COUNT_FIELD));
        assertThat(e.getMessage(), containsString("numeric count column"));
    }

    public void testSummaryCountFieldPresentShouldSucceedOnNext() throws IOException {
        RecordingSyncQuery extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD, SUMMARY_COUNT_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE), column(SUMMARY_COUNT_FIELD, LONG)), isoAtEpochMillis(100L), 5L);

        DataExtractor.Result result = extractor.next();

        String ndjson = asString(result.data().get());
        assertThat(ndjson, containsString("\"" + SUMMARY_COUNT_FIELD + "\":5"));
    }

    public void testNoRowsShouldReturnEmptyOptionalOnNext() throws IOException {
        RecordingSyncQuery extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(List.of(column(TIME_FIELD, DATE)), List.of());

        DataExtractor.Result result = extractor.next();

        assertThat(result.searchInterval(), equalTo(new SearchInterval(0L, 10000L)));
        assertThat(result.data().isPresent(), is(false));
    }

    public void testMalformedDateShouldThrowOnNext() throws IOException {
        RecordingSyncQuery extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), "not-a-valid-iso-date");

        expectThrows(Exception.class, extractor::next);
    }

    public void testNextAfterExhaustionShouldThrowNoSuchElement() throws IOException {
        RecordingSyncQuery extractor = createExtractor(0L, 1000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(List.of(column(TIME_FIELD, DATE)), List.of());

        extractor.next();
        expectThrows(NoSuchElementException.class, extractor::next);
    }

    public void testNodeClosedOnceShouldRetrySameRange() throws IOException {
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueFailure(new NodeClosedException((DiscoveryNode) null));
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        String expectedQuery = DEFAULT_QUERY + " | SORT ??timeField ASC | LIMIT 10000";
        QueryBuilder expectedFilter = new RangeQueryBuilder(TIME_FIELD).gte(1000L).lt(2000L).format("epoch_millis");
        List<EsqlQueryParam> expectedParams = List.of(new EsqlQueryParam("timeField", TIME_FIELD, IDENTIFIER));
        assertThat(extractor.capturedQueries, equalTo(List.of(expectedQuery, expectedQuery)));
        assertThat(extractor.capturedTimeFilters, equalTo(List.of(expectedFilter, expectedFilter)));
        assertThat(extractor.capturedParameterLists, equalTo(List.of(expectedParams, expectedParams)));
    }

    public void testNodeClosedTwiceShouldFailCycleWithoutAdditionalRetry() {
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueFailure(new NodeClosedException((DiscoveryNode) null));
        extractor.enqueueFailure(new NodeClosedException((DiscoveryNode) null));

        expectThrows(NodeClosedException.class, extractor::next);

        assertThat(extractor.capturedQueries.size(), equalTo(2));
    }

    public void testCancelledInFlightQueryShouldNotRetry() {
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.cancelBeforeNextFailure();
        extractor.enqueueFailure(new NodeClosedException((DiscoveryNode) null));

        expectThrows(NodeClosedException.class, extractor::next);

        assertThat(extractor.capturedQueries.size(), equalTo(1));
    }

    public void testScalarEmittedTimeInWindowShouldSucceedOnNext() throws IOException {
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, LONG)), 1500L);

        assertThat(asString(extractor.next().data().get()), equalTo("{\"" + TIME_FIELD + "\":1500}"));
    }

    public void testEmittedTimeBeforeStartShouldFailWholeBatchOnNext() {
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, LONG)), 999L);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString("before the source window start"));
        assertThat(e.getMessage(), containsString("1000"));
        assertThat(e.getMessage(), containsString("2000"));
    }

    public void testEmittedTimeAtEndShouldFailWholeBatchOnNext() {
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, LONG)), 2000L);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString("at or after the source window end"));
    }

    public void testNullEmittedTimeShouldFailWholeBatchOnNext() {
        RecordingSyncQuery extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(List.of(column(TIME_FIELD, DATE)), List.of(Arrays.asList((Object) null)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString("value is null"));
    }

    public void testWrongTypeEmittedTimeShouldFailWholeBatchOnNext() {
        RecordingSyncQuery extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), "not-a-valid-iso-date");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString("unsupported type"));
        assertNotNull(e.getCause());
    }

    public void testInvalidEmittedTimeShouldNotExposeNdjsonOnNext() {
        RecordingSyncQuery extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(List.of(column(TIME_FIELD, LONG)), List.of(List.of(1500L), List.of(2500L)));

        expectThrows(IllegalArgumentException.class, extractor::next);
    }

    public void testRenamedEmittedTimeFieldShouldFilterOnSourceTimeField() throws IOException {
        RecordingSyncQuery extractor = createExtractorWithDistinctTimeFields(1000L, 2000L, DEFAULT_QUERY, SOURCE_TIME_FIELD, "bucket_time");
        extractor.enqueueRow(List.of(column("bucket_time", DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        assertThat(
            extractor.capturedTimeFilter,
            equalTo(new RangeQueryBuilder(SOURCE_TIME_FIELD).gte(1000L).lt(2000L).format("epoch_millis"))
        );
        assertThat(extractor.capturedParams, equalTo(List.of(new EsqlQueryParam("timeField", "bucket_time", IDENTIFIER))));
    }

    // getSummary() binds ??timeField to source_time_field for the source-level probe (leading FROM/TS only),
    // not the emitted time column name used after the user's pipeline renames it.

    public void testSingleNextCallShouldToggleHasNextFromTrueToFalse() throws IOException {
        RecordingSyncQuery extractor = createExtractor(0L, 1000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(List.of(column(TIME_FIELD, DATE)), List.of());

        assertThat(extractor.hasNext(), is(true));
        extractor.next();
        assertThat(extractor.hasNext(), is(false));
    }

    public void testCancelBeforeNextShouldReturnEmptyResultWithoutExecutingQuery() throws IOException {
        RecordingSyncQuery extractor = createExtractor(0L, 1000L, DEFAULT_QUERY, TIME_FIELD);

        assertThat(extractor.isCancelled(), is(false));
        extractor.cancel();
        assertThat(extractor.isCancelled(), is(true));

        assertThat(extractor.hasNext(), is(true));
        DataExtractor.Result result = extractor.next();
        assertThat(result.data().isPresent(), is(false));
        assertThat(extractor.capturedOrderedQuery, equalTo(null));
        verify(timingStatsReporter, never()).reportSearchDuration(any());
    }

    public void testDestroyShouldCancelLikeExplicitCancel() throws IOException {
        RecordingSyncQuery extractor = createExtractor(0L, 1000L, DEFAULT_QUERY, TIME_FIELD);

        extractor.destroy();
        assertThat(extractor.isCancelled(), is(true));

        DataExtractor.Result result = extractor.next();
        assertThat(result.data().isPresent(), is(false));
        assertThat(extractor.capturedOrderedQuery, equalTo(null));
        verify(timingStatsReporter, never()).reportSearchDuration(any());
    }

    public void testExtractorContextEndShouldBeReturnedAsEndTime() {
        RecordingSyncQuery extractor = createExtractor(500L, 9999L, DEFAULT_QUERY, TIME_FIELD);
        assertThat(extractor.getEndTime(), equalTo(9999L));
    }

    public void testPassThroughQueryShouldBuildStatsQueryAndConvertDateColumns() {
        RecordingSyncQuery extractor = createExtractor(1000L, 9000L, DEFAULT_QUERY, TIME_FIELD);
        String earliestIso = "1970-01-01T00:00:01.500Z";
        String latestIso = "1970-01-01T00:00:08.500Z";
        long expectedEarliest = epochMillis(earliestIso);
        long expectedLatest = epochMillis(latestIso);

        extractor.enqueueRow(
            List.of(column("earliest_time", DATE), column("latest_time", DATE), column("total_hits", LONG)),
            earliestIso,
            latestIso,
            100L
        );

        DataExtractor.DataSummary summary = extractor.getSummary();

        assertThat(summary.earliestTime(), equalTo(expectedEarliest));
        assertThat(summary.latestTime(), equalTo(expectedLatest));
        assertThat(summary.totalHits(), equalTo(100L));
        assertThat(summary.hasData(), is(true));

        assertThat(
            extractor.capturedOrderedQuery,
            equalTo(DEFAULT_QUERY + " | STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)")
        );
        assertThat(extractor.capturedParams, equalTo(List.of(new EsqlQueryParam("timeField", TIME_FIELD, IDENTIFIER))));
        assertThat(extractor.capturedTimeFilter, equalTo(new RangeQueryBuilder(TIME_FIELD).gte(1000L).lt(9000L).format("epoch_millis")));

        verify(timingStatsReporter).reportSearchDuration(any());
    }

    public void testStatsRowWithNullMinMaxShouldReturnNoDataFromSummary() {
        RecordingSyncQuery extractor = createExtractor(1000L, 9000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(
            List.of(column("earliest_time", DATE), column("latest_time", DATE), column("total_hits", LONG)),
            List.of(Arrays.asList(null, null, 0L))
        );

        DataExtractor.DataSummary summary = extractor.getSummary();

        assertThat(summary.hasData(), is(false));
        assertThat(summary.totalHits(), equalTo(0L));
    }

    public void testInvalidTimeFieldShouldNotBeSubstitutedInSummary() {
        String invalidTimeField = "field | STATS COUNT(*)";
        RecordingSyncQuery extractor = createExtractor(1000L, 9000L, DEFAULT_QUERY, invalidTimeField);
        extractor.enqueueRow(
            List.of(column("earliest_time", DATE), column("latest_time", DATE), column("total_hits", LONG)),
            "1970-01-01T00:00:01.500Z",
            "1970-01-01T00:00:08.500Z",
            100L
        );

        extractor.getSummary();

        assertThat(extractor.capturedOrderedQuery, not(containsString(invalidTimeField)));
        assertThat(extractor.capturedOrderedQuery, containsString("??timeField"));
        assertThat(extractor.capturedParams, equalTo(List.of(new EsqlQueryParam("timeField", invalidTimeField, IDENTIFIER))));
    }

    public void testSummaryShouldDetectTimeColumnTypesIndependently() {
        RecordingSyncQuery extractor = createExtractor(1000L, 9000L, DEFAULT_QUERY, TIME_FIELD);
        String latestIso = "1970-01-01T00:00:08.500Z";
        long expectedLatest = epochMillis(latestIso);

        extractor.enqueueRow(
            List.of(column("earliest_time", LONG), column("latest_time", DATE), column("total_hits", LONG)),
            1500L,
            latestIso,
            100L
        );

        DataExtractor.DataSummary summary = extractor.getSummary();

        assertThat(summary.earliestTime(), equalTo(1500L));
        assertThat(summary.latestTime(), equalTo(expectedLatest));
    }

    public void testSummaryProbeShouldUseSourceTimeFieldForFastRange() {
        RecordingSyncQuery extractor = createExtractorWithDistinctTimeFields(1000L, 9000L, DEFAULT_QUERY, SOURCE_TIME_FIELD, "bucket_time");
        extractor.enqueueRow(
            List.of(column("earliest_time", DATE), column("latest_time", DATE), column("total_hits", LONG)),
            "1970-01-01T00:00:01.500Z",
            "1970-01-01T00:00:08.500Z",
            42L
        );

        extractor.getSummary();

        assertThat(extractor.capturedOrderedQuery, not(containsString(SOURCE_TIME_FIELD)));
        assertThat(extractor.capturedParams, equalTo(List.of(new EsqlQueryParam("timeField", SOURCE_TIME_FIELD, IDENTIFIER))));
        assertThat(
            extractor.capturedTimeFilter,
            equalTo(new RangeQueryBuilder(SOURCE_TIME_FIELD).gte(1000L).lt(9000L).format("epoch_millis"))
        );
        // Only one query is issued: DEFAULT_QUERY ("FROM logs") has no top-level STATS, so the fast
        // source-level probe's raw doc count is used directly as the output-row estimate.
        assertThat(extractor.capturedQueries.size(), equalTo(1));
    }

    public void testAggregatingQueryShouldRunBoundedProbeInsteadOfFullRange() {
        String aggregatingQuery = "FROM logs | STATS doc_count = COUNT(*) BY bucket = BUCKET(ts, 1h)";
        RecordingSyncQuery extractor = createExtractor(0L, 100_000_000L, aggregatingQuery, TIME_FIELD);

        // Source-level probe (fast, FROM-clause only): earliest/latest/raw-doc-count over the full range.
        extractor.enqueueRow(
            List.of(column("earliest_time", LONG), column("latest_time", LONG), column("total_hits", LONG)),
            1_000_000L,
            91_000_000L,
            900_000L
        );
        // Bounded density probe: the user's actual pipeline, but only over one grouping-interval-sized
        // window right after earliestTime (here GROUPING_INTERVAL_MILLIS=60_000), producing far fewer
        // output rows than the raw doc count above.
        extractor.enqueueRow(List.of(column("probe_output_rows", LONG)), 3L);

        DataExtractor.DataSummary summary = extractor.getSummary();

        assertThat(summary.earliestTime(), equalTo(1_000_000L));
        assertThat(summary.latestTime(), equalTo(91_000_000L));
        // Extrapolated from the bounded probe (3 rows / 60_000ms), not the raw source doc count (900_000):
        // density * timeSpread = (3 / 60_000) * (91_000_000 - 1_000_000) = 4_500.
        assertThat(summary.totalHits(), equalTo(4_500L));
        assertThat(summary.totalHits(), lessThan(900_000L));

        assertThat(extractor.capturedQueries.size(), equalTo(2));
        assertThat(
            extractor.capturedQueries.get(0),
            equalTo("FROM logs | STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)")
        );
        assertThat(extractor.capturedQueries.get(1), equalTo(aggregatingQuery + " | STATS probe_output_rows = COUNT(*)"));
        assertThat(
            extractor.capturedTimeFilters.get(1),
            equalTo(new RangeQueryBuilder(TIME_FIELD).gte(1_000_000L).lt(1_000_000L + GROUPING_INTERVAL_MILLIS).format("epoch_millis"))
        );
    }

    public void testLeadingCommandEndingInLineCommentShouldAppendSourceStatsOnNewLine() {
        String commentedQuery = "FROM logs // web tier\n| STATS doc_count = COUNT(*) BY bucket = BUCKET(ts, 1h)";
        RecordingSyncQuery extractor = createExtractor(0L, 100_000_000L, commentedQuery, TIME_FIELD);
        extractor.enqueueRow(
            List.of(column("earliest_time", LONG), column("latest_time", LONG), column("total_hits", LONG)),
            1_000_000L,
            1_000_000L,
            10L
        );
        extractor.enqueueRow(List.of(column("probe_output_rows", LONG)), 3L);

        extractor.getSummary();

        assertThat(
            extractor.capturedQueries.get(0),
            equalTo(
                "FROM logs // web tier\n | STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)"
            )
        );
    }

    public void testQueryEndingInLineCommentShouldAppendProbeStatsOnNewLine() {
        String commentedQuery = "FROM logs | STATS doc_count = COUNT(*) BY bucket = BUCKET(ts, 1h) // hourly";
        RecordingSyncQuery extractor = createExtractor(0L, 100_000_000L, commentedQuery, TIME_FIELD);
        extractor.enqueueRow(
            List.of(column("earliest_time", LONG), column("latest_time", LONG), column("total_hits", LONG)),
            1_000_000L,
            91_000_000L,
            900_000L
        );
        extractor.enqueueRow(List.of(column("probe_output_rows", LONG)), 3L);

        extractor.getSummary();

        assertThat(extractor.capturedQueries.size(), equalTo(2));
        assertThat(extractor.capturedQueries.get(1), equalTo(commentedQuery + "\n | STATS probe_output_rows = COUNT(*)"));
    }

    public void testAggregatingQueryWithNoDataInProbeWindowShouldFallBackToProbeCount() {
        String aggregatingQuery = "FROM logs | STATS doc_count = COUNT(*) BY bucket = BUCKET(ts, 1h)";
        RecordingSyncQuery extractor = createExtractor(0L, 100_000_000L, aggregatingQuery, TIME_FIELD);

        extractor.enqueueRow(
            List.of(column("earliest_time", LONG), column("latest_time", LONG), column("total_hits", LONG)),
            1_000_000L,
            91_000_000L,
            900_000L
        );
        // Sparse/bursty data: the probe window right after earliestTime happens to contain no output rows.
        extractor.enqueueRow(List.of(column("probe_output_rows", LONG)), 0L);

        DataExtractor.DataSummary summary = extractor.getSummary();

        // A zero-density extrapolation would starve chunking, so this falls back to a floor of 1 rather than 0.
        assertThat(summary.totalHits(), equalTo(1L));
    }

    public void testAggregatingQueryWithNoSourceDataShouldReturnNoData() {
        String aggregatingQuery = "FROM logs | STATS doc_count = COUNT(*) BY bucket = BUCKET(ts, 1h)";
        RecordingSyncQuery extractor = createExtractor(0L, 100_000_000L, aggregatingQuery, TIME_FIELD);

        extractor.enqueueRows(
            List.of(column("earliest_time", LONG), column("latest_time", LONG), column("total_hits", LONG)),
            List.of(Arrays.asList(null, null, 0L))
        );

        DataExtractor.DataSummary summary = extractor.getSummary();

        assertThat(summary.hasData(), is(false));
        assertThat(summary.totalHits(), equalTo(0L));
        // No point probing an aggregating query's output-row density when the source itself has no data
        // in range.
        assertThat(extractor.capturedQueries.size(), equalTo(1));
    }

    // COUNT(*) can be > 0 while MIN/MAX(??timeField) are null when every matching doc is missing a value for
    // the source time field. getSummary() must use hasData() (not totalHits() == 0) so this case does not
    // call estimateAggregatingOutputRows() with null earliest/latest bounds.

    public void testAggregatingQueryWithNullMinMaxButNonZeroTotalHitsShouldReturnNoData() {
        String aggregatingQuery = "FROM logs | STATS doc_count = COUNT(*) BY bucket = BUCKET(ts, 1h)";
        RecordingSyncQuery extractor = createExtractor(0L, 100_000_000L, aggregatingQuery, TIME_FIELD);

        extractor.enqueueRows(
            List.of(column("earliest_time", LONG), column("latest_time", LONG), column("total_hits", LONG)),
            List.of(Arrays.asList(null, null, 900_000L))
        );

        DataExtractor.DataSummary summary = extractor.getSummary();

        assertThat(summary.hasData(), is(false));
        assertThat(summary.totalHits(), equalTo(900_000L));
        // No point probing an aggregating query's output-row density when the source-level probe could not
        // resolve time bounds.
        assertThat(extractor.capturedQueries.size(), equalTo(1));
    }

    // Same scenario as above but for a pass-through (non-aggregating) query, which never called
    // estimateAggregatingOutputRows() and so never hit the NPE directly -- but must still report
    // hasData() == false consistently, mirroring DataExtractorUtils.getDataSummary()'s DSL precedent where
    // totalHits() and null earliest/latest can likewise diverge.

    public void testPassThroughQueryWithNullMinMaxButNonZeroTotalHitsShouldReturnNoData() {
        RecordingSyncQuery extractor = createExtractor(1000L, 9000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(
            List.of(column("earliest_time", DATE), column("latest_time", DATE), column("total_hits", LONG)),
            List.of(Arrays.asList(null, null, 5L))
        );

        DataExtractor.DataSummary summary = extractor.getSummary();

        assertThat(summary.hasData(), is(false));
        assertThat(summary.totalHits(), equalTo(5L));
    }

    private RecordingSyncQuery createExtractor(long start, long end, String esqlQuery, String timeField) {
        return createExtractor(start, end, esqlQuery, timeField, null);
    }

    private static EsqlDataExtractorContext context(long start, long end) {
        return new EsqlDataExtractorContext(
            JOB_ID,
            DEFAULT_QUERY,
            TIME_FIELD,
            TIME_FIELD,
            GROUPING_INTERVAL_MILLIS,
            start,
            end,
            Map.of(),
            null,
            null
        );
    }

    private static EsqlDataExtractorContext contextWithHeaders(long start, long end, Map<String, String> headers) {
        return new EsqlDataExtractorContext(
            JOB_ID,
            DEFAULT_QUERY,
            TIME_FIELD,
            TIME_FIELD,
            GROUPING_INTERVAL_MILLIS,
            start,
            end,
            headers,
            null,
            null
        );
    }

    private RecordingSyncQuery createExtractor(long start, long end, String esqlQuery, String timeField, String requiredSummaryCountField) {
        return createExtractorWithDistinctTimeFields(start, end, esqlQuery, timeField, timeField, requiredSummaryCountField);
    }

    private RecordingSyncQuery createExtractorWithDistinctTimeFields(
        long start,
        long end,
        String esqlQuery,
        String sourceTimeField,
        String emittedTimeField
    ) {
        return createExtractorWithDistinctTimeFields(start, end, esqlQuery, sourceTimeField, emittedTimeField, null);
    }

    private RecordingSyncQuery createExtractorWithDistinctTimeFields(
        long start,
        long end,
        String esqlQuery,
        String sourceTimeField,
        String emittedTimeField,
        String requiredSummaryCountField
    ) {
        EsqlDataExtractorContext context = new EsqlDataExtractorContext(
            JOB_ID,
            esqlQuery,
            sourceTimeField,
            emittedTimeField,
            GROUPING_INTERVAL_MILLIS,
            start,
            end,
            Collections.emptyMap(),
            requiredSummaryCountField,
            null
        );
        return new RecordingSyncQuery(client, context, timingStatsReporter);
    }

    private ColumnInfo column(String name, String outputType) {
        ColumnInfo col = mock(ColumnInfo.class);
        when(col.name()).thenReturn(name);
        when(col.outputType()).thenReturn(outputType);
        return col;
    }

    private static long epochMillis(String iso) {
        return Instant.parse(iso).toEpochMilli();
    }

    private static String isoAtEpochMillis(long millis) {
        return Instant.ofEpochMilli(millis).toString();
    }

    private static List<List<Object>> singleRow(Object... columnValues) {
        return List.of(Arrays.asList(columnValues));
    }

    @SuppressWarnings("unchecked")
    private EsqlResponse mockEsqlResponse(List<ColumnInfo> columns, List<List<Object>> rows) {
        EsqlResponse response = mock(EsqlResponse.class);
        doReturn(columns).when(response).columns();
        when(response.rows()).thenReturn((Iterable<Iterable<Object>>) (Iterable<?>) rows);
        return response;
    }

    private static EsqlQueryResponse wrapResponse(EsqlResponse esqlResponse) {
        return new TestEsqlQueryResponse(esqlResponse);
    }

    private static String asString(InputStream inputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    private final class RecordingSyncQuery {

        private final EsqlDataExtractor extractor;
        private final Queue<Object> responses = new ArrayDeque<>();
        final List<String> capturedQueries = new ArrayList<>();
        final List<QueryBuilder> capturedTimeFilters = new ArrayList<>();
        final List<List<EsqlQueryParam>> capturedParameterLists = new ArrayList<>();
        private boolean cancelBeforeNextFailure;
        String capturedOrderedQuery;
        QueryBuilder capturedTimeFilter;
        List<EsqlQueryParam> capturedParams;

        RecordingSyncQuery(Client client, EsqlDataExtractorContext context, DatafeedTimingStatsReporter timingStatsReporter) {
            EsqlDataExtractor[] extractorRef = new EsqlDataExtractor[1];
            EsqlSyncQueryRunner runner = (orderedQuery, timeFilter, params) -> {
                capturedOrderedQuery = orderedQuery;
                capturedTimeFilter = timeFilter;
                capturedParams = params;
                capturedQueries.add(orderedQuery);
                capturedTimeFilters.add(timeFilter);
                capturedParameterLists.add(params);
                Object response = responses.poll();
                if (cancelBeforeNextFailure) {
                    cancelBeforeNextFailure = false;
                    extractorRef[0].cancel();
                }
                if (response instanceof RuntimeException failure) {
                    throw failure;
                }
                return (EsqlQueryResponse) response;
            };
            this.extractor = new EsqlDataExtractor(client, context, timingStatsReporter, runner);
            extractorRef[0] = this.extractor;
        }

        void enqueueRow(List<ColumnInfo> columns, Object... columnValues) {
            enqueueResponse(wrapResponse(mockEsqlResponse(columns, singleRow(columnValues))));
        }

        void enqueueRows(List<ColumnInfo> columns, List<List<Object>> rows) {
            enqueueResponse(wrapResponse(mockEsqlResponse(columns, rows)));
        }

        void enqueueResponse(EsqlQueryResponse response) {
            responses.add(response);
        }

        void enqueueFailure(RuntimeException failure) {
            responses.add(failure);
        }

        void cancelBeforeNextFailure() {
            cancelBeforeNextFailure = true;
        }

        boolean hasNext() {
            return extractor.hasNext();
        }

        DataExtractor.Result next() throws IOException {
            return extractor.next();
        }

        DataExtractor.DataSummary getSummary() {
            return extractor.getSummary();
        }

        void cancel() {
            extractor.cancel();
        }

        void destroy() {
            extractor.destroy();
        }

        long getEndTime() {
            return extractor.getEndTime();
        }

        boolean isCancelled() {
            return extractor.isCancelled();
        }
    }

    private static final class NodeClientTracking {
        final CountDownLatch queryStarted = new CountDownLatch(1);
        final CountDownLatch cancelRequestReceived = new CountDownLatch(1);
        final List<CancelTasksRequest> cancelRequests = new ArrayList<>();
        final List<String> cancelOrigins = new ArrayList<>();
        final Task queryTask = new Task(7L, "transport", "esql/query", null, TaskId.EMPTY_TASK_ID, Map.of());
        Runnable cancelBeforeTaskPublication;
        ActionListener<EsqlQueryResponse> queryListener;
        String queryRunAsHeader;
    }

    @SuppressWarnings("unchecked")
    private NodeClient mockNodeClient(NodeClientTracking tracking) {
        return new NodeClient(Settings.EMPTY, trackingThreadPool, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
            @Override
            public String getLocalNodeId() {
                return "local-node";
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> Task executeAndReturnTask(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof CancelTasksRequest cancelRequest) {
                    tracking.cancelRequests.add(cancelRequest);
                    tracking.cancelOrigins.add(
                        trackingThreadPool.getThreadContext().getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME)
                    );
                    tracking.cancelRequestReceived.countDown();
                    return new Task(8L, "transport", "cluster:admin/tasks/cancel", null, TaskId.EMPTY_TASK_ID, Map.of());
                }
                tracking.queryListener = (ActionListener<EsqlQueryResponse>) listener;
                tracking.queryRunAsHeader = trackingThreadPool.getThreadContext().getHeader("es-security-runas-user");
                if (tracking.cancelBeforeTaskPublication != null) {
                    tracking.cancelBeforeTaskPublication.run();
                }
                tracking.queryStarted.countDown();
                return tracking.queryTask;
            }
        };
    }

    private static class TestEsqlQueryRequestBuilder extends EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> {
        TestEsqlQueryRequestBuilder(EsqlQueryRequest request, ActionType<EsqlQueryResponse> action) {
            super(mock(), action, request);
        }

        @Override
        public EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> query(String query) {
            return this;
        }

        @Override
        public EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> filter(QueryBuilder filter) {
            return this;
        }

        @Override
        public EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> allowPartialResults(boolean allowPartialResults) {
            return this;
        }

        @Override
        public EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> profile(boolean profile) {
            return this;
        }

        @Override
        public EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> projectRouting(String projectRouting) {
            return this;
        }

        @Override
        public EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> params(List<EsqlQueryParam> params) {
            return this;
        }
    }

    private static class TestEsqlQueryResponse extends EsqlQueryResponse {

        private final EsqlResponse esqlResponse;

        TestEsqlQueryResponse(EsqlResponse esqlResponse) {
            this.esqlResponse = esqlResponse;
        }

        @Override
        protected EsqlResponse responseInternal() {
            return esqlResponse;
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException("not needed in tests");
        }
    }
}
