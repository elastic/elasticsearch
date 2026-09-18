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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
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

    public void testExecuteTrackedCopiesParentAndCancelsTaskPublishedAfterCancel() throws Exception {
        TrackingNodeClient nodeClient = new TrackingNodeClient(trackingThreadPool);
        TaskId parentTaskId = new TaskId("parent-node", 42L);
        EsqlDataExtractor extractor = new EsqlDataExtractor(
            new ParentTaskAssigningClient(nodeClient, parentTaskId),
            context(0L, 1000L),
            timingStatsReporter
        );
        EsqlQueryRequest request = mock(EsqlQueryRequest.class);
        ActionType<EsqlQueryResponse> action = mock();
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = new TestEsqlQueryRequestBuilder(request, action);
        nodeClient.cancelBeforeTaskPublication = extractor::cancel;

        Future<EsqlQueryResponse> result = trackingThreadPool.generic().submit(() -> extractor.execute(builder));
        assertTrue(nodeClient.queryStarted.await(10, TimeUnit.SECONDS));
        assertTrue(nodeClient.cancelRequestReceived.await(10, TimeUnit.SECONDS));
        assertThat(nodeClient.cancelRequests.size(), equalTo(1));
        assertThat(nodeClient.cancelRequests.get(0).getTargetTaskId(), equalTo(new TaskId("local-node", 7L)));
        assertThat(nodeClient.cancelOrigins, equalTo(List.of("tasks")));
        verify(request).setParentTask(parentTaskId);

        nodeClient.queryListener.onResponse(mock(EsqlQueryResponse.class));
        result.get(10, TimeUnit.SECONDS);
        extractor.destroy();
        assertThat(nodeClient.cancelRequests.size(), equalTo(1));
    }

    public void testExecuteTrackedClearsTaskAfterNormalCompletion() throws Exception {
        TrackingNodeClient nodeClient = new TrackingNodeClient(trackingThreadPool);
        EsqlDataExtractor extractor = new EsqlDataExtractor(nodeClient, context(0L, 1000L), timingStatsReporter);
        EsqlQueryRequest request = mock(EsqlQueryRequest.class);
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = new TestEsqlQueryRequestBuilder(request, mock());

        Future<EsqlQueryResponse> result = trackingThreadPool.generic().submit(() -> extractor.execute(builder));
        assertTrue(nodeClient.queryStarted.await(10, TimeUnit.SECONDS));
        nodeClient.queryListener.onResponse(mock(EsqlQueryResponse.class));
        result.get(10, TimeUnit.SECONDS);

        extractor.destroy();
        assertThat(nodeClient.cancelRequests, equalTo(List.of()));
    }

    public void testExecuteTrackedPassesStoredDatafeedHeadersToEsqlQuery() throws Exception {
        TrackingNodeClient nodeClient = new TrackingNodeClient(trackingThreadPool);
        EsqlDataExtractor extractor = new EsqlDataExtractor(
            nodeClient,
            contextWithHeaders(0L, 1000L, Map.of("es-security-runas-user", "stored-datafeed-user")),
            timingStatsReporter
        );
        EsqlQueryRequest request = mock(EsqlQueryRequest.class);
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = new TestEsqlQueryRequestBuilder(request, mock());

        Future<EsqlQueryResponse> result = trackingThreadPool.generic().submit(() -> extractor.execute(builder));
        assertTrue(nodeClient.queryStarted.await(10, TimeUnit.SECONDS));
        assertThat(nodeClient.queryRunAsHeader, equalTo("stored-datafeed-user"));

        nodeClient.queryListener.onResponse(mock(EsqlQueryResponse.class));
        result.get(10, TimeUnit.SECONDS);
    }

    public void testNextGivenSortIsInjected() throws IOException {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE), column("value", LONG)), isoAtEpochMillis(1500L), 42L);

        assertThat(extractor.hasNext(), is(true));
        DataExtractor.Result result = extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(DEFAULT_QUERY + " | LIMIT 10000 | SORT ??timeField ASC"));
        assertThat(extractor.capturedParams, equalTo(List.of(new EsqlQueryParam("timeField", "timestamp", IDENTIFIER))));
        assertThat(result.rowCount(), equalTo(1L));
        verify(timingStatsReporter).reportSearchDuration(any());
    }

    public void testNextBuildsEsqlRequestWithoutIndicesOptions() throws IOException {
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = mock();
        when(builder.query(any())).thenReturn(builder);
        when(builder.filter(any())).thenReturn(builder);
        when(builder.params(any())).thenReturn(builder);
        when(builder.allowPartialResults(anyBoolean())).thenReturn(builder);
        SharedSecrets.setEsqlQueryRequestBuilderAccess(client -> builder);
        try {
            RequestCapturingDataExtractor extractor = new RequestCapturingDataExtractor(context(1000L, 2000L));

            extractor.next();

            verify(builder).query(DEFAULT_QUERY + " | LIMIT 10000 | SORT ??timeField ASC");
            verify(builder).filter(any(RangeQueryBuilder.class));
            verify(builder).params(List.of(new EsqlQueryParam("timeField", TIME_FIELD, IDENTIFIER)));
            verify(builder).allowPartialResults(false);
            assertThat(extractor.capturedRequest, sameInstance(builder));
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

    public void testNextGivenSortIsInjectedEvenWhenUserQueryAlreadyHasSort() throws IOException {
        String esqlQuery = "FROM logs | SORT value DESC";
        TestDataExtractor extractor = createExtractor(1000L, 2000L, esqlQuery, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE), column("value", LONG)), isoAtEpochMillis(1500L), 1L);

        extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(esqlQuery + " | LIMIT 10000 | SORT ??timeField ASC"));
    }

    public void testQueryWithoutLimitCommandShouldAppendDefaultLimit() {
        String query = "FROM logs-* | KEEP @timestamp, bytes";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query + " | LIMIT 10000"));
    }

    public void testQueryWithLimitCommandShouldPreserveUserLimit() {
        String query = "FROM logs-* | LIMIT 20";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query));
    }

    public void testNextGivenUserLimitShouldKeepUserLimitAndAppendTimeSort() throws IOException {
        String query = "FROM logs-* | LIMIT 20";
        TestDataExtractor extractor = createExtractor(1000L, 2000L, query, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(query + " | SORT ??timeField ASC"));
    }

    public void testNextGivenUserLimitEndingInLineCommentShouldAppendTimeSortOnNewLine() throws IOException {
        String query = "FROM logs-* | LIMIT 20 // explanation";
        TestDataExtractor extractor = createExtractor(1000L, 2000L, query, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(query + "\n | SORT ??timeField ASC"));
    }

    public void testQueryWithLimitInQuotedStringShouldAppendDefaultLimit() {
        String query = "FROM logs-* | WHERE message == \"LIMIT 20\"";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query + " | LIMIT 10000"));
    }

    public void testQueryWithLimitInTripleQuotedStringShouldAppendDefaultLimit() {
        String query = "FROM logs-* | WHERE message == \"\"\"| LIMIT 20\"\"\"";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query + " | LIMIT 10000"));
    }

    public void testNextGivenFourQuoteTripleStringAndOuterLimitShouldAppendTimeSort() throws IOException {
        String query = "FROM logs-* | WHERE message == \"\"\"literal\"\"\"\" | LIMIT 20";
        TestDataExtractor extractor = createExtractor(1000L, 2000L, query, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(query + " | SORT ??timeField ASC"));
    }

    public void testQueryWithFiveQuoteTripleStringAndOuterLimitShouldPreserveOuterLimit() {
        String query = "FROM logs-* | WHERE message == \"\"\"literal\"\"\"\"\" | LIMIT 20";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query));
    }

    public void testQueryWithLimitInCommentShouldAppendDefaultLimit() {
        String query = "FROM logs-* // | LIMIT 20\n| KEEP @timestamp";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query + " | LIMIT 10000"));
    }

    public void testQueryWithLimitInBlockCommentShouldAppendDefaultLimit() {
        String query = "FROM logs-* /* | LIMIT 20 */ | KEEP @timestamp";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query + " | LIMIT 10000"));
    }

    public void testQueryWithLimitInNestedBlockCommentShouldAppendDefaultLimit() {
        String query = "FROM logs-* /* outer /* | LIMIT 20 */ comment */ | KEEP @timestamp";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query + " | LIMIT 10000"));
    }

    public void testQueryWithLimitSubstringShouldAppendDefaultLimit() {
        String query = "FROM logs-* | KEEP limit_value, `LIMIT``_value`";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query + " | LIMIT 10000"));
    }

    public void testQueryWithLimitInInSubqueryShouldAppendDefaultLimit() {
        String query = "FROM logs-* | WHERE id IN (FROM other-logs | LIMIT 3 | KEEP id)";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query + " | LIMIT 10000"));
    }

    public void testQueryWithLimitInForkBranchesShouldAppendDefaultLimit() {
        String query = "FROM logs-* | FORK (WHERE level == \"warn\" | LIMIT 3) (WHERE level == \"error\" | LIMIT 4)";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query + " | LIMIT 10000"));
    }

    public void testQueryWithNestedAndOuterLimitShouldPreserveOuterLimit() {
        String query = "FROM logs-* | WHERE id IN (FROM other-logs | LIMIT 3 | KEEP id) | LIMIT 20";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query));
    }

    public void testQueryEndingInLineCommentShouldAppendLimitOnNewLine() {
        String query = "FROM logs-* // no user limit";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query + "\n | LIMIT 10000"));
    }

    public void testMultilineMixedCaseLimitCommandShouldPreserveUserLimit() {
        String query = "FROM logs-*\n| KEEP @timestamp\n| lImIt 42";

        assertThat(EsqlDataExtractor.maybeInjectLimit(query), equalTo(query));
    }

    public void testNextGivenTimeFilterIsHalfOpen() throws IOException {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        assertThat(extractor.capturedTimeFilter, equalTo(new RangeQueryBuilder("timestamp").gte(1000L).lt(2000L).format("epoch_millis")));
    }

    public void testNextGivenStringAndLongFieldsPassThrough() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 5000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(
            List.of(column(TIME_FIELD, DATE), column("host", KEYWORD), column("count", LONG)),
            "1970-01-01T00:00:01.500Z",
            "web-1",
            7L
        );

        DataExtractor.Result result = extractor.next();

        assertThat(result.searchInterval(), equalTo(new SearchInterval(0L, 5000L)));
        String ndjson = asString(result.data().get());
        assertThat(ndjson, containsString("\"" + TIME_FIELD + "\":1500"));
        assertThat(ndjson, containsString("\"host\":\"web-1\""));
        assertThat(ndjson, containsString("\"count\":7"));
    }

    public void testNextGivenDateColumnConvertedToEpochMillis() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        String isoDate = isoAtEpochMillis(500L);
        long expectedMillis = 500L;

        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), isoDate);

        assertThat(asString(extractor.next().data().get()), equalTo("{\"" + TIME_FIELD + "\":" + expectedMillis + "}"));
    }

    public void testNextGivenDateNanosColumnConvertedToEpochMillis() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        String isoDate = "1970-01-01T00:00:00.500123456Z";
        long expectedMillis = epochMillis(isoDate);

        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE_NANOS)), isoDate);

        assertThat(asString(extractor.next().data().get()), equalTo("{\"" + TIME_FIELD + "\":" + expectedMillis + "}"));
    }

    public void testNextGivenMultiValuedEmittedTimeShouldFailWholeBatch() {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), List.of(isoAtEpochMillis(100L), isoAtEpochMillis(200L)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString("multi-valued"));
    }

    public void testNextGivenMultiValuedNonDateColumnPassedThrough() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE), column("tags", KEYWORD)), isoAtEpochMillis(100L), List.of("a", "b"));

        assertThat(asString(extractor.next().data().get()), containsString("\"tags\":[\"a\",\"b\"]"));
    }

    public void testNextGivenNullFieldIsOmittedFromRow() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(
            List.of(column(TIME_FIELD, DATE), column("optional_field", KEYWORD)),
            List.of(Arrays.asList(isoAtEpochMillis(100L), null))
        );

        String ndjson = asString(extractor.next().data().get());
        assertThat(ndjson, containsString("\"" + TIME_FIELD + "\":"));
        assertThat(ndjson, not(containsString("optional_field")));
    }

    public void testNextGivenMissingTimeFieldThrowsIllegalArgument() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column("other_field", KEYWORD)), "some-value");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString("final ES|QL output"));
        assertThat(e.getMessage(), containsString("data_description.time_field [" + TIME_FIELD + "]"));
    }

    public void testNextGivenMissingSummaryCountFieldThrowsIllegalArgument() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD, SUMMARY_COUNT_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), isoAtEpochMillis(100L));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(SUMMARY_COUNT_FIELD));
        assertThat(e.getMessage(), containsString("numeric count column"));
    }

    public void testNextGivenSummaryCountFieldPresentSucceeds() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD, SUMMARY_COUNT_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE), column(SUMMARY_COUNT_FIELD, LONG)), isoAtEpochMillis(100L), 5L);

        DataExtractor.Result result = extractor.next();

        String ndjson = asString(result.data().get());
        assertThat(ndjson, containsString("\"" + SUMMARY_COUNT_FIELD + "\":5"));
    }

    public void testNextGivenNoRowsReturnsEmptyOptional() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(List.of(column(TIME_FIELD, DATE)), List.of());

        DataExtractor.Result result = extractor.next();

        assertThat(result.searchInterval(), equalTo(new SearchInterval(0L, 10000L)));
        assertThat(result.data().isPresent(), is(false));
    }

    public void testNextGivenMalformedDateThrows() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), "not-a-valid-iso-date");

        expectThrows(Exception.class, extractor::next);
    }

    public void testHasNextIsTrueThenFalseAfterSingleCall() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 1000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(List.of(column(TIME_FIELD, DATE)), List.of());

        assertThat(extractor.hasNext(), is(true));
        extractor.next();
        assertThat(extractor.hasNext(), is(false));
    }

    public void testNextAfterExhaustionThrowsNoSuchElement() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 1000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(List.of(column(TIME_FIELD, DATE)), List.of());

        extractor.next();
        expectThrows(NoSuchElementException.class, extractor::next);
    }

    public void testGetEndTimeReturnsContextEnd() {
        TestDataExtractor extractor = createExtractor(500L, 9999L, DEFAULT_QUERY, TIME_FIELD);
        assertThat(extractor.getEndTime(), equalTo(9999L));
    }

    public void testCancelReturnsEmptyResultWithoutExecutingQuery() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 1000L, DEFAULT_QUERY, TIME_FIELD);

        assertThat(extractor.isCancelled(), is(false));
        extractor.cancel();
        assertThat(extractor.isCancelled(), is(true));

        assertThat(extractor.hasNext(), is(true));
        DataExtractor.Result result = extractor.next();
        assertThat(result.data().isPresent(), is(false));
        assertThat(extractor.capturedOrderedQuery, equalTo(null));
        verify(timingStatsReporter, never()).reportSearchDuration(any());
    }

    public void testDestroyAlsoCancels() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 1000L, DEFAULT_QUERY, TIME_FIELD);

        extractor.destroy();
        assertThat(extractor.isCancelled(), is(true));

        DataExtractor.Result result = extractor.next();
        assertThat(result.data().isPresent(), is(false));
        assertThat(extractor.capturedOrderedQuery, equalTo(null));
        verify(timingStatsReporter, never()).reportSearchDuration(any());
    }

    public void testNodeClosedOnceShouldRetrySameRange() throws IOException {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueFailure(new NodeClosedException((DiscoveryNode) null));
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        String expectedQuery = DEFAULT_QUERY + " | LIMIT 10000 | SORT ??timeField ASC";
        QueryBuilder expectedFilter = new RangeQueryBuilder(TIME_FIELD).gte(1000L).lt(2000L).format("epoch_millis");
        List<EsqlQueryParam> expectedParams = List.of(new EsqlQueryParam("timeField", TIME_FIELD, IDENTIFIER));
        assertThat(extractor.capturedQueries, equalTo(List.of(expectedQuery, expectedQuery)));
        assertThat(extractor.capturedTimeFilters, equalTo(List.of(expectedFilter, expectedFilter)));
        assertThat(extractor.capturedParameterLists, equalTo(List.of(expectedParams, expectedParams)));
    }

    public void testNodeClosedTwiceShouldFailCycleWithoutAdditionalRetry() {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueFailure(new NodeClosedException((DiscoveryNode) null));
        extractor.enqueueFailure(new NodeClosedException((DiscoveryNode) null));

        expectThrows(NodeClosedException.class, extractor::next);

        assertThat(extractor.capturedQueries.size(), equalTo(2));
    }

    public void testCancelledInFlightQueryShouldNotRetry() {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.cancelBeforeNextFailure();
        extractor.enqueueFailure(new NodeClosedException((DiscoveryNode) null));

        expectThrows(NodeClosedException.class, extractor::next);

        assertThat(extractor.capturedQueries.size(), equalTo(1));
    }

    public void testGetSummaryBuildsEsqlStatsQueryAndConvertsDateColumns() {
        TestDataExtractor extractor = createExtractor(1000L, 9000L, DEFAULT_QUERY, TIME_FIELD);
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

    public void testGetSummaryReturnsNoDataWhenStatsRowHasNullMinMax() {
        TestDataExtractor extractor = createExtractor(1000L, 9000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(
            List.of(column("earliest_time", DATE), column("latest_time", DATE), column("total_hits", LONG)),
            List.of(Arrays.asList(null, null, 0L))
        );

        DataExtractor.DataSummary summary = extractor.getSummary();

        assertThat(summary.hasData(), is(false));
        assertThat(summary.totalHits(), equalTo(0L));
    }

    public void testGetSummaryPassesDoesNotSubstituteInvalidTimeField() {
        String invalidTimeField = "field | STATS COUNT(*)";
        TestDataExtractor extractor = createExtractor(1000L, 9000L, DEFAULT_QUERY, invalidTimeField);
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

    public void testGetSummaryDetectsTimeColumnTypesIndependently() {
        TestDataExtractor extractor = createExtractor(1000L, 9000L, DEFAULT_QUERY, TIME_FIELD);
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

    public void testNextGivenScalarEmittedTimeInWindowShouldSucceed() throws IOException {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, LONG)), 1500L);

        assertThat(asString(extractor.next().data().get()), equalTo("{\"" + TIME_FIELD + "\":1500}"));
    }

    public void testNextGivenEmittedTimeBeforeStartShouldFailWholeBatch() {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, LONG)), 999L);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString("before the source window start"));
        assertThat(e.getMessage(), containsString("1000"));
        assertThat(e.getMessage(), containsString("2000"));
    }

    public void testNextGivenEmittedTimeAtEndShouldFailWholeBatch() {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, LONG)), 2000L);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString("at or after the source window end"));
    }

    public void testNextGivenNullEmittedTimeShouldFailWholeBatch() {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(List.of(column(TIME_FIELD, DATE)), List.of(Arrays.asList((Object) null)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString("value is null"));
    }

    public void testNextGivenWrongTypeEmittedTimeShouldFailWholeBatch() {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), "not-a-valid-iso-date");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString("unsupported type"));
    }

    public void testNextGivenInvalidEmittedTimeShouldNotExposeNdjson() {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(List.of(column(TIME_FIELD, LONG)), List.of(List.of(1500L), List.of(2500L)));

        expectThrows(IllegalArgumentException.class, extractor::next);
    }

    public void testRenamedEmittedTimeFieldShouldFilterOnSourceTimeField() throws IOException {
        TestDataExtractor extractor = createExtractorWithDistinctTimeFields(1000L, 2000L, DEFAULT_QUERY, SOURCE_TIME_FIELD, "bucket_time");
        extractor.enqueueRow(List.of(column("bucket_time", DATE)), isoAtEpochMillis(1500L));

        extractor.next();

        assertThat(
            extractor.capturedTimeFilter,
            equalTo(new RangeQueryBuilder(SOURCE_TIME_FIELD).gte(1000L).lt(2000L).format("epoch_millis"))
        );
        assertThat(extractor.capturedParams, equalTo(List.of(new EsqlQueryParam("timeField", "bucket_time", IDENTIFIER))));
    }

    public void testQuerySummaryShouldNotUseSourceFieldAfterPipeline() {
        TestDataExtractor extractor = createExtractorWithDistinctTimeFields(1000L, 9000L, DEFAULT_QUERY, SOURCE_TIME_FIELD, "bucket_time");
        extractor.enqueueRow(
            List.of(column("earliest_time", DATE), column("latest_time", DATE), column("total_hits", LONG)),
            "1970-01-01T00:00:01.500Z",
            "1970-01-01T00:00:08.500Z",
            42L
        );

        extractor.getSummary();

        assertThat(extractor.capturedOrderedQuery, not(containsString(SOURCE_TIME_FIELD)));
        assertThat(extractor.capturedParams, equalTo(List.of(new EsqlQueryParam("timeField", "bucket_time", IDENTIFIER))));
        assertThat(
            extractor.capturedTimeFilter,
            equalTo(new RangeQueryBuilder(SOURCE_TIME_FIELD).gte(1000L).lt(9000L).format("epoch_millis"))
        );
    }

    private TestDataExtractor createExtractor(long start, long end, String esqlQuery, String timeField) {
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

    private TestDataExtractor createExtractor(long start, long end, String esqlQuery, String timeField, String requiredSummaryCountField) {
        return createExtractorWithDistinctTimeFields(start, end, esqlQuery, timeField, timeField, requiredSummaryCountField);
    }

    private TestDataExtractor createExtractorWithDistinctTimeFields(
        long start,
        long end,
        String esqlQuery,
        String sourceTimeField,
        String emittedTimeField
    ) {
        return createExtractorWithDistinctTimeFields(start, end, esqlQuery, sourceTimeField, emittedTimeField, null);
    }

    private TestDataExtractor createExtractorWithDistinctTimeFields(
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
        return new TestDataExtractor(context);
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

    /**
     * Test subclass of {@link EsqlDataExtractor} that overrides {@link #runEsqlQuery} to avoid the
     * {@code SharedSecrets} / esql-plugin dependency that is absent from the ml plugin's test classpath.
     * The override captures the query parameters for assertion and returns responses from a queue.
     */
    private class TestDataExtractor extends EsqlDataExtractor {

        private final Queue<Object> responses = new ArrayDeque<>();
        private final List<String> capturedQueries = new ArrayList<>();
        private final List<QueryBuilder> capturedTimeFilters = new ArrayList<>();
        private final List<List<EsqlQueryParam>> capturedParameterLists = new ArrayList<>();
        private boolean cancelBeforeNextFailure;
        String capturedOrderedQuery;
        QueryBuilder capturedTimeFilter;
        List<EsqlQueryParam> capturedParams;

        TestDataExtractor(EsqlDataExtractorContext context) {
            super(client, context, timingStatsReporter);
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

        @Override
        protected EsqlQueryResponse runEsqlQuery(String orderedQuery, QueryBuilder timeFilter, List<EsqlQueryParam> params) {
            capturedOrderedQuery = orderedQuery;
            capturedTimeFilter = timeFilter;
            capturedParams = params;
            capturedQueries.add(orderedQuery);
            capturedTimeFilters.add(timeFilter);
            capturedParameterLists.add(params);
            Object response = responses.poll();
            if (cancelBeforeNextFailure) {
                cancelBeforeNextFailure = false;
                cancel();
            }
            if (response instanceof RuntimeException failure) {
                throw failure;
            }
            return (EsqlQueryResponse) response;
        }
    }

    private class RequestCapturingDataExtractor extends EsqlDataExtractor {

        private EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> capturedRequest;

        RequestCapturingDataExtractor(EsqlDataExtractorContext context) {
            super(client, context, timingStatsReporter);
        }

        @Override
        EsqlQueryResponse execute(EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request) {
            capturedRequest = request;
            return wrapResponse(mockEsqlResponse(List.of(column(TIME_FIELD, DATE)), List.of()));
        }
    }

    private static class TrackingNodeClient extends NodeClient {
        private final CountDownLatch queryStarted = new CountDownLatch(1);
        private final CountDownLatch cancelRequestReceived = new CountDownLatch(1);
        private final List<CancelTasksRequest> cancelRequests = new ArrayList<>();
        private final List<String> cancelOrigins = new ArrayList<>();
        private final Task queryTask = new Task(7L, "transport", "esql/query", null, TaskId.EMPTY_TASK_ID, Map.of());
        private Runnable cancelBeforeTaskPublication;
        private ActionListener<EsqlQueryResponse> queryListener;
        private String queryRunAsHeader;

        TrackingNodeClient(ThreadPool threadPool) {
            super(Settings.EMPTY, threadPool, TestProjectResolvers.mustExecuteFirst());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeAndReturnTask(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof CancelTasksRequest cancelRequest) {
                cancelRequests.add(cancelRequest);
                cancelOrigins.add(threadPool().getThreadContext().getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME));
                cancelRequestReceived.countDown();
                return new Task(8L, "transport", "cluster:admin/tasks/cancel", null, TaskId.EMPTY_TASK_ID, Map.of());
            }
            queryListener = (ActionListener<EsqlQueryResponse>) listener;
            queryRunAsHeader = threadPool().getThreadContext().getHeader("es-security-runas-user");
            if (cancelBeforeTaskPublication != null) {
                cancelBeforeTaskPublication.run();
            }
            queryStarted.countDown();
            return queryTask;
        }

        @Override
        public String getLocalNodeId() {
            return "local-node";
        }
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
