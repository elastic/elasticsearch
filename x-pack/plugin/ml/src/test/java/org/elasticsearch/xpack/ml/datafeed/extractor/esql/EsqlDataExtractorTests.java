/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EsqlDataExtractorTests extends ESTestCase {

    private Client client;
    private DatafeedTimingStatsReporter timingStatsReporter;

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(mock(ThreadPool.class));
        when(client.threadPool().getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        timingStatsReporter = mock(DatafeedTimingStatsReporter.class);
    }

    // -------------------------------------------------------------------------
    // next() — query shape
    // -------------------------------------------------------------------------

    public void testNextGivenSortIsInjected() throws IOException {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, "FROM logs", "timestamp");
        extractor.enqueueResponse(
            wrapResponse(
                mockEsqlResponse(List.of(column("timestamp", "date"), column("value", "long")), singleRow("2000-01-01T00:00:00.100Z", 42L))
            )
        );

        assertThat(extractor.hasNext(), is(true));
        extractor.next();

        // SORT is appended using backtick-quoting, regardless of existing query content
        assertThat(extractor.capturedOrderedQuery, equalTo("FROM logs | SORT `timestamp` ASC"));
    }

    public void testNextGivenSortIsInjectedEvenWhenUserQueryAlreadyHasSort() throws IOException {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, "FROM logs | SORT value DESC", "timestamp");
        extractor.enqueueResponse(
            wrapResponse(
                mockEsqlResponse(List.of(column("timestamp", "date"), column("value", "long")), singleRow("2000-01-01T00:00:00.100Z", 1L))
            )
        );

        extractor.next();

        // The trailing SORT overrides any user-supplied SORT
        assertThat(extractor.capturedOrderedQuery, equalTo("FROM logs | SORT value DESC | SORT `timestamp` ASC"));
    }

    public void testNextGivenTimeFilterIsHalfOpen() throws IOException {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, "FROM logs", "timestamp");
        extractor.enqueueResponse(
            wrapResponse(mockEsqlResponse(List.of(column("timestamp", "date")), singleRow("2000-01-01T00:00:01.000Z")))
        );

        extractor.next();

        assertThat(extractor.capturedTimeFilter, equalTo(new RangeQueryBuilder("timestamp").gte(1000L).lt(2000L).format("epoch_millis")));
    }

    // -------------------------------------------------------------------------
    // next() — toNdjson data conversion
    // -------------------------------------------------------------------------

    public void testNextGivenStringAndLongFieldsPassThrough() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 5000L, "FROM logs", "ts");
        extractor.enqueueResponse(
            wrapResponse(
                mockEsqlResponse(
                    List.of(column("ts", "date"), column("host", "keyword"), column("count", "long")),
                    singleRow("1970-01-01T00:00:01.500Z", "web-1", 7L)
                )
            )
        );

        DataExtractor.Result result = extractor.next();

        assertThat(result.searchInterval(), equalTo(new SearchInterval(0L, 5000L)));
        String ndjson = asString(result.data().get());
        // ts is a date column → epoch millis; host and count pass through unchanged
        assertThat(ndjson, containsString("\"ts\":1500"));
        assertThat(ndjson, containsString("\"host\":\"web-1\""));
        assertThat(ndjson, containsString("\"count\":7"));
    }

    public void testNextGivenDateColumnConvertedToEpochMillis() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, "FROM logs", "ts");
        String isoDate = "2000-01-01T00:00:00.500Z";
        long expectedMillis = Instant.parse(isoDate).toEpochMilli();

        extractor.enqueueResponse(wrapResponse(mockEsqlResponse(List.of(column("ts", "date")), singleRow(isoDate))));

        assertThat(asString(extractor.next().data().get()), equalTo("{\"ts\":" + expectedMillis + "}"));
    }

    public void testNextGivenDateNanosColumnIsAlsoConverted() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, "FROM logs", "ts");
        String isoDate = "2000-01-01T00:00:00.123456789Z";
        long expectedMillis = Instant.parse(isoDate).toEpochMilli();

        extractor.enqueueResponse(wrapResponse(mockEsqlResponse(List.of(column("ts", "date_nanos")), singleRow(isoDate))));

        assertThat(asString(extractor.next().data().get()), equalTo("{\"ts\":" + expectedMillis + "}"));
    }

    public void testNextGivenMultiValuedDateColumnConvertedElementWise() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, "FROM logs", "ts");
        String iso1 = "2000-01-01T00:00:00.100Z";
        String iso2 = "2000-01-01T00:00:00.200Z";
        long millis1 = Instant.parse(iso1).toEpochMilli();
        long millis2 = Instant.parse(iso2).toEpochMilli();

        // The column value is itself a List (multi-value); pass it as a single Object
        extractor.enqueueResponse(wrapResponse(mockEsqlResponse(List.of(column("ts", "date")), singleRow((Object) List.of(iso1, iso2)))));

        assertThat(asString(extractor.next().data().get()), equalTo("{\"ts\":[" + millis1 + "," + millis2 + "]}"));
    }

    public void testNextGivenMultiValuedNonDateColumnPassedThrough() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, "FROM logs", "ts");
        extractor.enqueueResponse(
            wrapResponse(
                mockEsqlResponse(
                    List.of(column("ts", "date"), column("tags", "keyword")),
                    singleRow("2000-01-01T00:00:00.100Z", (Object) List.of("a", "b"))
                )
            )
        );

        assertThat(asString(extractor.next().data().get()), containsString("\"tags\":[\"a\",\"b\"]"));
    }

    public void testNextGivenNullFieldIsOmittedFromRow() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, "FROM logs", "ts");
        // Arrays.asList permits null; List.of does not
        List<Object> rowWithNull = Arrays.asList("2000-01-01T00:00:00.100Z", null);
        extractor.enqueueResponse(
            wrapResponse(mockEsqlResponseFromRows(List.of(column("ts", "date"), column("optional_field", "keyword")), List.of(rowWithNull)))
        );

        String ndjson = asString(extractor.next().data().get());
        assertThat(ndjson, containsString("\"ts\":"));
        assertThat(ndjson, not(containsString("optional_field")));
    }

    public void testNextGivenMissingTimeFieldThrowsIllegalArgument() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, "FROM logs", "ts");
        // Response has no column named "ts" — the configured time field is absent
        extractor.enqueueResponse(wrapResponse(mockEsqlResponse(List.of(column("other_field", "keyword")), singleRow("some-value"))));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString("ts"));
        assertThat(e.getMessage(), containsString("ESQL query response is missing the configured time field"));
    }

    public void testNextGivenNoRowsReturnsEmptyOptional() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, "FROM logs", "ts");
        extractor.enqueueResponse(wrapResponse(mockEsqlResponse(List.of(column("ts", "date")), Collections.emptyList())));

        DataExtractor.Result result = extractor.next();

        assertThat(result.searchInterval(), equalTo(new SearchInterval(0L, 10000L)));
        assertThat(result.data().isPresent(), is(false));
    }

    public void testNextGivenMalformedDateThrows() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, "FROM logs", "ts");
        extractor.enqueueResponse(wrapResponse(mockEsqlResponse(List.of(column("ts", "date")), singleRow("not-a-valid-iso-date"))));

        expectThrows(Exception.class, extractor::next);
    }

    // -------------------------------------------------------------------------
    // next() — iteration protocol
    // -------------------------------------------------------------------------

    public void testHasNextIsTrueThenFalseAfterSingleCall() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 1000L, "FROM logs", "ts");
        extractor.enqueueResponse(wrapResponse(mockEsqlResponse(List.of(column("ts", "date")), Collections.emptyList())));

        assertThat(extractor.hasNext(), is(true));
        extractor.next();
        assertThat(extractor.hasNext(), is(false));
    }

    public void testNextAfterExhaustionThrowsNoSuchElement() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 1000L, "FROM logs", "ts");
        extractor.enqueueResponse(wrapResponse(mockEsqlResponse(List.of(column("ts", "date")), Collections.emptyList())));

        extractor.next();
        expectThrows(NoSuchElementException.class, extractor::next);
    }

    public void testGetEndTimeReturnsContextEnd() {
        TestDataExtractor extractor = createExtractor(500L, 9999L, "FROM logs", "ts");
        assertThat(extractor.getEndTime(), equalTo(9999L));
    }

    // -------------------------------------------------------------------------
    // cancel / destroy
    // -------------------------------------------------------------------------

    public void testCancelReturnsEmptyResultWithoutExecutingQuery() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 1000L, "FROM logs", "ts");

        assertThat(extractor.isCancelled(), is(false));
        extractor.cancel();
        assertThat(extractor.isCancelled(), is(true));

        // hasNext() is still true (extractor has not run yet), but next() short-circuits
        assertThat(extractor.hasNext(), is(true));
        DataExtractor.Result result = extractor.next();
        assertThat(result.data().isPresent(), is(false));

        // runEsqlQuery was never called — no response was enqueued and none was consumed
        assertThat(extractor.capturedOrderedQuery, equalTo(null));
    }

    public void testDestroyAlsoCancels() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 1000L, "FROM logs", "ts");

        extractor.destroy();
        assertThat(extractor.isCancelled(), is(true));

        DataExtractor.Result result = extractor.next();
        assertThat(result.data().isPresent(), is(false));
        assertThat(extractor.capturedOrderedQuery, equalTo(null));
    }

    // -------------------------------------------------------------------------
    // getSummary
    // -------------------------------------------------------------------------

    public void testGetSummaryUsesMatchAllQueryAndReportsSearchDuration() {
        TestDataExtractor extractor = createExtractor(1000L, 9000L, "FROM logs", "ts");

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        ActionFuture<SearchResponse> future = toActionFuture(createSummarySearchResponse(1500L, 8500L, 100L));
        when(client.execute(eq(TransportSearchAction.TYPE), captor.capture())).thenReturn(future);

        DataExtractor.DataSummary summary = extractor.getSummary();

        assertThat(summary.earliestTime(), equalTo(1500L));
        assertThat(summary.latestTime(), equalTo(8500L));
        assertThat(summary.totalHits(), equalTo(100L));

        // Uses a match_all query — not the esql_query string
        String searchRequestStr = captor.getValue().toString().replaceAll("\\s", "");
        assertThat(searchRequestStr, containsString("match_all"));

        verify(timingStatsReporter).reportSearchDuration(any(TimeValue.class));
    }

    // -------------------------------------------------------------------------
    // Helper factories
    // -------------------------------------------------------------------------

    private TestDataExtractor createExtractor(long start, long end, String esqlQuery, String timeField) {
        EsqlDataExtractorContext context = new EsqlDataExtractorContext(
            "test-job",
            esqlQuery,
            timeField,
            start,
            end,
            Collections.emptyMap(),
            List.of("test-index"),
            IndicesOptions.STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED
        );
        return new TestDataExtractor(context);
    }

    private ColumnInfo column(String name, String outputType) {
        ColumnInfo col = mock(ColumnInfo.class);
        when(col.name()).thenReturn(name);
        when(col.outputType()).thenReturn(outputType);
        return col;
    }

    /**
     * Wraps the given column values into a single-row {@code Iterable<Iterable<Object>>} that
     * mirrors what {@link EsqlResponse#rows()} returns. Each argument is one column value in the row.
     */
    private static Iterable<Iterable<Object>> singleRow(Object... columnValues) {
        return List.of(Arrays.asList(columnValues));
    }

    private EsqlResponse mockEsqlResponse(List<ColumnInfo> columns, Iterable<Iterable<Object>> rows) {
        EsqlResponse response = mock(EsqlResponse.class);
        doReturn(columns).when(response).columns();
        when(response.rows()).thenReturn(rows);
        return response;
    }

    /**
     * Variant for cases where rows are pre-built as {@code List<List<Object>>} (e.g. rows containing nulls).
     */
    @SuppressWarnings("unchecked")
    private EsqlResponse mockEsqlResponseFromRows(List<ColumnInfo> columns, List<List<Object>> rows) {
        EsqlResponse response = mock(EsqlResponse.class);
        doReturn(columns).when(response).columns();
        when(response.rows()).thenReturn((Iterable<Iterable<Object>>) (Iterable<?>) rows);
        return response;
    }

    private static EsqlQueryResponse wrapResponse(EsqlResponse esqlResponse) {
        return new TestEsqlQueryResponse(esqlResponse);
    }

    private <T> ActionFuture<T> toActionFuture(T value) {
        @SuppressWarnings("unchecked")
        ActionFuture<T> future = mock(ActionFuture.class);
        when(future.actionGet()).thenReturn(value);
        return future;
    }

    private SearchResponse createSummarySearchResponse(long earliest, long latest, long totalHits) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(
            new SearchHits(SearchHits.EMPTY, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 1)
        );
        when(searchResponse.getAggregations()).thenReturn(
            InternalAggregations.from(List.of(new Min("earliest_time", earliest, null, null), new Max("latest_time", latest, null, null)))
        );
        when(searchResponse.getTook()).thenReturn(TimeValue.timeValueMillis(randomNonNegativeLong()));
        return searchResponse;
    }

    private static String asString(InputStream inputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    // -------------------------------------------------------------------------
    // Test infrastructure
    // -------------------------------------------------------------------------

    /**
     * Test subclass of {@link EsqlDataExtractor} that overrides {@link #runEsqlQuery} to avoid the
     * {@code SharedSecrets} / esql-plugin dependency that is absent from the ml plugin's test classpath.
     * The override captures the query parameters for assertion and returns responses from a queue.
     */
    private class TestDataExtractor extends EsqlDataExtractor {

        private final Queue<EsqlQueryResponse> responses = new ArrayDeque<>();
        String capturedOrderedQuery;
        QueryBuilder capturedTimeFilter;

        TestDataExtractor(EsqlDataExtractorContext context) {
            super(client, context, timingStatsReporter);
        }

        void enqueueResponse(EsqlQueryResponse response) {
            responses.add(response);
        }

        @Override
        protected EsqlQueryResponse runEsqlQuery(String orderedQuery, QueryBuilder timeFilter) {
            capturedOrderedQuery = orderedQuery;
            capturedTimeFilter = timeFilter;
            return responses.poll();
        }
    }

    /**
     * Minimal concrete {@link EsqlQueryResponse} used in tests. {@code writeTo} is not needed and
     * intentionally unsupported.
     */
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
