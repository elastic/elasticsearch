/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.junit.Before;

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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EsqlDataExtractorTests extends ESTestCase {

    private static final String DEFAULT_QUERY = "FROM logs";
    private static final String TIME_FIELD = "ts";
    private static final String SUMMARY_COUNT_FIELD = "doc_count";
    private static final String JOB_ID = "test-job";
    private static final String DATE = "date";
    private static final String DATE_NANOS = "date_nanos";
    private static final String LONG = "long";
    private static final String KEYWORD = "keyword";

    private Client client;
    private DatafeedTimingStatsReporter timingStatsReporter;

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(mock(ThreadPool.class));
        when(client.threadPool().getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        timingStatsReporter = mock(DatafeedTimingStatsReporter.class);
    }

    public void testNextGivenSortIsInjected() throws IOException {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE), column("value", LONG)), "2000-01-01T00:00:00.100Z", 42L);

        assertThat(extractor.hasNext(), is(true));
        extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(DEFAULT_QUERY + " | SORT `timestamp` ASC"));
    }

    public void testNextGivenSortIsInjectedEvenWhenUserQueryAlreadyHasSort() throws IOException {
        String esqlQuery = "FROM logs | SORT value DESC";
        TestDataExtractor extractor = createExtractor(1000L, 2000L, esqlQuery, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE), column("value", LONG)), "2000-01-01T00:00:00.100Z", 1L);

        extractor.next();

        assertThat(extractor.capturedOrderedQuery, equalTo(esqlQuery + " | SORT `timestamp` ASC"));
    }

    public void testNextGivenTimeFilterIsHalfOpen() throws IOException {
        TestDataExtractor extractor = createExtractor(1000L, 2000L, DEFAULT_QUERY, "timestamp");
        extractor.enqueueRow(List.of(column("timestamp", DATE)), "2000-01-01T00:00:01.000Z");

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
        String isoDate = "2000-01-01T00:00:00.500Z";
        long expectedMillis = epochMillis(isoDate);

        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), isoDate);

        assertThat(asString(extractor.next().data().get()), equalTo("{\"" + TIME_FIELD + "\":" + expectedMillis + "}"));
    }

    public void testNextGivenDateNanosColumnConvertedToEpochMillis() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        String isoDate = "2000-01-01T00:00:00.123456789Z";
        long expectedMillis = epochMillis(isoDate);

        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE_NANOS)), isoDate);

        assertThat(asString(extractor.next().data().get()), equalTo("{\"" + TIME_FIELD + "\":" + expectedMillis + "}"));
    }

    public void testNextGivenMultiValuedDateColumnConvertedElementWise() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        String iso1 = "2000-01-01T00:00:00.100Z";
        String iso2 = "2000-01-01T00:00:00.200Z";
        long millis1 = epochMillis(iso1);
        long millis2 = epochMillis(iso2);

        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), List.of(iso1, iso2));

        assertThat(asString(extractor.next().data().get()), equalTo("{\"" + TIME_FIELD + "\":[" + millis1 + "," + millis2 + "]}"));
    }

    public void testNextGivenMultiValuedNonDateColumnPassedThrough() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE), column("tags", KEYWORD)), "2000-01-01T00:00:00.100Z", List.of("a", "b"));

        assertThat(asString(extractor.next().data().get()), containsString("\"tags\":[\"a\",\"b\"]"));
    }

    public void testNextGivenNullFieldIsOmittedFromRow() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRows(
            List.of(column(TIME_FIELD, DATE), column("optional_field", KEYWORD)),
            List.of(Arrays.asList("2000-01-01T00:00:00.100Z", null))
        );

        String ndjson = asString(extractor.next().data().get());
        assertThat(ndjson, containsString("\"" + TIME_FIELD + "\":"));
        assertThat(ndjson, not(containsString("optional_field")));
    }

    public void testNextGivenMissingTimeFieldThrowsIllegalArgument() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD);
        extractor.enqueueRow(List.of(column("other_field", KEYWORD)), "some-value");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString("ESQL query response is missing the required columns: " + TIME_FIELD));
    }

    public void testNextGivenMissingSummaryCountFieldThrowsIllegalArgument() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD, SUMMARY_COUNT_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE)), "2000-01-01T00:00:00.000Z");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString("ESQL query response is missing the required columns: " + SUMMARY_COUNT_FIELD));
    }

    public void testNextGivenSummaryCountFieldPresentSucceeds() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 10000L, DEFAULT_QUERY, TIME_FIELD, SUMMARY_COUNT_FIELD);
        extractor.enqueueRow(List.of(column(TIME_FIELD, DATE), column(SUMMARY_COUNT_FIELD, LONG)), "2000-01-01T00:00:00.000Z", 5L);

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
    }

    public void testDestroyAlsoCancels() throws IOException {
        TestDataExtractor extractor = createExtractor(0L, 1000L, DEFAULT_QUERY, TIME_FIELD);

        extractor.destroy();
        assertThat(extractor.isCancelled(), is(true));

        DataExtractor.Result result = extractor.next();
        assertThat(result.data().isPresent(), is(false));
        assertThat(extractor.capturedOrderedQuery, equalTo(null));
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
            equalTo(
                DEFAULT_QUERY
                    + " | STATS earliest_time = MIN(`"
                    + TIME_FIELD
                    + "`), latest_time = MAX(`"
                    + TIME_FIELD
                    + "`), total_hits = COUNT(*)"
            )
        );
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

    private TestDataExtractor createExtractor(long start, long end, String esqlQuery, String timeField) {
        return createExtractor(start, end, esqlQuery, timeField, null);
    }

    private TestDataExtractor createExtractor(long start, long end, String esqlQuery, String timeField, String requiredSummaryCountField) {
        EsqlDataExtractorContext context = new EsqlDataExtractorContext(
            JOB_ID,
            esqlQuery,
            timeField,
            start,
            end,
            Collections.emptyMap(),
            requiredSummaryCountField
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

        private final Queue<EsqlQueryResponse> responses = new ArrayDeque<>();
        String capturedOrderedQuery;
        QueryBuilder capturedTimeFilter;

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

        @Override
        protected EsqlQueryResponse runEsqlQuery(String orderedQuery, QueryBuilder timeFilter) {
            capturedOrderedQuery = orderedQuery;
            capturedTimeFilter = timeFilter;
            return responses.poll();
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
