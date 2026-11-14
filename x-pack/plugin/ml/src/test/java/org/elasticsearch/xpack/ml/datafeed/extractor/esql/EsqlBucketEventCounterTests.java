/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER;
import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.VALUE;
import static org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlBucketEventCounter.ML_BUCKET_COLUMN;
import static org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlBucketEventCounter.ML_COUNT_COLUMN;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsqlBucketEventCounterTests extends ESTestCase {

    private static final long BUCKET_SPAN_MS = TimeValue.timeValueMinutes(1).millis();
    private static final long START_MS = TimeValue.timeValueMinutes(1).millis();
    private static final long END_MS = TimeValue.timeValueMinutes(11).millis();
    private static final long BUCKET_TWO_MIN_MS = TimeValue.timeValueMinutes(2).millis();
    private static final long BUCKET_THREE_MIN_MS = TimeValue.timeValueMinutes(3).millis();
    private static final String TIME_FIELD = "ts";
    private static final String COUNT_FIELD = "event_count";
    private static final String SOURCE_TIME_FIELD = "@timestamp";
    private static final String USER_QUERY = "FROM index1";

    public void testUserQueryEndingInLineCommentShouldKeepSuffixOnNewLine() {
        String userQuery = "FROM logs // trailing";
        assertThat(EsqlBucketEventCounter.bucketEventCountQuery(userQuery), equalTo(userQuery + expectedSuffix()));
    }

    public void testGeneratedSuffixShouldStartOnNewLine() {
        assertThat(EsqlBucketEventCounter.bucketEventCountQuery("FROM logs"), equalTo("FROM logs" + expectedSuffix()));
        assertThat(expectedSuffix().startsWith("\n| STATS"), is(true));
    }

    public void testRowsShouldMapBucketIndexToBucketStartAndSkipNullKeys() {
        AtomicReference<String> capturedQuery = new AtomicReference<>();
        EsqlBucketEventCounter counter = newCounter((query, filter, params) -> {
            capturedQuery.set(query);
            return responseWithRows(
                List.of(
                    countsRow(null, 13L),
                    countsRow(bucketIndex(BUCKET_TWO_MIN_MS), 6L),
                    countsRow(bucketIndex(BUCKET_THREE_MIN_MS), 2L)
                )
            );
        });

        Map<Long, Long> counts = counter.currentBucketEventCounts(START_MS, END_MS);

        assertThat(counts, equalTo(Map.of(BUCKET_TWO_MIN_MS, 6L, BUCKET_THREE_MIN_MS, 2L)));
        assertThat(capturedQuery.get(), equalTo(USER_QUERY + expectedSuffix()));
    }

    public void testRowWithNullCountShouldBeSkipped() {
        EsqlBucketEventCounter counter = newCounter(
            (query, filter, params) -> responseWithRows(
                List.of(countsRow(bucketIndex(BUCKET_TWO_MIN_MS), null), countsRow(bucketIndex(BUCKET_THREE_MIN_MS), 2L))
            )
        );

        assertThat(counter.currentBucketEventCounts(START_MS, END_MS), equalTo(Map.of(BUCKET_THREE_MIN_MS, 2L)));
    }

    public void testQueryShouldUseSourceTimeFilterAndTypedParams() {
        AtomicReference<QueryBuilder> filter = new AtomicReference<>();
        AtomicReference<List<EsqlQueryParam>> params = new AtomicReference<>();
        EsqlBucketEventCounter counter = newCounter((query, timeFilter, queryParams) -> {
            filter.set(timeFilter);
            params.set(queryParams);
            return responseWithRows(List.of());
        });

        counter.currentBucketEventCounts(START_MS, END_MS);

        assertThat(filter.get(), instanceOf(RangeQueryBuilder.class));
        RangeQueryBuilder range = (RangeQueryBuilder) filter.get();
        assertThat(range.fieldName(), equalTo(SOURCE_TIME_FIELD));
        assertThat(range.from(), equalTo(START_MS));
        assertThat(range.to(), equalTo(END_MS));
        assertThat(
            params.get(),
            equalTo(
                List.of(
                    new EsqlQueryParam("countField", COUNT_FIELD, IDENTIFIER),
                    new EsqlQueryParam("timeField", TIME_FIELD, IDENTIFIER),
                    new EsqlQueryParam("bucketSpan", BUCKET_SPAN_MS, VALUE)
                )
            )
        );
    }

    public void testNodeFailureShouldBeRetriedOnce() {
        AtomicInteger calls = new AtomicInteger();
        EsqlBucketEventCounter counter = newCounter((query, filter, params) -> {
            if (calls.getAndIncrement() == 0) {
                throw new NodeDisconnectedException(null, "test");
            }
            return responseWithRows(List.<List<Object>>of(countsRow(bucketIndex(BUCKET_TWO_MIN_MS), 4L)));
        });

        assertThat(counter.currentBucketEventCounts(START_MS, END_MS), equalTo(Map.of(BUCKET_TWO_MIN_MS, 4L)));
        assertThat(calls.get(), equalTo(2));
    }

    public void testSecondNodeFailureShouldPropagate() {
        EsqlBucketEventCounter counter = newCounter((query, filter, params) -> { throw new NodeDisconnectedException(null, "test"); });

        expectThrows(NodeDisconnectedException.class, () -> counter.currentBucketEventCounts(START_MS, END_MS));
    }

    public void testResponseShouldBeClosedAfterParsing() {
        AtomicReference<TestEsqlQueryResponse> response = new AtomicReference<>();
        EsqlBucketEventCounter counter = newCounter((query, filter, params) -> {
            response.set(responseWithRows(List.of()));
            return response.get();
        });

        counter.currentBucketEventCounts(START_MS, END_MS);

        expectThrows(IllegalStateException.class, () -> response.get().response());
    }

    private static String expectedSuffix() {
        return EsqlBucketEventCounter.bucketEventCountQuery("");
    }

    private static EsqlBucketEventCounter newCounter(EsqlSyncQueryRunner queryRunner) {
        return new EsqlBucketEventCounter("test-job", BUCKET_SPAN_MS, TIME_FIELD, COUNT_FIELD, USER_QUERY, SOURCE_TIME_FIELD, queryRunner);
    }

    private static long bucketIndex(long bucketStartMs) {
        return bucketStartMs / BUCKET_SPAN_MS;
    }

    private static List<Object> countsRow(Long bucketIndex, Long count) {
        List<Object> row = new ArrayList<>(2);
        row.add(count);
        row.add(bucketIndex);
        return row;
    }

    @SuppressWarnings("unchecked")
    private static TestEsqlQueryResponse responseWithRows(List<List<Object>> rows) {
        List<? extends ColumnInfo> columns = List.of(mockColumn(ML_COUNT_COLUMN), mockColumn(ML_BUCKET_COLUMN));
        EsqlResponse esqlResponse = mock(EsqlResponse.class);
        doReturn(columns).when(esqlResponse).columns();
        when(esqlResponse.rows()).thenReturn((Iterable<Iterable<Object>>) (Iterable<?>) rows);
        return new TestEsqlQueryResponse(esqlResponse);
    }

    private static ColumnInfo mockColumn(String name) {
        ColumnInfo column = mock(ColumnInfo.class);
        when(column.name()).thenReturn(name);
        return column;
    }

    private static final class TestEsqlQueryResponse extends EsqlQueryResponse {

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
