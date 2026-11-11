/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDelayedDataDetector.ML_BUCKET_COLUMN;
import static org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDelayedDataDetector.ML_COUNT_COLUMN;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsqlDelayedDataDetectorTests extends ESTestCase {

    private static final long BUCKET_SPAN_MS = TimeValue.timeValueMinutes(1).millis();
    private static final long WINDOW_MS = TimeValue.timeValueMinutes(10).millis();
    private static final long BUCKET_TWO_MIN_MS = TimeValue.timeValueMinutes(2).millis();
    private static final long BUCKET_THREE_MIN_MS = TimeValue.timeValueMinutes(3).millis();
    private static final String JOB_ID = "test-job";
    private static final String TIME_FIELD = "ts";
    private static final String COUNT_FIELD = "event_count";
    private static final String SOURCE_TIME_FIELD = "@timestamp";
    private static final long LATEST_MS = BUCKET_SPAN_MS + WINDOW_MS;
    private static final long START_MS = LATEST_MS - WINDOW_MS;

    private Client client;

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    public void testBucketEventCountQueryShouldAppendSuffixAfterTrailingComment() {
        String userQuery = "FROM logs // trailing";
        assertThat(EsqlDelayedDataDetector.bucketEventCountQuery(userQuery), equalTo(userQuery + expectedSuffix()));
    }

    public void testWindowSmallerThanAlignmentShouldReturnEmptyWithoutQuerying() {
        EsqlDelayedDataDetector detector = new EsqlDelayedDataDetector(
            BUCKET_SPAN_MS,
            BUCKET_SPAN_MS / 2,
            JOB_ID,
            TIME_FIELD,
            COUNT_FIELD,
            "FROM index1",
            Map.of(),
            null,
            SOURCE_TIME_FIELD,
            client,
            (query, filter, params) -> {
                throw new AssertionError("delayed-data check must not run when the window is empty");
            }
        );
        assertThat(detector.detectMissingData(BUCKET_SPAN_MS / 2), equalTo(List.of()));
    }

    public void testUnchangedOrRemovedDocsShouldNotBeReported() {
        Bucket unchanged = newBucket(BUCKET_TWO_MIN_MS, 5L);
        Bucket withMissing = newBucket(BUCKET_THREE_MIN_MS, 3L);
        Bucket dataRemoved = newBucket(BUCKET_FOUR_MIN_MS(), 5L);
        stubBuckets(List.of(unchanged, withMissing, dataRemoved));
        Map<Long, Long> counts = Map.of(BUCKET_TWO_MIN_MS, 5L, BUCKET_THREE_MIN_MS, 7L, BUCKET_FOUR_MIN_MS(), 3L);

        List<BucketWithMissingData> missing = newDetector(counts).detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getBucket(), equalTo(withMissing));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(4L));
    }

    public void testNullBucketKeyShouldBeSkipped() {
        Bucket target = newBucket(BUCKET_TWO_MIN_MS, 4L);
        stubBuckets(List.of(target));

        AtomicReference<String> capturedQuery = new AtomicReference<>();
        EsqlDelayedDataDetector detector = newDetector((query, filter, params) -> {
            capturedQuery.set(query);
            return responseWithRows(List.of(countsRow(null, 13L), countsRow(bucketIndex(BUCKET_TWO_MIN_MS), 6L)));
        });

        List<BucketWithMissingData> missing = detector.detectMissingData(LATEST_MS);
        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L));
        assertThat(capturedQuery.get(), equalTo("FROM index1" + expectedSuffix()));
    }

    public void testQueryRunnerShouldUseSourceTimeFilterAndParams() {
        Bucket target = newBucket(BUCKET_TWO_MIN_MS, 1L);
        stubBuckets(List.of(target));
        AtomicReference<QueryBuilder> filter = new AtomicReference<>();
        AtomicReference<List<EsqlQueryParam>> params = new AtomicReference<>();

        EsqlDelayedDataDetector detector = newDetector((query, timeFilter, queryParams) -> {
            filter.set(timeFilter);
            params.set(queryParams);
            return responseWithRows(List.of(countsRow(bucketIndex(BUCKET_TWO_MIN_MS), 1L)));
        });
        detector.detectMissingData(LATEST_MS);

        assertThat(filter.get(), instanceOf(RangeQueryBuilder.class));
        RangeQueryBuilder range = (RangeQueryBuilder) filter.get();
        assertThat(range.fieldName(), equalTo(SOURCE_TIME_FIELD));
        assertThat(range.from(), equalTo(START_MS));
        assertThat(range.to(), equalTo(LATEST_MS));
        assertThat(params.get(), hasSize(3));
    }

    private static long BUCKET_FOUR_MIN_MS() {
        return TimeValue.timeValueMinutes(4).millis();
    }

    private static String expectedSuffix() {
        return EsqlDelayedDataDetector.bucketEventCountQuery("");
    }

    private EsqlDelayedDataDetector newDetector(Map<Long, Long> counts) {
        return newDetector(
            (query, filter, params) -> responseWithRows(
                counts.entrySet().stream().map(e -> countsRow(bucketIndex(e.getKey()), e.getValue())).toList()
            )
        );
    }

    private EsqlDelayedDataDetector newDetector(EsqlSyncQueryRunner queryRunner) {
        return new EsqlDelayedDataDetector(
            BUCKET_SPAN_MS,
            WINDOW_MS,
            JOB_ID,
            TIME_FIELD,
            COUNT_FIELD,
            "FROM index1",
            Map.of(),
            null,
            SOURCE_TIME_FIELD,
            client,
            queryRunner
        );
    }

    private static long bucketIndex(long bucketStartMs) {
        return bucketStartMs / BUCKET_SPAN_MS;
    }

    private static List<Object> countsRow(Long bucketIndex, long count) {
        List<Object> row = new ArrayList<>(2);
        row.add(count);
        row.add(bucketIndex);
        return row;
    }

    private static Bucket newBucket(long epochMs, long eventCount) {
        Bucket bucket = new Bucket(JOB_ID, new Date(epochMs), BUCKET_SPAN_MS / 1000);
        bucket.setEventCount(eventCount);
        return bucket;
    }

    @SuppressWarnings("unchecked")
    private void stubBuckets(List<Bucket> buckets) {
        QueryPage<Bucket> page = new QueryPage<>(buckets, buckets.size(), Bucket.RESULTS_FIELD);
        ActionFuture<GetBucketsAction.Response> future = mock(ActionFuture.class);
        when(future.actionGet()).thenReturn(new GetBucketsAction.Response(page));
        when(client.execute(eq(GetBucketsAction.INSTANCE), any(GetBucketsAction.Request.class))).thenReturn(future);
    }

    @SuppressWarnings("unchecked")
    private static EsqlQueryResponse responseWithRows(List<List<Object>> rows) {
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
