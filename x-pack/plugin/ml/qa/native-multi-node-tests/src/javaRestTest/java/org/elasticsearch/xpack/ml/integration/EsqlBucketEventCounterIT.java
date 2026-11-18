/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlBucketEventCounter;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/** Runs the delayed-data bucket count query on a real cluster, so the generated ES|QL is parsed, analyzed and executed. */
public class EsqlBucketEventCounterIT extends MlNativeAutodetectIntegTestCase {

    private static final String INDEX = "esql_bucket_event_counter";
    private static final String TIME_FIELD = "time";
    private static final String COUNT_FIELD = "event_count";
    private static final long BUCKET_SPAN_MS = TimeValue.timeValueMinutes(1).millis();

    private long firstBucketMs;

    @Before
    public void putDataIntoIndex() {
        client().admin().indices().prepareCreate(INDEX).setMapping(TIME_FIELD, "type=date", COUNT_FIELD, "type=long").get();
        long now = System.currentTimeMillis();
        firstBucketMs = now - now % BUCKET_SPAN_MS - 10 * BUCKET_SPAN_MS;
        // Two documents in the first bucket, one in the third. The last one lies outside of the counted window.
        BulkRequestBuilder bulk = client().prepareBulk();
        bulk.add(document(firstBucketMs + 1_000, 3));
        bulk.add(document(firstBucketMs + 30_000, 4));
        bulk.add(document(firstBucketMs + 2 * BUCKET_SPAN_MS + 5_000, 5));
        bulk.add(document(firstBucketMs + 5 * BUCKET_SPAN_MS, 100));
        BulkResponse response = bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
    }

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    public void testCountsShouldBeSummedPerBucketWithinTheWindow() {
        Map<Long, Long> counts = counter("FROM " + INDEX).currentBucketEventCounts(firstBucketMs, firstBucketMs + 3 * BUCKET_SPAN_MS);

        assertThat(counts, equalTo(Map.of(firstBucketMs, 7L, firstBucketMs + 2 * BUCKET_SPAN_MS, 5L)));
    }

    public void testQueryEndingInLineCommentShouldStillBeCounted() {
        Map<Long, Long> counts = counter("FROM " + INDEX + " // trailing comment").currentBucketEventCounts(
            firstBucketMs,
            firstBucketMs + 3 * BUCKET_SPAN_MS
        );

        assertThat(counts, equalTo(Map.of(firstBucketMs, 7L, firstBucketMs + 2 * BUCKET_SPAN_MS, 5L)));
    }

    public void testWindowWithoutDocumentsShouldYieldNoCounts() {
        Map<Long, Long> counts = counter("FROM " + INDEX).currentBucketEventCounts(
            firstBucketMs + 3 * BUCKET_SPAN_MS,
            firstBucketMs + 4 * BUCKET_SPAN_MS
        );

        assertThat(counts, equalTo(Map.of()));
    }

    private static IndexRequest document(long timestamp, long count) {
        return new IndexRequest(INDEX).source(TIME_FIELD, timestamp, COUNT_FIELD, count);
    }

    private EsqlBucketEventCounter counter(String esqlQuery) {
        return new EsqlBucketEventCounter(
            "esql-bucket-event-counter-job",
            BUCKET_SPAN_MS,
            TIME_FIELD,
            COUNT_FIELD,
            esqlQuery,
            TIME_FIELD,
            Map.of(),
            null,
            client()
        );
    }
}
