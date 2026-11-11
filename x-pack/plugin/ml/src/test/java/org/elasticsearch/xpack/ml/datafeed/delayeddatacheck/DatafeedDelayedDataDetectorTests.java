/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.junit.After;
import org.junit.Before;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class DatafeedDelayedDataDetectorTests extends ESTestCase {

    private static final long BUCKET_SPAN_MS = 60_000L;
    private static final long WINDOW_MS = 600_000L;
    private static final long LATEST_FINALIZED_BUCKET_MS = 3_600_000L;

    private ThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void terminateThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testDetectMissingDataWhenSearchResponseHasNoAggregations() {
        // A search that has no shard results to reduce returns a response with null aggregations, even though aggregations were requested
        SearchResponse searchResponse = SearchResponseUtils.successfulResponse(SearchHits.EMPTY_WITH_TOTAL_HITS);
        assertNull(searchResponse.getAggregations());

        try {
            assertThat(detectorWithSearchResponse(searchResponse).detectMissingData(LATEST_FINALIZED_BUCKET_MS), empty());
        } finally {
            searchResponse.decRef();
        }
    }

    public void testDetectMissingDataWhenSearchResponseHasNoDateBuckets() {
        SearchResponse searchResponse = SearchResponseUtils.response(SearchHits.EMPTY_WITH_TOTAL_HITS)
            .aggregations(InternalAggregations.EMPTY)
            .build();

        try {
            assertThat(detectorWithSearchResponse(searchResponse).detectMissingData(LATEST_FINALIZED_BUCKET_MS), empty());
        } finally {
            searchResponse.decRef();
        }
    }

    public void testBucketsWithUnchangedOrRemovedDocsShouldNotBeReported() {
        Bucket unchanged = newBucket(LATEST_FINALIZED_BUCKET_MS - 3 * BUCKET_SPAN_MS, 5L);
        Bucket withMissing = newBucket(LATEST_FINALIZED_BUCKET_MS - 2 * BUCKET_SPAN_MS, 3L);
        Bucket dataRemoved = newBucket(LATEST_FINALIZED_BUCKET_MS - BUCKET_SPAN_MS, 5L);
        Map<Long, Long> currentCounts = Map.of(
            unchanged.getTimestamp().getTime(),
            5L,
            withMissing.getTimestamp().getTime(),
            7L,
            dataRemoved.getTimestamp().getTime(),
            3L
        );
        DatafeedDelayedDataDetector detector = new DatafeedDelayedDataDetector(
            BUCKET_SPAN_MS,
            WINDOW_MS,
            "job",
            clientRespondingWith(List.of(unchanged, withMissing, dataRemoved), null),
            (start, end) -> currentCounts
        );

        List<BucketWithMissingData> missing = detector.detectMissingData(LATEST_FINALIZED_BUCKET_MS);

        assertThat(missing, equalTo(List.of(BucketWithMissingData.fromMissingAndBucket(4L, withMissing))));
    }

    public void testWindowSmallerThanOneBucketShouldNotCountEvents() {
        DatafeedDelayedDataDetector detector = new DatafeedDelayedDataDetector(
            BUCKET_SPAN_MS,
            BUCKET_SPAN_MS / 2,
            "job",
            clientRespondingWith(List.of(), null),
            (start, end) -> {
                throw new AssertionError("events must not be counted when the window is empty");
            }
        );

        assertThat(detector.detectMissingData(LATEST_FINALIZED_BUCKET_MS + 3 * BUCKET_SPAN_MS / 4), empty());
    }

    private DatafeedDelayedDataDetector detectorWithSearchResponse(SearchResponse searchResponse) {
        Client client = clientRespondingWith(List.of(), searchResponse);
        SearchBucketEventCounter counter = new SearchBucketEventCounter(
            "job",
            BUCKET_SPAN_MS,
            "time",
            QueryBuilders.matchAllQuery(),
            new String[] { "index" },
            IndicesOptions.lenientExpandOpen(),
            Map.of(),
            client
        );
        return new DatafeedDelayedDataDetector(BUCKET_SPAN_MS, WINDOW_MS, "job", client, counter);
    }

    private static Bucket newBucket(long epochMs, long eventCount) {
        Bucket bucket = new Bucket("job", new Date(epochMs), BUCKET_SPAN_MS / 1000);
        bucket.setEventCount(eventCount);
        return bucket;
    }

    /**
     * A client that returns the given finalized buckets for the job and the given response for the search over the datafeed indices.
     */
    private Client clientRespondingWith(List<Bucket> finalizedBuckets, SearchResponse searchResponse) {
        return new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action == GetBucketsAction.INSTANCE) {
                    listener.onResponse(
                        (Response) new GetBucketsAction.Response(
                            new QueryPage<>(finalizedBuckets, finalizedBuckets.size(), Bucket.RESULTS_FIELD)
                        )
                    );
                } else if (action == TransportSearchAction.TYPE && searchResponse != null) {
                    listener.onResponse((Response) searchResponse);
                } else {
                    throw new AssertionError("unexpected action [" + action.name() + "]");
                }
            }
        };
    }
}
