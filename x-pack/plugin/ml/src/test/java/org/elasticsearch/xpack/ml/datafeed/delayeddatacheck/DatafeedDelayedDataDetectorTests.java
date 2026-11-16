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
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;

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
        SearchResponse searchResponse = searchResponse(null);
        assertNull(searchResponse.getAggregations());

        try {
            assertThat(detectorWithSearchResponse(searchResponse).detectMissingData(LATEST_FINALIZED_BUCKET_MS), empty());
        } finally {
            searchResponse.decRef();
        }
    }

    public void testDetectMissingDataWhenSearchResponseHasNoDateBuckets() {
        SearchResponse searchResponse = searchResponse(InternalAggregations.EMPTY);

        try {
            assertThat(detectorWithSearchResponse(searchResponse).detectMissingData(LATEST_FINALIZED_BUCKET_MS), empty());
        } finally {
            searchResponse.decRef();
        }
    }

    private DatafeedDelayedDataDetector detectorWithSearchResponse(SearchResponse searchResponse) {
        return new DatafeedDelayedDataDetector(
            BUCKET_SPAN_MS,
            WINDOW_MS,
            "job",
            "time",
            QueryBuilders.matchAllQuery(),
            new String[] { "index" },
            IndicesOptions.lenientExpandOpen(),
            Map.of(),
            clientRespondingWith(searchResponse)
        );
    }

    /**
     * A client that returns no finalized buckets for the job and the given response for the search over the datafeed indices.
     */
    private Client clientRespondingWith(SearchResponse searchResponse) {
        return new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action == GetBucketsAction.INSTANCE) {
                    listener.onResponse((Response) new GetBucketsAction.Response(new QueryPage<>(List.of(), 0, Bucket.RESULTS_FIELD)));
                } else if (action == TransportSearchAction.TYPE) {
                    listener.onResponse((Response) searchResponse);
                } else {
                    throw new AssertionError("unexpected action [" + action.name() + "]");
                }
            }
        };
    }

    private static SearchResponse searchResponse(InternalAggregations aggregations) {
        return new SearchResponse(
            SearchHits.EMPTY_WITH_TOTAL_HITS,
            aggregations,
            null,
            false,
            null,
            null,
            1,
            null,
            1,
            1,
            0,
            42L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }
}
