/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.PartialSearchResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.tasks.TaskId.EMPTY_TASK_ID;

/**
 * Task that tracks the progress of a currently running {@link SearchRequest}.
 */
class AsyncSearchTask extends SearchTask {
    private final AsyncSearchId searchId;
    private final Supplier<InternalAggregation.ReduceContext> reduceContextSupplier;
    private final Listener progressListener;

    private final Map<String, String> originHeaders;

    // a latch that notifies when the first response is available
    private final CountDownLatch initLatch = new CountDownLatch(1);
    // a latch that notifies the completion (or failure) of the request
    private final CountDownLatch completionLatch = new CountDownLatch(1);

    // the current response, updated last in mutually exclusive methods below (onListShards, onReduce,
    // onPartialReduce, onResponse and onFailure)
    private volatile AsyncSearchResponse response;

    // set once in onListShards (when the search starts)
    private volatile int totalShards = -1;
    private final AtomicInteger version = new AtomicInteger(0);
    private final AtomicInteger shardFailures = new AtomicInteger(0);

    /**
     * Creates an instance of {@link AsyncSearchTask}.
     *
     * @param id The id of the task.
     * @param type The type of the task.
     * @param action The action name.
     * @param originHeaders All the request context headers.
     * @param taskHeaders The filtered request headers for the task.
     * @param searchId The {@link AsyncSearchId} of the task.
     * @param reduceContextSupplier A supplier to create final reduce contexts.
     */
    AsyncSearchTask(long id,
                    String type,
                    String action,
                    Map<String, String> originHeaders,
                    Map<String, String> taskHeaders,
                    AsyncSearchId searchId,
                    Supplier<InternalAggregation.ReduceContext> reduceContextSupplier) {
        super(id, type, action, "async_search", EMPTY_TASK_ID, taskHeaders);
        this.originHeaders = originHeaders;
        this.searchId = searchId;
        this.reduceContextSupplier = reduceContextSupplier;
        this.progressListener = new Listener();
        setProgressListener(progressListener);
    }

    /**
     * Returns all of the request contexts headers
     */
    Map<String, String> getOriginHeaders() {
        return originHeaders;
    }

    /**
     * Returns the {@link AsyncSearchId} of the task
     */
    AsyncSearchId getSearchId() {
        return searchId;
    }

    @Override
    public SearchProgressActionListener getProgressListener() {
        return (Listener) super.getProgressListener();
    }

    /**
     * Waits up to the provided <code>waitForCompletionMillis</code> for the task completion and then returns a not-modified
     * response if the provided version is less than or equals to the current version, and the full response otherwise.
     *
     * Consumers should fork in a different thread to avoid blocking a network thread.
     */
    AsyncSearchResponse getAsyncResponse(long waitForCompletionMillis, int minimumVersion) throws InterruptedException {
        if (waitForCompletionMillis > 0) {
            completionLatch.await(waitForCompletionMillis, TimeUnit.MILLISECONDS);
        }
        initLatch.await();
        assert response != null;

        AsyncSearchResponse resp = response;
        // return a not-modified response
        AsyncSearchResponse newResp = resp.getVersion() > minimumVersion ? createFinalResponse(resp) :
            new AsyncSearchResponse(resp.getId(), resp.getVersion(), resp.isRunning()); // not-modified response
        newResp.setTaskInfo(taskInfo(searchId.getTaskId().getNodeId(), false));
        return newResp;
    }

    private AsyncSearchResponse createFinalResponse(AsyncSearchResponse resp) {
        PartialSearchResponse partialResp = resp.getPartialResponse();
        if (partialResp != null) {
            InternalAggregations newAggs = resp.getPartialResponse().getAggregations();
            if (partialResp.isFinalReduce() == false && newAggs != null) {
                newAggs = InternalAggregations.topLevelReduce(Collections.singletonList(newAggs), reduceContextSupplier.get());
            }
            partialResp = new PartialSearchResponse(partialResp.getTotalShards(), partialResp.getSuccessfulShards(),
                shardFailures.get(), // update shard failures
                partialResp.getTotalHits(),
                newAggs, true // update aggs
            );
        }
        AsyncSearchResponse newResp = new AsyncSearchResponse(resp.getId(),
            partialResp, // update partial response,
            resp.getSearchResponse(), resp.getFailure(), resp.getVersion(), resp.isRunning());
        return newResp;
    }

    private class Listener extends SearchProgressActionListener {
        @Override
        public void onListShards(List<SearchShard> shards, boolean fetchPhase) {
            try {
                totalShards = shards.size();
                response = new AsyncSearchResponse(searchId.getEncoded(),
                    new PartialSearchResponse(totalShards), version.incrementAndGet(), true);
            } finally {
                initLatch.countDown();
            }
        }

        @Override
        public void onQueryFailure(int shardIndex, Exception exc) {
            shardFailures.incrementAndGet();
        }

        @Override
        public void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
            response = new AsyncSearchResponse(searchId.getEncoded(),
                new PartialSearchResponse(totalShards, shards.size(), shardFailures.get(), totalHits, aggs, false),
                version.incrementAndGet(),
                true
            );
        }

        @Override
        public void onReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs) {
            response = new AsyncSearchResponse(searchId.getEncoded(),
                new PartialSearchResponse(totalShards, shards.size(), shardFailures.get(), totalHits, aggs, true),
                version.incrementAndGet(),
                true
            );
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            try {
                response = new AsyncSearchResponse(searchId.getEncoded(),
                    searchResponse, version.incrementAndGet(), false);
            } finally {
                completionLatch.countDown();
            }
        }

        @Override
        public void onFailure(Exception exc) {
            try {
                response = new AsyncSearchResponse(searchId.getEncoded(), response != null ? response.getPartialResponse() : null,
                    exc != null ? ElasticsearchException.guessRootCauses(exc)[0] : null, version.incrementAndGet(), false);
            } finally {
                if (initLatch.getCount() == 1) {
                    // the failure happened before the initialization of the query phase
                    initLatch.countDown();
                }
                completionLatch.countDown();
            }
        }
    }
}
