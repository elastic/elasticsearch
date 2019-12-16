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
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.PartialSearchResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    Map<String, String> getOriginHeaders() {
        return originHeaders;
    }

    AsyncSearchId getSearchId() {
        return searchId;
    }

    @Override
    public SearchProgressActionListener getProgressListener() {
        return (Listener) super.getProgressListener();
    }

    /**
     * Perform the final reduce on the current {@link AsyncSearchResponse} if requested
     * and return the result.
     */
    AsyncSearchResponse getAsyncResponse(boolean doFinalReduce) {
        return progressListener.response.get(doFinalReduce);
    }

    private class Listener extends SearchProgressActionListener {
        private int totalShards = -1;
        private AtomicInteger version = new AtomicInteger(0);
        private AtomicInteger shardFailures = new AtomicInteger(0);

        private int lastSuccess = 0;
        private int lastFailures = 0;

        private volatile Response response;

        Listener() {
            final AsyncSearchResponse initial = new AsyncSearchResponse(searchId.getEncoded(),
                new PartialSearchResponse(totalShards), version.get(), true);
            this.response = new Response(initial, false);
        }

        @Override
        public void onListShards(List<SearchShard> shards, boolean fetchPhase) {
            this.totalShards = shards.size();
            final AsyncSearchResponse newResp = new AsyncSearchResponse(searchId.getEncoded(),
                new PartialSearchResponse(totalShards), version.incrementAndGet(), true);
            response = new Response(newResp, false);
        }

        @Override
        public void onQueryFailure(int shardIndex, Exception exc) {
            shardFailures.incrementAndGet();
        }

        @Override
        public void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
            lastSuccess = shards.size();
            lastFailures = shardFailures.get();
            final AsyncSearchResponse newResp = new AsyncSearchResponse(searchId.getEncoded(),
                new PartialSearchResponse(totalShards, lastSuccess, lastFailures, totalHits, aggs),
                version.incrementAndGet(),
                true
            );
            response = new Response(newResp, aggs != null);
        }

        @Override
        public void onReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs) {
            int failures = shardFailures.get();
            int ver = (lastSuccess == shards.size() && lastFailures == failures) ? version.get() : version.incrementAndGet();
            final AsyncSearchResponse newResp = new AsyncSearchResponse(searchId.getEncoded(),
                new PartialSearchResponse(totalShards, shards.size(), failures, totalHits, aggs),
                ver,
                true
            );
            response = new Response(newResp, false);
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            AsyncSearchResponse newResp = new AsyncSearchResponse(searchId.getEncoded(),
                searchResponse, version.incrementAndGet(), false);
            response = new Response(newResp, false);
        }

        @Override
        public void onFailure(Exception exc) {
            AsyncSearchResponse previous = response.get(true);
            response = new Response(new AsyncSearchResponse(searchId.getEncoded(),
                newPartialResponse(previous, shardFailures.get()),
                exc != null ? new ElasticsearchException(exc) : null, version.incrementAndGet(), false), false);
        }

        private PartialSearchResponse newPartialResponse(AsyncSearchResponse response, int numFailures) {
            PartialSearchResponse old = response.getPartialResponse();
            return response.hasPartialResponse() ? new PartialSearchResponse(totalShards, old.getSuccessfulShards(), shardFailures.get(),
                old.getTotalHits(), old.getAggregations()) : null;
        }
    }

    private class Response {
        AsyncSearchResponse internal;
        boolean needFinalReduce;

        Response(AsyncSearchResponse response, boolean needFinalReduce) {
            this.internal = response;
            this.needFinalReduce = needFinalReduce;
        }

        /**
         * Ensure that we're performing the final reduce only when users explicitly requested
         * a response through a {@link GetAsyncSearchAction.Request}.
         */
        public synchronized AsyncSearchResponse get(boolean doFinalReduce) {
            if (doFinalReduce && needFinalReduce) {
                InternalAggregations reducedAggs = internal.getPartialResponse().getAggregations();
                reducedAggs = InternalAggregations.topLevelReduce(Collections.singletonList(reducedAggs), reduceContextSupplier.get());
                PartialSearchResponse old = internal.getPartialResponse();
                PartialSearchResponse clone = new PartialSearchResponse(old.getTotalShards(), old.getSuccessfulShards(),
                    old.getShardFailures(), old.getTotalHits(), reducedAggs);
                needFinalReduce = false;
                return internal = new AsyncSearchResponse(internal.id(), clone, internal.getFailure(), internal.getVersion(), true);
            } else {
                return internal;
            }
        }
    }
}
