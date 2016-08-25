/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.Retry;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.action.bulk.BackoffPolicy.exponentialBackoff;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;
import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.SIZE_ALL_MATCHES;
import static org.elasticsearch.rest.RestStatus.CONFLICT;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

/**
 * Abstract base for scrolling across a search and executing bulk actions on all results. All package private methods are package private so
 * their tests can use them. Most methods run in the listener thread pool because the are meant to be fast and don't expect to block.
 */
public abstract class AbstractAsyncBulkByScrollAction<Request extends AbstractBulkByScrollRequest<Request>> {
    protected final ESLogger logger;
    protected final BulkByScrollTask task;
    protected final ThreadPool threadPool;
    /**
     * The request for this action. Named mainRequest because we create lots of <code>request</code> variables all representing child
     * requests of this mainRequest.
     */
    protected final Request mainRequest;

    private final AtomicLong startTime = new AtomicLong(-1);
    private final Set<String> destinationIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ParentTaskAssigningClient client;
    private final ActionListener<BulkIndexByScrollResponse> listener;
    private final Retry bulkRetry;
    private final ScrollableHitSource scrollSource;

    public AbstractAsyncBulkByScrollAction(BulkByScrollTask task, ESLogger logger, ParentTaskAssigningClient client,
                                           ThreadPool threadPool, Request mainRequest, ActionListener<BulkIndexByScrollResponse> listener) {
        this.task = task;
        this.logger = logger;
        this.client = client;
        this.threadPool = threadPool;
        this.mainRequest = mainRequest;
        this.listener = listener;
        BackoffPolicy backoffPolicy = buildBackoffPolicy();
        bulkRetry = Retry.on(EsRejectedExecutionException.class).policy(BackoffPolicy.wrap(backoffPolicy, task::countBulkRetry));
        scrollSource = buildScrollableResultSource(backoffPolicy);
        /*
         * Default to sorting by doc. We can't do this in the request itself because it is normal to *add* to the sorts rather than replace
         * them and if we add _doc as the first sort by default then sorts will never work.... So we add it here, only if there isn't
         * another sort.
         */
        List<SortBuilder<?>> sorts = mainRequest.getSearchRequest().source().sorts();
        if (sorts == null || sorts.isEmpty()) {
            mainRequest.getSearchRequest().source().sort(fieldSort("_doc"));
        }
        mainRequest.getSearchRequest().source().version(needsSourceDocumentVersions());
    }

    /**
     * Does this operation need the versions of the source documents?
     */
    protected abstract boolean needsSourceDocumentVersions();

    protected abstract BulkRequest buildBulk(Iterable<? extends ScrollableHitSource.Hit> docs);

    protected ScrollableHitSource buildScrollableResultSource(BackoffPolicy backoffPolicy) {
        return new ClientScrollableHitSource(logger, backoffPolicy, threadPool, task::countSearchRetry, this::finishHim, client,
                mainRequest.getSearchRequest());
    }

    /**
     * Build the response for reindex actions.
     */
    protected BulkIndexByScrollResponse buildResponse(TimeValue took, List<BulkItemResponse.Failure> indexingFailures,
                                                      List<SearchFailure> searchFailures, boolean timedOut) {
        return new BulkIndexByScrollResponse(took, task.getStatus(), indexingFailures, searchFailures, timedOut);
    }

    /**
     * Start the action by firing the initial search request.
     */
    public void start() {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        try {
            startTime.set(System.nanoTime());
            scrollSource.start(response -> onScrollResponse(timeValueNanos(System.nanoTime()), 0, response));
        } catch (Exception e) {
            finishHim(e);
        }
    }

    /**
     * Process a scroll response.
     * @param lastBatchStartTime the time when the last batch started. Used to calculate the throttling delay.
     * @param lastBatchSize the size of the last batch. Used to calculate the throttling delay.
     * @param response the scroll response to process
     */
    void onScrollResponse(TimeValue lastBatchStartTime, int lastBatchSize, ScrollableHitSource.Response response) {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        if (    // If any of the shards failed that should abort the request.
                (response.getFailures().size() > 0)
                // Timeouts aren't shard failures but we still need to pass them back to the user.
                || response.isTimedOut()
                ) {
            refreshAndFinish(emptyList(), response.getFailures(), response.isTimedOut());
            return;
        }
        long total = response.getTotalHits();
        if (mainRequest.getSize() > 0) {
            total = min(total, mainRequest.getSize());
        }
        task.setTotal(total);
        AbstractRunnable prepareBulkRequestRunnable = new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                /*
                 * It is important that the batch start time be calculated from here, scroll response to scroll response. That way the time
                 * waiting on the scroll doesn't count against this batch in the throttle.
                 */
                prepareBulkRequest(timeValueNanos(System.nanoTime()), response);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        };
        prepareBulkRequestRunnable = (AbstractRunnable) threadPool.getThreadContext().preserveContext(prepareBulkRequestRunnable);
        task.delayPrepareBulkRequest(threadPool, lastBatchStartTime, lastBatchSize, prepareBulkRequestRunnable);
    }

    /**
     * Prepare the bulk request. Called on the generic thread pool after some preflight checks have been done one the SearchResponse and any
     * delay has been slept. Uses the generic thread pool because reindex is rare enough not to need its own thread pool and because the
     * thread may be blocked by the user script.
     */
    void prepareBulkRequest(TimeValue thisBatchStartTime, ScrollableHitSource.Response response) {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        if (response.getHits().isEmpty()) {
            refreshAndFinish(emptyList(), emptyList(), false);
            return;
        }
        task.countBatch();
        List<? extends ScrollableHitSource.Hit> hits = response.getHits();
        if (mainRequest.getSize() != SIZE_ALL_MATCHES) {
            // Truncate the hits if we have more than the request size
            long remaining = max(0, mainRequest.getSize() - task.getSuccessfullyProcessed());
            if (remaining < hits.size()) {
                hits = hits.subList(0, (int) remaining);
            }
        }
        BulkRequest request = buildBulk(hits);
        if (request.requests().isEmpty()) {
            /*
             * If we noop-ed the entire batch then just skip to the next batch or the BulkRequest would fail validation.
             */
            startNextScroll(thisBatchStartTime, 0);
            return;
        }
        request.timeout(mainRequest.getTimeout());
        request.waitForActiveShards(mainRequest.getWaitForActiveShards());
        if (logger.isDebugEnabled()) {
            logger.debug("sending [{}] entry, [{}] bulk request", request.requests().size(),
                    new ByteSizeValue(request.estimatedSizeInBytes()));
        }
        sendBulkRequest(thisBatchStartTime, request);
    }

    /**
     * Send a bulk request, handling retries.
     */
    void sendBulkRequest(TimeValue thisBatchStartTime, BulkRequest request) {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        bulkRetry.withAsyncBackoff(client, request, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse response) {
                onBulkResponse(thisBatchStartTime, response);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        });
    }

    /**
     * Processes bulk responses, accounting for failures.
     */
    void onBulkResponse(TimeValue thisBatchStartTime, BulkResponse response) {
        try {
            List<Failure> failures = new ArrayList<Failure>();
            Set<String> destinationIndicesThisBatch = new HashSet<>();
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    recordFailure(item.getFailure(), failures);
                    continue;
                }

                switch (item.getOpType()) {
                case "index":
                case "create":
                    IndexResponse ir = item.getResponse();
                    if (ir.getResult() == DocWriteResponse.Result.CREATED) {
                        task.countCreated();
                    } else {
                        task.countUpdated();
                    }
                    break;
                case "delete":
                    task.countDeleted();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown op type:  " + item.getOpType());
                }
                // Track the indexes we've seen so we can refresh them if requested
                destinationIndicesThisBatch.add(item.getIndex());
            }

            if (task.isCancelled()) {
                finishHim(null);
                return;
            }

            addDestinationIndices(destinationIndicesThisBatch);

            if (false == failures.isEmpty()) {
                refreshAndFinish(unmodifiableList(failures), emptyList(), false);
                return;
            }

            if (mainRequest.getSize() != SIZE_ALL_MATCHES && task.getSuccessfullyProcessed() >= mainRequest.getSize()) {
                // We've processed all the requested docs.
                refreshAndFinish(emptyList(), emptyList(), false);
                return;
            }

            startNextScroll(thisBatchStartTime, response.getItems().length);
        } catch (Exception t) {
            finishHim(t);
        }
    }

    /**
     * Start the next scroll request.
     *
     * @param lastBatchSize the number of requests sent in the last batch. This is used to calculate the throttling values which are applied
     *        when the scroll returns
     */
    void startNextScroll(TimeValue lastBatchStartTime, int lastBatchSize) {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        TimeValue extraKeepAlive = task.throttleWaitTime(lastBatchStartTime, lastBatchSize);
        scrollSource.startNextScroll(extraKeepAlive, response -> {
            onScrollResponse(lastBatchStartTime, lastBatchSize, response);
        });
    }

    private void recordFailure(Failure failure, List<Failure> failures) {
        if (failure.getStatus() == CONFLICT) {
            task.countVersionConflict();
            if (false == mainRequest.isAbortOnVersionConflict()) {
                return;
            }
        }
        failures.add(failure);
    }

    /**
     * Start terminating a request that finished non-catastrophically by refreshing the modified indices and then proceeding to
     * {@link #finishHim(Exception, List, List, boolean)}.
     */
    void refreshAndFinish(List<Failure> indexingFailures, List<SearchFailure> searchFailures, boolean timedOut) {
        if (task.isCancelled() || false == mainRequest.isRefresh() || destinationIndices.isEmpty()) {
            finishHim(null, indexingFailures, searchFailures, timedOut);
            return;
        }
        RefreshRequest refresh = new RefreshRequest();
        refresh.indices(destinationIndices.toArray(new String[destinationIndices.size()]));
        client.admin().indices().refresh(refresh, new ActionListener<RefreshResponse>() {
            @Override
            public void onResponse(RefreshResponse response) {
                finishHim(null, indexingFailures, searchFailures, timedOut);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        });
    }

    /**
     * Finish the request.
     *
     * @param failure if non null then the request failed catastrophically with this exception
     */
    void finishHim(Exception failure) {
        finishHim(failure, emptyList(), emptyList(), false);
    }

    /**
     * Finish the request.
     * @param failure if non null then the request failed catastrophically with this exception
     * @param indexingFailures any indexing failures accumulated during the request
     * @param searchFailures any search failures accumulated during the request
     * @param timedOut have any of the sub-requests timed out?
     */
    void finishHim(Exception failure, List<Failure> indexingFailures, List<SearchFailure> searchFailures, boolean timedOut) {
        scrollSource.close();
        if (failure == null) {
            listener.onResponse(
                    buildResponse(timeValueNanos(System.nanoTime() - startTime.get()), indexingFailures, searchFailures, timedOut));
        } else {
            listener.onFailure(failure);
        }
    }

    /**
     * Get the backoff policy for use with retries.
     */
    BackoffPolicy buildBackoffPolicy() {
        return exponentialBackoff(mainRequest.getRetryBackoffInitialTime(), mainRequest.getMaxRetries());
    }

    /**
     * Add to the list of indices that were modified by this request. This is the list of indices refreshed at the end of the request if the
     * request asks for a refresh.
     */
    void addDestinationIndices(Collection<String> indices) {
        destinationIndices.addAll(indices);
    }

    /**
     * Set the last returned scrollId. Exists entirely for testing.
     */
    void setScroll(String scroll) {
        scrollSource.setScroll(scroll);
    }
}
