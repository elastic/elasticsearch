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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.Retry;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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
    /**
     * The request for this action. Named mainRequest because we create lots of <code>request</code> variables all representing child
     * requests of this mainRequest.
     */
    protected final Request mainRequest;
    protected final BulkByScrollTask task;

    private final AtomicLong startTime = new AtomicLong(-1);
    private final AtomicReference<String> scroll = new AtomicReference<>();
    private final Set<String> destinationIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ESLogger logger;
    private final ParentTaskAssigningClient client;
    private final ThreadPool threadPool;
    private final SearchRequest firstSearchRequest;
    private final ActionListener<BulkIndexByScrollResponse> listener;
    private final BackoffPolicy backoffPolicy;
    private final Retry bulkRetry;

    public AbstractAsyncBulkByScrollAction(BulkByScrollTask task, ESLogger logger, ParentTaskAssigningClient client,
                                           ThreadPool threadPool, Request mainRequest, SearchRequest firstSearchRequest,
                                           ActionListener<BulkIndexByScrollResponse> listener) {
        this.task = task;
        this.logger = logger;
        this.client = client;
        this.threadPool = threadPool;
        this.mainRequest = mainRequest;
        this.firstSearchRequest = firstSearchRequest;
        this.listener = listener;
        backoffPolicy = buildBackoffPolicy();
        bulkRetry = Retry.on(EsRejectedExecutionException.class).policy(wrapBackoffPolicy(backoffPolicy));
    }

    protected abstract BulkRequest buildBulk(Iterable<SearchHit> docs);

    /**
     * Build the response for reindex actions.
     */
    protected BulkIndexByScrollResponse buildResponse(TimeValue took, List<BulkItemResponse.Failure> indexingFailures,
                                                      List<ShardSearchFailure> searchFailures, boolean timedOut) {
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
            // Default to sorting by _doc if it hasn't been changed.
            if (firstSearchRequest.source().sorts() == null) {
                firstSearchRequest.source().sort(fieldSort("_doc"));
            }
            startTime.set(System.nanoTime());
            if (logger.isDebugEnabled()) {
                logger.debug("executing initial scroll against {}{}",
                        firstSearchRequest.indices() == null || firstSearchRequest.indices().length == 0 ? "all indices"
                                : firstSearchRequest.indices(),
                        firstSearchRequest.types() == null || firstSearchRequest.types().length == 0 ? ""
                                : firstSearchRequest.types());
            }
        } catch (Throwable t) {
            finishHim(t);
            return;
        }
        searchWithRetry(listener -> client.search(firstSearchRequest, listener), (SearchResponse response) -> {
            logger.debug("[{}] documents match query", response.getHits().getTotalHits());
            onScrollResponse(timeValueNanos(System.nanoTime()), 0, response);
        });
    }

    /**
     * Process a scroll response.
     * @param lastBatchStartTime the time when the last batch started. Used to calculate the throttling delay.
     * @param lastBatchSize the size of the last batch. Used to calculate the throttling delay.
     * @param searchResponse the scroll response to process
     */
    void onScrollResponse(TimeValue lastBatchStartTime, int lastBatchSize, SearchResponse searchResponse) {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        setScroll(searchResponse.getScrollId());
        if (    // If any of the shards failed that should abort the request.
                (searchResponse.getShardFailures() != null && searchResponse.getShardFailures().length > 0)
                // Timeouts aren't shard failures but we still need to pass them back to the user.
                || searchResponse.isTimedOut()
                ) {
            startNormalTermination(emptyList(), unmodifiableList(Arrays.asList(searchResponse.getShardFailures())),
                    searchResponse.isTimedOut());
            return;
        }
        long total = searchResponse.getHits().totalHits();
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
                prepareBulkRequest(timeValueNanos(System.nanoTime()), searchResponse);
            }

            @Override
            public void onFailure(Throwable t) {
                finishHim(t);
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
    void prepareBulkRequest(TimeValue thisBatchStartTime, SearchResponse searchResponse) {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        SearchHit[] docs = searchResponse.getHits().getHits();
        logger.debug("scroll returned [{}] documents with a scroll id of [{}]", docs.length, searchResponse.getScrollId());
        if (docs.length == 0) {
            startNormalTermination(emptyList(), emptyList(), false);
            return;
        }
        task.countBatch();
        List<SearchHit> docsIterable = Arrays.asList(docs);
        if (mainRequest.getSize() != SIZE_ALL_MATCHES) {
            // Truncate the docs if we have more than the request size
            long remaining = max(0, mainRequest.getSize() - task.getSuccessfullyProcessed());
            if (remaining < docs.length) {
                docsIterable = docsIterable.subList(0, (int) remaining);
            }
        }
        BulkRequest request = buildBulk(docsIterable);
        if (request.requests().isEmpty()) {
            /*
             * If we noop-ed the entire batch then just skip to the next batch or the BulkRequest would fail validation.
             */
            startNextScroll(thisBatchStartTime, 0);
            return;
        }
        request.timeout(mainRequest.getTimeout());
        request.consistencyLevel(mainRequest.getConsistency());
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
            public void onFailure(Throwable e) {
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
                    if (ir.isCreated()) {
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
                startNormalTermination(unmodifiableList(failures), emptyList(), false);
                return;
            }

            if (mainRequest.getSize() != SIZE_ALL_MATCHES && task.getSuccessfullyProcessed() >= mainRequest.getSize()) {
                // We've processed all the requested docs.
                startNormalTermination(emptyList(), emptyList(), false);
                return;
            }

            startNextScroll(thisBatchStartTime, response.getItems().length);
        } catch (Throwable t) {
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
        SearchScrollRequest request = new SearchScrollRequest();
        // Add the wait time into the scroll timeout so it won't timeout while we wait for throttling
        request.scrollId(scroll.get()).scroll(timeValueNanos(
                firstSearchRequest.scroll().keepAlive().nanos() + task.throttleWaitTime(lastBatchStartTime, lastBatchSize).nanos()));
        searchWithRetry(listener -> client.searchScroll(request, listener), (SearchResponse response) -> {
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
     * Start terminating a request that finished non-catastrophically.
     */
    void startNormalTermination(List<Failure> indexingFailures, List<ShardSearchFailure> searchFailures, boolean timedOut) {
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
            public void onFailure(Throwable e) {
                finishHim(e);
            }
        });
    }

    /**
     * Finish the request.
     *
     * @param failure if non null then the request failed catastrophically with this exception
     */
    void finishHim(Throwable failure) {
        finishHim(failure, emptyList(), emptyList(), false);
    }

    /**
     * Finish the request.
     *
     * @param failure if non null then the request failed catastrophically with this exception
     * @param indexingFailures any indexing failures accumulated during the request
     * @param searchFailures any search failures accumulated during the request
     * @param timedOut have any of the sub-requests timed out?
     */
    void finishHim(Throwable failure, List<Failure> indexingFailures, List<ShardSearchFailure> searchFailures, boolean timedOut) {
        String scrollId = scroll.get();
        if (Strings.hasLength(scrollId)) {
            /*
             * Fire off the clear scroll but don't wait for it it return before
             * we send the use their response.
             */
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            /*
             * Unwrap the client so we don't set our task as the parent. If we *did* set our ID then the clear scroll would be cancelled as
             * if this task is cancelled. But we want to clear the scroll regardless of whether or not the main request was cancelled.
             */
            client.unwrap().clearScroll(clearScrollRequest, new ActionListener<ClearScrollResponse>() {
                @Override
                public void onResponse(ClearScrollResponse response) {
                    logger.debug("Freed [{}] contexts", response.getNumFreed());
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.warn("Failed to clear scroll [" + scrollId + ']', e);
                }
            });
        }
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
        this.scroll.set(scroll);
    }

    /**
     * Wraps a backoffPolicy in another policy that counts the number of backoffs acquired. Used to count bulk backoffs.
     */
    private BackoffPolicy wrapBackoffPolicy(BackoffPolicy backoffPolicy) {
        return new BackoffPolicy() {
            @Override
            public Iterator<TimeValue> iterator() {
                return new Iterator<TimeValue>() {
                    private final Iterator<TimeValue> delegate = backoffPolicy.iterator();
                    @Override
                    public boolean hasNext() {
                        return delegate.hasNext();
                    }

                    @Override
                    public TimeValue next() {
                        if (false == delegate.hasNext()) {
                            return null;
                        }
                        task.countBulkRetry();
                        return delegate.next();
                    }
                };
            }
        };
    }

    /**
     * Run a search action and call onResponse when a the response comes in, retrying if the action fails with an exception caused by
     * rejected execution.
     *
     * @param action consumes a listener and starts the action. The listener it consumes is rigged to retry on failure.
     * @param onResponse consumes the response from the action
     */
    private <T> void searchWithRetry(Consumer<ActionListener<T>> action, Consumer<T> onResponse) {
        class RetryHelper extends AbstractRunnable implements ActionListener<T> {
            private final Iterator<TimeValue> retries = backoffPolicy.iterator();

            @Override
            public void onResponse(T response) {
                onResponse.accept(response);
            }

            @Override
            protected void doRun() throws Exception {
                action.accept(this);
            }

            @Override
            public void onFailure(Throwable e) {
                if (ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class) != null) {
                    if (retries.hasNext()) {
                        TimeValue delay = retries.next();
                        logger.trace("retrying rejected search after [{}]", e, delay);
                        threadPool.schedule(delay, ThreadPool.Names.SAME, this);
                        task.countSearchRetry();
                    } else {
                        logger.warn("giving up on search because we retried {} times without success", e, retries);
                        finishHim(e);
                    }
                } else {
                    logger.warn("giving up on search because it failed with a non-retryable exception", e);
                    finishHim(e);
                }
            }
        }
        new RetryHelper().run();
    }
}
