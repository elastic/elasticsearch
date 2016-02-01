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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.max;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.plugin.reindex.AbstractBulkByScrollRequest.SIZE_ALL_MATCHES;
import static org.elasticsearch.rest.RestStatus.CONFLICT;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

/**
 * Abstract base for scrolling across a search and executing bulk actions on all
 * results.
 */
public abstract class AbstractAsyncBulkByScrollAction<Request extends AbstractBulkByScrollRequest<Request>, Response> {
    protected final Request mainRequest;

    private final AtomicLong startTime = new AtomicLong(-1);
    private final AtomicLong updated = new AtomicLong(0);
    private final AtomicLong created = new AtomicLong(0);
    private final AtomicLong deleted = new AtomicLong(0);
    private final AtomicInteger batches = new AtomicInteger(0);
    private final AtomicLong versionConflicts = new AtomicLong(0);
    private final AtomicReference<String> scroll = new AtomicReference<>();
    private final List<Failure> indexingFailures = new CopyOnWriteArrayList<>();
    private final List<ShardSearchFailure> searchFailures = new CopyOnWriteArrayList<>();
    private final Set<String> destinationIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ESLogger logger;
    private final Client client;
    private final ThreadPool threadPool;
    private final SearchRequest firstSearchRequest;
    private final ActionListener<Response> listener;

    public AbstractAsyncBulkByScrollAction(ESLogger logger, Client client, ThreadPool threadPool, Request mainRequest,
            SearchRequest firstSearchRequest, ActionListener<Response> listener) {
        this.logger = logger;
        this.client = client;
        this.threadPool = threadPool;
        this.mainRequest = mainRequest;
        this.firstSearchRequest = firstSearchRequest;
        this.listener = listener;
    }

    protected abstract BulkRequest buildBulk(Iterable<SearchHit> docs);

    protected abstract Response buildResponse(long took);

    public void start() {
        initialSearch();
    }

    /**
     * Count of documents updated.
     */
    public long updated() {
        return updated.get();
    }

    /**
     * Count of documents created.
     */
    public long created() {
        return created.get();
    }

    /**
     * Count of successful delete operations.
     */
    public long deleted() {
        return deleted.get();
    }

    /**
     * The number of scan responses this request has processed.
     */
    public int batches() {
        return batches.get();
    }

    public long versionConflicts() {
        return versionConflicts.get();
    }

    public long successfullyProcessed() {
        return updated.get() + created.get() + deleted.get();
    }

    public List<Failure> indexingFailures() {
        return unmodifiableList(indexingFailures);
    }

    public List<ShardSearchFailure> searchFailures() {
        return unmodifiableList(searchFailures);
    }

    private void initialSearch() {
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
            client.search(firstSearchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    logger.debug("[{}] documents match query", response.getHits().getTotalHits());
                    onScrollResponse(response);
                }

                @Override
                public void onFailure(Throwable e) {
                    finishHim(e);
                }
            });
        } catch (Throwable t) {
            finishHim(t);
        }
    }

    void onScrollResponse(SearchResponse searchResponse) {
        scroll.set(searchResponse.getScrollId());
        if (searchResponse.getShardFailures() != null && searchResponse.getShardFailures().length > 0) {
            Collections.addAll(searchFailures, searchResponse.getShardFailures());
            startNormalTermination();
            return;
        }
        threadPool.generic().execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                try {
                    SearchHit[] docs = searchResponse.getHits().getHits();
                    logger.debug("scroll returned [{}] documents with a scroll id of [{}]", docs.length, searchResponse.getScrollId());
                    if (docs.length == 0) {
                        startNormalTermination();
                        return;
                    }
                    batches.incrementAndGet();
                    List<SearchHit> docsIterable = Arrays.asList(docs);
                    if (mainRequest.getSize() != SIZE_ALL_MATCHES) {
                        // Truncate the docs if we have more than the request size
                        long remaining = max(0, mainRequest.getSize() - successfullyProcessed());
                        if (remaining < docs.length) {
                            docsIterable = docsIterable.subList(0, (int) remaining);
                        }
                    }
                    BulkRequest request = buildBulk(docsIterable);
                    if (request.requests().isEmpty()) {
                        /*
                         * If we noop-ed the entire batch then just skip to the next
                         * batch or the BulkRequest would fail validation.
                         */
                        startNextScrollRequest();
                        return;
                    }
                    request.timeout(mainRequest.getTimeout());
                    request.consistencyLevel(mainRequest.getConsistency());
                    if (logger.isDebugEnabled()) {
                        logger.debug("sending [{}] entry, [{}] bulk request", request.requests().size(),
                                new ByteSizeValue(request.estimatedSizeInBytes()));
                    }
                    // NOCOMMIT handle rejections
                    client.bulk(request, new ActionListener<BulkResponse>() {
                        @Override
                        public void onResponse(BulkResponse response) {
                            onBulkResponse(response);
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            finishHim(e);
                        }
                    });
                } catch (Throwable t) {
                    finishHim(t);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                finishHim(t);
            }
        });
    }

    void onBulkResponse(BulkResponse response) {
        try {
            Set<String> destinationIndicesThisBatch = new HashSet<>();
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    recordFailure(item.getFailure());
                    continue;
                }

                switch (item.getOpType()) {
                case "index":
                case "create":
                    IndexResponse ir = item.getResponse();
                    if (ir.isCreated()) {
                        created.incrementAndGet();
                    } else {
                        updated.incrementAndGet();
                    }
                    break;
                case "delete":
                    deleted.incrementAndGet();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown op type:  " + item.getOpType());
                }
                // Track the indexes we've seen so we can refresh them if requested
                destinationIndices.add(item.getIndex());
            }
            destinationIndices.addAll(destinationIndicesThisBatch);

            if (false == indexingFailures.isEmpty()) {
                startNormalTermination();
                return;
            }

            if (mainRequest.getSize() != SIZE_ALL_MATCHES && successfullyProcessed() >= mainRequest.getSize()) {
                // We've processed all the requested docs.
                startNormalTermination();
                return;
            }
            startNextScrollRequest();
        } catch (Throwable t) {
            finishHim(t);
        }
    }

    void startNextScrollRequest() {
        SearchScrollRequest request = new SearchScrollRequest();
        request.scrollId(scroll.get()).scroll(firstSearchRequest.scroll());
        client.searchScroll(request, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                onScrollResponse(response);
            }

            @Override
            public void onFailure(Throwable e) {
                finishHim(e);
            }
        });
    }

    private void recordFailure(Failure failure) {
        if (failure.getStatus() == CONFLICT) {
            versionConflicts.incrementAndGet();
            if (false == mainRequest.isAbortOnVersionConflict()) {
                return;
            }
        }
        indexingFailures.add(failure);
    }

    void startNormalTermination() {
        if (false == mainRequest.isRefresh()) {
            finishHim(null);
            return;
        }
        RefreshRequest refresh = new RefreshRequest();
        refresh.indices(destinationIndices.toArray(new String[destinationIndices.size()]));
        client.admin().indices().refresh(refresh, new ActionListener<RefreshResponse>() {
            @Override
            public void onResponse(RefreshResponse response) {
                finishHim(null);
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
     * @param failure
     *            the failure that caused the request to fail prematurely if not
     *            null. If not null this doesn't mean the request was entirely
     *            successful - it may have accumulated failures in the failures
     *            list.
     */
    void finishHim(Throwable failure) {
        String scrollId = scroll.get();
        if (Strings.hasLength(scrollId)) {
            /*
             * Fire off the clear scroll but don't wait for it it return before
             * we send the use their response.
             */
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest, new ActionListener<ClearScrollResponse>() {
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
            listener.onResponse(buildResponse(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime.get())));
        } else {
            listener.onFailure(failure);
        }
    }
}
