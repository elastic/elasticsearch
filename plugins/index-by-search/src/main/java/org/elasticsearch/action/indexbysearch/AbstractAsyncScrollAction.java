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

package org.elasticsearch.action.indexbysearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.search.SearchHit;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.max;

/**
 * Abstract base for scrolling across a search and executing bulk actions on all
 * results. Right now that is only used by AsyncIndexBySearchAction but its
 * still nice because this handles the scrolling while AsyncIndexBySearchAction
 * handles the document building.
 */
public abstract class AbstractAsyncScrollAction<Request extends ActionRequest<?>, Response> {
    protected final Request mainRequest;

    private final AtomicLong total = new AtomicLong(-1);
    private final AtomicLong startTime = new AtomicLong(-1);
    private final AtomicLong indexed = new AtomicLong(0);
    private final AtomicLong created = new AtomicLong(0);
    private final AtomicLong deleted = new AtomicLong(0);
    private final AtomicReference<String> scroll = new AtomicReference<>();

    private final ESLogger logger;
    private final TransportSearchAction searchAction;
    private final TransportSearchScrollAction scrollAction;
    private final TransportBulkAction bulkAction;
    private final TransportClearScrollAction clearScroll;
    private final SearchRequest firstSearchRequest;
    private final ActionListener<Response> listener;
    private final int maximumDocs;

    public AbstractAsyncScrollAction(ESLogger logger, TransportSearchAction searchAction, TransportSearchScrollAction scrollAction,
            TransportBulkAction bulkAction, TransportClearScrollAction clearScroll, Request mainRequest, SearchRequest firstSearchRequest,
            ActionListener<Response> listener, int maximumDocs) {
        this.logger = logger;
        this.searchAction = searchAction;
        this.scrollAction = scrollAction;
        this.bulkAction = bulkAction;
        this.clearScroll = clearScroll;
        this.mainRequest = mainRequest;
        this.firstSearchRequest = firstSearchRequest;
        this.listener = listener;
        this.maximumDocs = maximumDocs;
    }

    protected abstract BulkRequest buildBulk(Iterable<SearchHit> docs);

    protected abstract Response buildResponse();

    public void start() {
        initialSearch();
    }

    public long indexed() {
        return indexed.get();
    }

    public long created() {
        return created.get();
    }

    public long deleted() {
        return deleted.get();
    }

    public long processed() {
        return indexed.get() + created.get() + deleted.get();
    }

    void initialSearch() {
        try {
            startTime.set(System.currentTimeMillis());
            if (logger.isWarnEnabled()) {
                logger.warn("executing initial scroll against {}{}",
                        firstSearchRequest.indices() == null ? "all indices" : firstSearchRequest.indices(),
                        firstSearchRequest.types() == null || firstSearchRequest.types().length == 0 ? "" : firstSearchRequest.types());
            }
            searchAction.execute(firstSearchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    logger.warn("[{}] documents match query", response.getHits().getTotalHits());
                    total.set(response.getHits().getTotalHits());
                    onScrollResponse(response);
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.error("failed while executing the initial scroll request", e);
                    listener.onFailure(e);
                }
            });
        } catch (Throwable t) {
            finishHim(t);
        }
    }

    void onScrollResponse(final SearchResponse searchResponse) {
        try {
            scroll.set(searchResponse.getScrollId());
            SearchHit[] docs = searchResponse.getHits().getHits();
            logger.warn("scroll returned [{}] documents with a scroll id of [{}]", docs.length, searchResponse.getScrollId());
            if (docs.length == 0) {
                finishHim(null);
                return;
            }
            List<SearchHit> docsIterable = Arrays.asList(docs);
            if (maximumDocs != -1) {
                // Truncate the docs if we have more than the request size
                long remaining = max(0, maximumDocs - processed());
                if (remaining <= docs.length) {
                    if (remaining < docs.length) {
                        docsIterable = docsIterable.subList(0, (int) remaining);
                    }
                }
            }
            BulkRequest request = buildBulk(docsIterable);
            if (logger.isWarnEnabled()) {
                logger.warn("sending [{}] entry, [{}] bulk request", request.requests().size(),
                        new ByteSizeValue(request.estimatedSizeInBytes()));
            }
            bulkAction.execute(request, new ActionListener<BulkResponse>() {
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

    void onBulkResponse(BulkResponse response) {
        try {
            // TODO error checking
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    // TODO something
                    continue;
                }
                switch (item.getOpType()) {
                case "index":
                    indexed.incrementAndGet();
                    break;
                case "delete":
                    deleted.incrementAndGet();
                    break;
                case "create":
                    created.incrementAndGet();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown op type:  " + item.getOpType());
                }
            }

            if (maximumDocs != -1 && processed() >= maximumDocs) {
                // We've processed all the requested docs.
                finishHim(null);
                return;
            }
            SearchScrollRequest request = new SearchScrollRequest(mainRequest);
            request.scrollId(scroll.get()).scroll(firstSearchRequest.scroll());
            scrollAction.execute(request, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
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

    void finishHim(final Throwable failure) {
        if (failure != null) {
            logger.warn("scrolling failed", failure);
        }

        String scroll = this.scroll.get();
        if (Strings.hasLength(scroll) == false) {
            return;
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest(mainRequest);
        clearScrollRequest.addScrollId(scroll);
        clearScroll.execute(clearScrollRequest, new ActionListener<ClearScrollResponse>() {
            @Override
            public void onResponse(ClearScrollResponse response) {
                logger.warn("Freed [{}] contexts", response.getNumFreed());
                if (failure == null) {
                    listener.onResponse(buildResponse());
                } else {
                    listener.onFailure(failure);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                logger.warn("Failed to clear scroll", e);
                if (failure == null) {
                    listener.onFailure(e);
                    return;
                }
                listener.onFailure(failure);
            }
        });
    }
}
