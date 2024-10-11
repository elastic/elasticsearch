/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.util.CollectionUtils.isEmpty;
import static org.elasticsearch.core.TimeValue.timeValueNanos;

/**
 * A scrollable source of hits from a {@linkplain Client} instance.
 */
public class ClientScrollableHitSource extends ScrollableHitSource {
    private final ParentTaskAssigningClient client;
    private final SearchRequest firstSearchRequest;

    public ClientScrollableHitSource(
        Logger logger,
        BackoffPolicy backoffPolicy,
        ThreadPool threadPool,
        Runnable countSearchRetry,
        Consumer<AsyncResponse> onResponse,
        Consumer<Exception> fail,
        ParentTaskAssigningClient client,
        SearchRequest firstSearchRequest
    ) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, onResponse, fail);
        this.client = client;
        this.firstSearchRequest = firstSearchRequest;
        firstSearchRequest.allowPartialSearchResults(false);
    }

    @Override
    public void doStart(RejectAwareActionListener<Response> searchListener) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "executing initial scroll against {}",
                isEmpty(firstSearchRequest.indices()) ? "all indices" : firstSearchRequest.indices()
            );
        }
        client.search(firstSearchRequest, wrapListener(searchListener));
    }

    @Override
    protected void doStartNextScroll(String scrollId, TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        SearchScrollRequest request = new SearchScrollRequest();
        // Add the wait time into the scroll timeout so it won't timeout while we wait for throttling
        request.scrollId(scrollId).scroll(timeValueNanos(firstSearchRequest.scroll().keepAlive().nanos() + extraKeepAlive.nanos()));
        client.searchScroll(request, wrapListener(searchListener));
    }

    private static ActionListener<SearchResponse> wrapListener(RejectAwareActionListener<Response> searchListener) {
        return new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                searchListener.onResponse(wrapSearchResponse(searchResponse));
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class) != null) {
                    searchListener.onRejection(e);
                } else {
                    searchListener.onFailure(e);
                }
            }
        };
    }

    @Override
    public void clearScroll(String scrollId, Runnable onCompletion) {
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
                onCompletion.run();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> "Failed to clear scroll [" + scrollId + "]", e);
                onCompletion.run();
            }
        });
    }

    @Override
    protected void cleanup(Runnable onCompletion) {
        onCompletion.run();
    }

    private static Response wrapSearchResponse(SearchResponse response) {
        List<SearchFailure> failures;
        if (response.getShardFailures() == null) {
            failures = emptyList();
        } else {
            failures = new ArrayList<>(response.getShardFailures().length);
            for (ShardSearchFailure failure : response.getShardFailures()) {
                String nodeId = failure.shard() == null ? null : failure.shard().getNodeId();
                failures.add(new SearchFailure(failure.getCause(), failure.index(), failure.shardId(), nodeId));
            }
        }
        List<Hit> hits;
        if (response.getHits().getHits() == null || response.getHits().getHits().length == 0) {
            hits = emptyList();
        } else {
            hits = new ArrayList<>(response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                hits.add(new ClientHit(hit));
            }
            hits = unmodifiableList(hits);
        }
        long total = response.getHits().getTotalHits().value;
        return new Response(response.isTimedOut(), failures, total, hits, response.getScrollId());
    }

    private static class ClientHit implements Hit {
        private final SearchHit delegate;
        private final BytesReference source;

        ClientHit(SearchHit delegate) {
            this.delegate = delegate.asUnpooled(); // TODO: use pooled version here
            source = this.delegate.hasSource() ? this.delegate.getSourceRef() : null;
        }

        @Override
        public String getIndex() {
            return delegate.getIndex();
        }

        @Override
        public String getId() {
            return delegate.getId();
        }

        @Override
        public BytesReference getSource() {
            return source;
        }

        @Override
        public XContentType getXContentType() {
            return XContentHelper.xContentType(source);
        }

        @Override
        public long getVersion() {
            return delegate.getVersion();
        }

        @Override
        public long getSeqNo() {
            return delegate.getSeqNo();
        }

        @Override
        public long getPrimaryTerm() {
            return delegate.getPrimaryTerm();
        }

        @Override
        public String getRouting() {
            return fieldValue(RoutingFieldMapper.NAME);
        }

        private <T> T fieldValue(String fieldName) {
            DocumentField field = delegate.field(fieldName);
            return field == null ? null : field.getValue();
        }
    }
}
