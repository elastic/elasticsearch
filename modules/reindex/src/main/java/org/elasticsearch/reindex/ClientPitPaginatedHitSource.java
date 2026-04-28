/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.ResumeInfo.PitWorkerResumeInfo;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.util.CollectionUtils.isEmpty;
import static org.elasticsearch.core.TimeValue.timeValueNanos;

/**
 * PIT-based paginated search retrieves large result sets by opening a point-in-time and repeatedly
 * requesting the next batch using {@code search_after} with the sort values of the last hit.
 * This is the recommended approach for deep pagination as it avoids the resource costs of scroll.
 * <p>
 * This implementation is a PIT-based source of hits from a {@linkplain org.elasticsearch.client.internal.Client} instance.
 * The PIT must already be opened and injected into the search request before this hit source is used.
 */
public class ClientPitPaginatedHitSource extends PitPaginatedHitSource {
    private final ParentTaskAssigningClient client;
    private final SearchRequest firstSearchRequest;
    private final AtomicReference<PointInTimeBuilder> pitBuilder;

    public ClientPitPaginatedHitSource(
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
        SearchSourceBuilder source = firstSearchRequest.source();
        PointInTimeBuilder initialPit = source == null ? null : source.pointInTimeBuilder();
        if (initialPit == null) {
            throw new IllegalArgumentException("SearchRequest must have pointInTimeBuilder set for PIT-based pagination");
        }
        this.pitBuilder = new AtomicReference<>(initialPit);
        firstSearchRequest.allowPartialSearchResults(false);
    }

    @Override
    public void doFirstSearch(RejectAwareActionListener<Response> searchListener) {
        logger.debug(
            "executing initial local PIT search against {}",
            () -> isEmpty(firstSearchRequest.indices()) ? "all indices" : firstSearchRequest.indices()
        );
        client.search(firstSearchRequest, wrapListener(searchListener));
    }

    @Override
    public BytesReference getPitId() {
        PointInTimeBuilder pit = pitBuilder.get();
        return pit != null ? pit.getEncodedId() : null;
    }

    @Override
    protected void restorePitState(PitWorkerResumeInfo resumeInfo) {
        pitBuilder.set(
            new PointInTimeBuilder(resumeInfo.pitId()).setKeepAlive(firstSearchRequest.source().pointInTimeBuilder().getKeepAlive())
        );
        setSearchAfterValues(resumeInfo.searchAfterValues());
    }

    @Override
    protected void doNextPitSearch(Object[] searchAfter, TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        SearchSourceBuilder source = firstSearchRequest.source()
            .shallowCopy()
            .searchAfter(searchAfter)
            .pointInTimeBuilder(extendPitKeepAlive(pitBuilder.get(), extraKeepAlive));
        SearchRequest nextRequest = new SearchRequest(firstSearchRequest).source(source);
        client.search(nextRequest, wrapListener(searchListener));
    }

    private static PointInTimeBuilder extendPitKeepAlive(PointInTimeBuilder pit, TimeValue extraKeepAlive) {
        TimeValue keepAlive = pit.getKeepAlive();
        if (keepAlive == null || extraKeepAlive == null || extraKeepAlive.nanos() == 0) {
            return pit;
        }
        return new PointInTimeBuilder(pit.getEncodedId()).setKeepAlive(timeValueNanos(keepAlive.nanos() + extraKeepAlive.nanos()));
    }

    private ActionListener<SearchResponse> wrapListener(RejectAwareActionListener<Response> searchListener) {
        return new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                if (searchResponse.pointInTimeId() != null) {
                    PointInTimeBuilder currentPit = pitBuilder.get();
                    pitBuilder.set(
                        new PointInTimeBuilder(searchResponse.pointInTimeId()).setKeepAlive(
                            currentPit != null ? currentPit.getKeepAlive() : null
                        )
                    );
                }
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
    protected void cleanup(Runnable onCompletion) {
        onCompletion.run();
    }

    private static Response wrapSearchResponse(SearchResponse response) {
        List<PaginatedSearchFailure> failures;
        if (response.getShardFailures() == null) {
            failures = emptyList();
        } else {
            failures = new ArrayList<>(response.getShardFailures().length);
            for (ShardSearchFailure failure : response.getShardFailures()) {
                String nodeId = failure.shard() == null ? null : failure.shard().getNodeId();
                failures.add(new PaginatedSearchFailure(failure.getCause(), failure.index(), failure.shardId(), nodeId));
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
        long total = response.getHits().getTotalHits().value();
        Object[] searchAfterValues = null;
        if (hits.isEmpty() == false) {
            Hit lastHit = hits.getLast();
            searchAfterValues = lastHit.getSortValues();
        }
        // PIT search returns no scrollId; search_after values come from last hit's sort values
        return new Response(response.isTimedOut(), failures, total, hits, null, searchAfterValues, response.pointInTimeId());
    }
}
