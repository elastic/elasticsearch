/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Stage 1 of the pipeline: picks kNN searches on the coordinating node and hands a copy of them to the
 * rest of the pipeline. The search itself always proceeds unchanged, and the work done for a search
 * that is not captured is a couple of field reads and one random draw.
 * <p>
 * For now only searches with a single top-level {@code knn} section, a literal float query vector and no
 * additional {@code query} are eligible. Searches with a parent task are skipped: those are the remote side of a cross-cluster
 * search or searches issued internally by other features, not user traffic arriving at this node.
 */
public final class QueryCaptureFilter implements MappedActionFilter {

    private static final Logger logger = LogManager.getLogger(QueryCaptureFilter.class);

    private final Consumer<CapturedSearch> consumer;
    private volatile boolean enabled;
    private volatile double captureRate;
    private final LongAdder knnSearches = new LongAdder();
    private final LongAdder captured = new LongAdder();

    public QueryCaptureFilter(ClusterSettings clusterSettings, Consumer<CapturedSearch> consumer) {
        this.consumer = consumer;
        clusterSettings.initializeAndWatch(QuerySamplingSettings.ENABLED, value -> this.enabled = value);
        clusterSettings.initializeAndWatch(QuerySamplingSettings.CAPTURE_RATE, value -> this.captureRate = value);
    }

    @Override
    public String actionName() {
        return TransportSearchAction.NAME;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        ActionListener<Response> searchListener = listener;
        if (enabled && request instanceof SearchRequest searchRequest && task.getParentTaskId().isSet() == false) {
            KnnSearchBuilder knn = eligibleKnn(searchRequest);
            if (knn != null) {
                knnSearches.increment();
            }
            if (knn != null && Randomness.get().nextDouble() < captureRate) {
                captured.increment();
                try {
                    searchListener = withResults(listener, capture(task, searchRequest, knn));
                } catch (Exception e) {
                    // capturing must never fail the search
                    logger.debug("failed to capture kNN search", e);
                }
            }
        }
        chain.proceed(task, action, request, searchListener);
    }

    /**
     * Wraps the listener so the response is copied before it goes back to the user. Failed searches have
     * nothing to learn from and are not captured.
     */
    private <Response extends ActionResponse> ActionListener<Response> withResults(ActionListener<Response> listener, CapturedQuery query) {
        return listener.delegateFailure((l, response) -> {
            if (response instanceof SearchResponse searchResponse) {
                try {
                    consumer.accept(captureResults(query, searchResponse));
                } catch (Exception e) {
                    logger.debug("failed to capture kNN search results", e);
                }
            }
            l.onResponse(response);
        });
    }

    /**
     * Copies what is needed out of the response: it is ref-counted, so it cannot be kept past this call.
     */
    private static CapturedSearch captureResults(CapturedQuery query, SearchResponse response) {
        SearchHit[] searchHits = response.getHits().getHits();
        List<CapturedSearch.Hit> hits = new ArrayList<>(searchHits.length);
        for (SearchHit hit : searchHits) {
            hits.add(new CapturedSearch.Hit(hit.getIndex(), hit.getId(), hit.getScore()));
        }
        return new CapturedSearch(query, hits, response.getTookInMillis());
    }

    /**
     * kNN searches the gate looked at while sampling was enabled, whether or not they were captured.
     */
    public long knnSearches() {
        return knnSearches.sum();
    }

    /**
     * Searches the gate picked for capture.
     */
    public long captured() {
        return captured.sum();
    }

    private static KnnSearchBuilder eligibleKnn(SearchRequest request) {
        SearchSourceBuilder source = request.source();
        // a search that also has a query returns a mix of both, which the captured kNN section alone cannot replay
        if (source == null || source.knnSearch().size() != 1 || source.query() != null) {
            return null;
        }
        KnnSearchBuilder knn = source.knnSearch().get(0);
        VectorData vector = knn.getQueryVector();
        if (knn.getQueryVectorBuilder() != null || vector == null || vector.isFloat() == false) {
            return null;
        }
        return knn;
    }

    private static CapturedQuery capture(Task task, SearchRequest request, KnnSearchBuilder knn) {
        return new CapturedQuery(
            request.indices().clone(),
            knn.getField(),
            knn.getQueryVector().floatVector().clone(),
            knn.k(),
            knn.getNumCands(),
            knn.getVisitPercentage(),
            knn.getRescoreVectorBuilder() == null ? null : knn.getRescoreVectorBuilder().oversample(),
            List.copyOf(knn.getFilterQueries()),
            task.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER)
        );
    }
}
