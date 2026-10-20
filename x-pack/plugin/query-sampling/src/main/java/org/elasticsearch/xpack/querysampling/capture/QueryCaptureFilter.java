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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Captures a copy of eligible kNN searches on the coordinating node; the search itself proceeds unchanged.
 * Eligible: a single top-level {@code knn} with a literal float vector, and nothing that changes which documents are
 * searched or which hits come back. Searches with a parent task (cross-cluster or internal) are skipped.
 * <p>
 * The filter sees the request before the search rewrites it, so a vector that is only known after the rewrite (a query
 * vector builder such as a {@code semantic_text} field, or a {@code knn} query inside {@code query}) is not captured.
 * Capturing those would need a hook after the rewrite.
 */
public final class QueryCaptureFilter implements MappedActionFilter {

    private static final Logger logger = LogManager.getLogger(QueryCaptureFilter.class);

    private static final int DEFAULT_SIZE = 10;

    private final Consumer<CapturedSearch> consumer;
    private final Supplier<Random> random;
    private volatile boolean enabled;
    private final CaptureRate captureRate = new CaptureRate(0, 0);
    private long knnSearchesAtLastUpdate;
    private final LongAdder knnSearches = new LongAdder();
    private final LongAdder captured = new LongAdder();

    public QueryCaptureFilter(ClusterSettings clusterSettings, Consumer<CapturedSearch> consumer) {
        this(clusterSettings, consumer, Randomness::get);
    }

    /** {@code random} is for the benchmark: {@link Randomness#get()} does not work in a JVM that has the test framework but no test. */
    public QueryCaptureFilter(ClusterSettings clusterSettings, Consumer<CapturedSearch> consumer, Supplier<Random> random) {
        this.consumer = consumer;
        this.random = random;
        clusterSettings.initializeAndWatch(QuerySamplingSettings.ENABLED, value -> this.enabled = value);
        clusterSettings.initializeAndWatch(QuerySamplingSettings.CAPTURE_RATE, captureRate::configured);
        clusterSettings.initializeAndWatch(QuerySamplingSettings.MIN_CAPTURES_PER_HOUR, captureRate::minPerHour);
    }

    public Scheduler.Cancellable startRateUpdates(ThreadPool threadPool, Executor executor) {
        long[] last = { System.nanoTime() };
        return threadPool.scheduleWithFixedDelay(() -> {
            long now = System.nanoTime();
            updateRate((now - last[0]) / 1_000_000_000.0);
            last[0] = now;
        }, TimeValue.timeValueSeconds(1), executor);
    }

    public synchronized void updateRate(double seconds) {
        long total = knnSearches.sum();
        long arrivals = total - knnSearchesAtLastUpdate;
        knnSearchesAtLastUpdate = total;
        if (enabled) {
            captureRate.observe(arrivals, seconds);
        } else {
            captureRate.forgetTraffic(); // nothing is counted while off, and what was seen before is out of date
        }
    }

    public double effectiveCaptureRate() {
        return captureRate.effective();
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
            // read once: the draw and the rate recorded with the search must be the same value
            double rate = captureRate.effective();
            if (knn != null && random.get().nextDouble() < rate) {
                captured.increment();
                try {
                    searchListener = withResults(listener, capture(task, searchRequest, knn), rate);
                } catch (Exception e) {
                    // capturing must never fail the search
                    logger.debug("failed to capture kNN search", e);
                }
            }
        }
        chain.proceed(task, action, request, searchListener);
    }

    private <Response extends ActionResponse> ActionListener<Response> withResults(
        ActionListener<Response> listener,
        CapturedQuery query,
        double rate
    ) {
        return listener.delegateFailure((l, response) -> {
            if (response instanceof SearchResponse searchResponse) {
                try {
                    consumer.accept(captureResults(query, searchResponse, rate));
                } catch (Exception e) {
                    logger.debug("failed to capture kNN search results", e);
                }
            }
            l.onResponse(response);
        });
    }

    // the response is ref-counted, so what is needed is copied out of it
    private static CapturedSearch captureResults(CapturedQuery query, SearchResponse response, double rate) {
        SearchHit[] searchHits = response.getHits().getHits();
        List<CapturedSearch.Hit> hits = new ArrayList<>(searchHits.length);
        for (SearchHit hit : searchHits) {
            hits.add(new CapturedSearch.Hit(hit.getIndex(), hit.getId(), hit.getScore()));
        }
        return new CapturedSearch(query, hits, response.getTookInMillis(), rate);
    }

    public long knnSearches() {
        return knnSearches.sum();
    }

    public long captured() {
        return captured.sum();
    }

    private static KnnSearchBuilder eligibleKnn(SearchRequest request) {
        SearchSourceBuilder source = request.source();
        // a query alongside knn returns a mix that the knn section alone cannot replay
        if (source == null || source.knnSearch().size() != 1 || source.query() != null) {
            return null;
        }
        if (searchesOtherDocumentsThanTheIndicesHold(request) || returnsOtherHitsThanTheNearestK(source)) {
            return null;
        }
        KnnSearchBuilder knn = source.knnSearch().get(0);
        VectorData vector = knn.getQueryVector();
        if (knn.getQueryVectorBuilder() != null || vector == null || vector.isFloat() == false) {
            return null;
        }
        // with a similarity threshold the result count depends on the data, so an exact search of k does not match
        if (knn.getSimilarity() != null) {
            return null;
        }
        return knn;
    }

    // ground truth scans the request's indices, so searches over other documents (routing, shard preference, PIT, scroll)
    // have nothing comparable
    private static boolean searchesOtherDocumentsThanTheIndicesHold(SearchRequest request) {
        return request.routing() != null
            || (request.preference() != null && request.preference().startsWith("_shards:"))
            || request.pointInTimeBuilder() != null
            || request.scroll() != null;
    }

    // recall compares the returned hits with the exact nearest ones, so hits that are offset, cut, collapsed or rescored are out
    private static boolean returnsOtherHitsThanTheNearestK(SearchSourceBuilder source) {
        return source.from() > 0
            || source.minScore() != null
            || source.postFilter() != null
            || source.collapse() != null
            || (source.rescores() != null && source.rescores().isEmpty() == false);
    }

    // the exact search and the recall use min(k, size), e.g. the default size of 10 with a bigger k
    private static int hitsReturned(SearchSourceBuilder source, KnnSearchBuilder knn) {
        int size = source.size() < 0 ? DEFAULT_SIZE : source.size();
        return Math.min(knn.k(), size);
    }

    private static CapturedQuery capture(Task task, SearchRequest request, KnnSearchBuilder knn) {
        return new CapturedQuery(
            request.indices().clone(),
            knn.getField(),
            knn.getQueryVector().floatVector().clone(),
            hitsReturned(request.source(), knn),
            knn.getNumCands(),
            knn.getVisitPercentage(),
            knn.getRescoreVectorBuilder() == null ? null : knn.getRescoreVectorBuilder().oversample(),
            List.copyOf(knn.getFilterQueries()),
            task.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER)
        );
    }
}
