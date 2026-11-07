/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

import org.elasticsearch.action.search.SearchLogContext;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.logging.activity.ActivityLogListener;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Captures a copy of eligible kNN searches on the coordinating node; the search itself proceeds unchanged. It follows
 * searches through the query log, at the point where the log sees them, whether or not the log is enabled: the decision
 * to capture is made when the search starts, so a search that is not captured is not followed.
 * Eligible: a single top-level {@code knn} with a literal float vector, and nothing that changes which documents are
 * searched (routing, any preference, PIT, scroll) or which hits come back or how they are ranked (an offset, no hits,
 * {@code search_after}, {@code terminate_after}, a minimum score, a post filter, collapsing, rescoring, boosts of indices,
 * a similarity threshold, a sort by a field, which leaves the hits without a score). Searches with a parent task
 * (cross-cluster or internal) are skipped, and so are answers that are partial: a timeout, a search that stopped early or
 * shards that failed would count as a low recall.
 * <p>
 * The query log sees the request before the search rewrites it, so a vector that is only known after the rewrite (a query
 * vector builder such as a {@code semantic_text} field, or a {@code knn} query inside {@code query}) is not captured.
 * Capturing those would need a hook after the rewrite.
 */
public final class QueryCaptureListener implements ActivityLogListener<SearchLogContext, SearchRequest> {

    private static final Logger logger = LogManager.getLogger(QueryCaptureListener.class);

    private static final int DEFAULT_SIZE = SearchService.DEFAULT_SIZE;

    private static final long NEVER = Long.MIN_VALUE;
    private static final long WARN_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);

    private final Consumer<CapturedSearch> consumer;
    private final Supplier<Random> random;
    private volatile boolean enabled;
    private final CaptureRate captureRate = new CaptureRate(0, 0);
    private final LongSupplier millis;
    private final AtomicReference<Mark> lastUpdate;
    private final AtomicLong lastWarning = new AtomicLong(NEVER);

    // how often the capture rate is brought up to date with the traffic, by the searches themselves
    private static final long UPDATE_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(10);

    /** The number of kNN searches there had been when the capture rate was last brought up to date, and when. */
    private record Mark(long knnSearches, long millis) {}

    private final LongAdder knnSearches = new LongAdder();
    private final LongAdder captured = new LongAdder();

    /**
     * @param millis a clock in milliseconds that is cheap to read: it is read by every kNN search, and a call to the system
     *               clock costs far more than the rest of the listener when many threads make it at once, so the cached time
     *               of the thread pool is the one to give
     */
    public QueryCaptureListener(ClusterSettings clusterSettings, Consumer<CapturedSearch> consumer, LongSupplier millis) {
        this(clusterSettings, consumer, millis, Randomness::get);
    }

    /** {@code random} is for the benchmark: {@link Randomness#get()} does not work in a JVM that has the test framework but no test. */
    public QueryCaptureListener(
        ClusterSettings clusterSettings,
        Consumer<CapturedSearch> consumer,
        LongSupplier millis,
        Supplier<Random> random
    ) {
        this.consumer = consumer;
        this.random = random;
        this.millis = millis;
        this.lastUpdate = new AtomicReference<>(new Mark(0, millis.getAsLong()));
        clusterSettings.initializeAndWatch(QuerySamplingSettings.ENABLED, value -> this.enabled = value);
        clusterSettings.initializeAndWatch(QuerySamplingSettings.CAPTURE_RATE, captureRate::configured);
        clusterSettings.initializeAndWatch(QuerySamplingSettings.MIN_CAPTURES_PER_HOUR, captureRate::minPerHour);
    }

    /**
     * Brings the capture rate up to date with the traffic once an interval has passed. The searches do it, so a node
     * without kNN searches does nothing, and the one that finds it due does it for all: the others go on.
     */
    private void updateRateIfDue() {
        Mark last = lastUpdate.get();
        long now = millis.getAsLong();
        if (now - last.millis() < UPDATE_INTERVAL_MILLIS) {
            return;
        }
        long total = knnSearches.sum();
        if (lastUpdate.compareAndSet(last, new Mark(total, now))) {
            long elapsed = now - last.millis();
            if (elapsed >= 2 * UPDATE_INTERVAL_MILLIS) {
                captureRate.forgetTraffic(); // after a quiet spell, or while sampling was off, the traffic before it says nothing
            }
            captureRate.observe(total - last.knnSearches(), elapsed / 1000.0);
        }
    }

    public double effectiveCaptureRate() {
        return captureRate.effective();
    }

    /**
     * Decides when the search starts whether it is captured, so a search that is not is not followed.
     */
    @Override
    @Nullable
    public Consumer<SearchLogContext> start(Task task, SearchRequest request) {
        if (enabled == false || task.getParentTaskId().isSet()) {
            return null;
        }
        KnnSearchBuilder knn = eligibleKnn(request);
        if (knn == null) {
            return null;
        }
        knnSearches.increment();
        updateRateIfDue();
        // read once: the draw and the rate recorded with the search must be the same value
        double rate = captureRate.effective();
        if (random.get().nextDouble() >= rate) {
            return null;
        }
        captured.increment();
        CapturedQuery query;
        try {
            query = capture(task, request, knn);
        } catch (Exception e) {
            // capturing must never fail the search
            warn("failed to capture kNN search", e);
            return null;
        }
        return context -> completed(context, query, rate);
    }

    private void completed(SearchLogContext context, CapturedQuery query, double rate) {
        // a failure or a partial answer (shards that failed, a timeout, a search that stopped early) would count as a low recall
        SearchResponse response = context.getResponse();
        if (response == null || isComplete(response) == false) {
            return;
        }
        try {
            consumer.accept(captureResults(query, response, rate));
        } catch (Exception e) {
            warn("failed to capture kNN search results", e);
        }
    }

    private static boolean isComplete(SearchResponse response) {
        return response.isTimedOut() == false
            && response.getFailedShards() == 0
            && Boolean.TRUE.equals(response.isTerminatedEarly()) == false;
    }

    // this runs for every captured search, so a failure that repeats must not fill the log
    private void warn(String message, Exception e) {
        long now = millis.getAsLong();
        long last = lastWarning.get();
        if ((last == NEVER || now - last >= WARN_INTERVAL_MILLIS) && lastWarning.compareAndSet(last, now)) {
            logger.warn(message + " (logged at most once a minute)", e);
        }
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
        if (searchesOtherDocumentsThanTheIndicesHold(request)
            || returnsOtherHitsThanTheNearestK(source)
            || returnsHitsWithoutScores(source)) {
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

    // ground truth scans the request's indices, so searches over other documents (routing, any preference, which can
    // pick the shards or the copies of them, PIT, scroll) have nothing comparable
    private static boolean searchesOtherDocumentsThanTheIndicesHold(SearchRequest request) {
        return request.routing() != null
            || request.preference() != null
            || request.pointInTimeBuilder() != null
            || request.scroll() != null;
    }

    // sorted by a field, the hits have no score, which the stored samples and the hardness of a query are made of
    private static boolean returnsHitsWithoutScores(SearchSourceBuilder source) {
        return source.sorts() != null && source.sorts().stream().anyMatch(sort -> sort instanceof ScoreSortBuilder == false);
    }

    // recall compares the returned hits with the exact nearest ones, so hits that are offset, cut, collapsed, rescored or
    // ranked with boosts of indices are out, and so is a search that asks for no hits
    private static boolean returnsOtherHitsThanTheNearestK(SearchSourceBuilder source) {
        return source.from() > 0
            || source.size() == 0
            || source.searchAfter() != null
            || source.terminateAfter() > 0
            || (source.indexBoosts() != null && source.indexBoosts().isEmpty() == false)
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
