/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Computes the {@link Selectivity} stratum of a filtered query (share of vectors passing the filters) with two
 * size-0 count searches, once per distinct query, in the background and only if enabled, as they cost extra searches.
 * Uncounted queries get no selectivity. Unlike the other strata it does not change pick probabilities.
 */
public final class SelectivityStrata {

    private static final Logger logger = LogManager.getLogger(SelectivityStrata.class);

    /** Counts are searches nobody asked for, so few run at a time. */
    static final int MAX_IN_FLIGHT = 4;

    static final long TOTAL_VALID_MILLIS = 10 * 60 * 1000L;

    static final int MAX_TOTALS = 1000;

    private record Total(long vectors, long countedAt) {}

    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> search;
    private final LongSupplier clock;
    private final Executor executor;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final Map<String, Total> totals = new ConcurrentHashMap<>();
    private final LongAdder counted = new LongAdder();
    private volatile boolean enabled;

    /**
     * @param search how the counts are made, which decides who they are made as
     * @param clock  milliseconds, monotonic
     * @param executor where the second count is started
     */
    public SelectivityStrata(BiConsumer<SearchRequest, ActionListener<SearchResponse>> search, LongSupplier clock, Executor executor) {
        this.search = search;
        this.clock = clock;
        this.executor = executor;
    }

    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.ESTIMATE_SELECTIVITY, value -> this.enabled = value);
    }

    /**
     * Unfiltered queries are assigned at once; filtered ones when the counts return, off this thread.
     */
    public void assign(TrackedQuery tracked, CapturedQuery query) {
        if (query.filters().isEmpty()) {
            tracked.selectivity(Selectivity.UNFILTERED);
            return;
        }
        if (enabled == false) {
            return;
        }
        if (inFlight.incrementAndGet() > MAX_IN_FLIGHT) {
            inFlight.decrementAndGet();
            return;
        }
        ActionListener<Double> done = ActionListener.runAfter(ActionListener.wrap(fraction -> {
            tracked.selectivity(Selectivity.of(fraction));
            counted.increment();
        }, e -> { logger.warn("failed to count the vectors that the filters of a query leave", e); }), inFlight::decrementAndGet);
        try {
            vectors(query, done.delegateFailureAndWrap((l, total) -> {
                if (total == 0) {
                    l.onFailure(new IllegalStateException("there are no vectors to take a share of"));
                    return;
                }
                // the first count may have been completed by a network thread, which must not start the next search
                executor.execute(
                    ActionRunnable.wrap(
                        l,
                        next -> count(query, filtered(query), next.map(passing -> Math.min(1.0, (double) passing / total)))
                    )
                );
            }));
        } catch (Exception e) {
            done.onFailure(e);
        }
    }

    private void vectors(CapturedQuery query, ActionListener<Long> listener) {
        String key = String.join(",", query.indices()) + "/" + query.field();
        Total known = totals.get(key);
        long now = clock.getAsLong();
        if (known != null && now - known.countedAt() < TOTAL_VALID_MILLIS) {
            listener.onResponse(known.vectors());
            return;
        }
        count(query, QueryBuilders.existsQuery(query.field()), listener.map(vectors -> {
            if (totals.size() >= MAX_TOTALS) {
                totals.clear();
            }
            totals.put(key, new Total(vectors, now));
            return vectors;
        }));
    }

    private static QueryBuilder filtered(CapturedQuery query) {
        BoolQueryBuilder bool = QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery(query.field()));
        query.filters().forEach(bool::filter);
        return bool;
    }

    private void count(CapturedQuery query, QueryBuilder what, ActionListener<Long> listener) {
        SearchRequest request = new SearchRequest(query.indices());
        request.source(new SearchSourceBuilder().size(0).trackTotalHits(true).query(what));
        // a count that leaves out the shards that failed would give a wrong share, and the total is kept for ten minutes
        request.allowPartialSearchResults(false);
        search.accept(request, listener.map(response -> response.getHits().getTotalHits().value()));
    }

    public long counted() {
        return counted.sum();
    }
}
