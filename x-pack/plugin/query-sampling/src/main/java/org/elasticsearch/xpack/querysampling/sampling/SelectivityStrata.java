/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.action.ActionListener;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Tells how much of the vectors a query with filters is allowed to return, which is the stratum of {@link Selectivity}:
 * the number of vectors that the filters leave over the number of the vectors there are.
 * <p>
 * Nothing in the search says it, so it is counted, by two searches that return no hits: of the vectors that pass the filters
 * and of all the vectors, the second of which is kept for a while as it is the same for every query of the indices. This
 * is done once for each distinct query, in the background and not more than a few at a time, which have to be paid for, so
 * it is only done if the setting asks for it. A query that is not counted, because there are too many waiting or a count
 * failed, has no selectivity, and is only left out of what is estimated by it.
 * <p>
 * Unlike the strata of the vector space and the hardness it does not change the probability of a query to be picked: it
 * is there to see where the recall is low, and not to choose the sample.
 */
public final class SelectivityStrata {

    private static final Logger logger = LogManager.getLogger(SelectivityStrata.class);

    /**
     * The most counts that are going on at a time, as they are searches that nobody asked for.
     */
    static final int MAX_IN_FLIGHT = 4;

    /**
     * For how long the number of the vectors of some indices is believed.
     */
    static final long TOTAL_VALID_MILLIS = 10 * 60 * 1000L;

    /**
     * The most totals that are kept, which is one for each field of some indices.
     */
    static final int MAX_TOTALS = 1000;

    private record Total(long vectors, long countedAt) {}

    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> search;
    private final LongSupplier clock;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final Map<String, Total> totals = new ConcurrentHashMap<>();
    private final LongAdder counted = new LongAdder();
    private final LongAdder skipped = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private volatile boolean enabled;

    /**
     * @param search how the counts are made, which decides who they are made as
     * @param clock  milliseconds, from a clock that only has to go on
     */
    public SelectivityStrata(BiConsumer<SearchRequest, ActionListener<SearchResponse>> search, LongSupplier clock) {
        this.search = search;
        this.clock = clock;
    }

    /**
     * A stratum that never counts, for when there is nothing to count with.
     */
    public static SelectivityStrata none() {
        return new SelectivityStrata((request, listener) -> listener.onFailure(new UnsupportedOperationException()), () -> 0L);
    }

    /**
     * Follows the setting that says whether to count, now and when it changes.
     */
    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.ESTIMATE_SELECTIVITY, value -> this.enabled = value);
    }

    /**
     * Tells the selectivity of a query that has just been seen for the first time. It is known at once if there are no
     * filters, and later, when the counts are back, if there are, which is not on this thread.
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
            skipped.increment();
            return;
        }
        ActionListener<Double> done = ActionListener.runAfter(ActionListener.wrap(fraction -> {
            tracked.selectivity(Selectivity.of(fraction));
            counted.increment();
        }, e -> {
            failed.increment();
            logger.debug("failed to count the vectors that the filters of a query leave", e);
        }), inFlight::decrementAndGet);
        try {
            vectors(query, done.delegateFailureAndWrap((l, total) -> {
                if (total == 0) {
                    l.onFailure(new IllegalStateException("there are no vectors to take a share of"));
                    return;
                }
                count(query, filtered(query), l.map(passing -> Math.min(1.0, (double) passing / total)));
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

    /**
     * What the filters leave, among the documents that have the vector at all.
     */
    private static QueryBuilder filtered(CapturedQuery query) {
        BoolQueryBuilder bool = QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery(query.field()));
        query.filters().forEach(bool::filter);
        return bool;
    }

    private void count(CapturedQuery query, QueryBuilder what, ActionListener<Long> listener) {
        SearchRequest request = new SearchRequest(query.indices());
        request.source(new SearchSourceBuilder().size(0).trackTotalHits(true).query(what));
        search.accept(request, listener.map(response -> response.getHits().getTotalHits().value()));
    }

    /**
     * Queries whose selectivity was counted.
     */
    public long counted() {
        return counted.sum();
    }

    /**
     * Queries that were not counted because there were too many counts going on.
     */
    public long skipped() {
        return skipped.sum();
    }

    /**
     * Queries whose count failed.
     */
    public long failed() {
        return failed.sum();
    }
}
