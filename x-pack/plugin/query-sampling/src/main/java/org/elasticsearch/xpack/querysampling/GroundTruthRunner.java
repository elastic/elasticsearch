/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;
import org.elasticsearch.xpack.querysampling.groundtruth.ExactSearch;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Computes the ground truth of sampled queries by running their exact search. Each exact search scans the
 * index, so they are run one after the other: how much work is done at a time is bounded by how many
 * queries the caller hands over, not by how fast searches complete.
 * <p>
 * Searches are issued through the given function so that they run with the identity of whoever asked for
 * the computation.
 */
public final class GroundTruthRunner {

    private static final Logger logger = LogManager.getLogger(GroundTruthRunner.class);

    /**
     * @param computed queries that got their ground truth
     * @param failed   queries whose exact search failed; they stay without ground truth
     */
    public record Result(int computed, int failed) {}

    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> search;

    public GroundTruthRunner(BiConsumer<SearchRequest, ActionListener<SearchResponse>> search) {
        this.search = search;
    }

    /**
     * Computes the ground truth of whatever holds a query, so that the samples are not tied to where they live.
     *
     * @param query how to get the query of an item
     * @param store what to do with the ground truth of an item, called once for each one that was computed
     */
    public <T> void run(
        List<T> items,
        Function<T, CapturedQuery> query,
        BiConsumer<T, GroundTruth> store,
        ActionListener<Result> listener
    ) {
        next(items, query, store, 0, 0, 0, listener);
    }

    private <T> void next(
        List<T> queries,
        Function<T, CapturedQuery> queryOf,
        BiConsumer<T, GroundTruth> store,
        int index,
        int computed,
        int failed,
        ActionListener<Result> listener
    ) {
        if (index == queries.size()) {
            listener.onResponse(new Result(computed, failed));
            return;
        }
        T query = queries.get(index);
        ActionListener<GroundTruth> computedListener = new ActionListener<>() {
            // a search function may complete the listener and then still throw, only the first outcome counts
            private final AtomicBoolean completed = new AtomicBoolean();

            @Override
            public void onResponse(GroundTruth groundTruth) {
                if (completed.compareAndSet(false, true) == false) {
                    return;
                }
                store.accept(query, groundTruth);
                next(queries, queryOf, store, index + 1, computed + 1, failed, listener);
            }

            @Override
            public void onFailure(Exception e) {
                if (completed.compareAndSet(false, true) == false) {
                    return;
                }
                logger.debug("failed to compute the ground truth of a sampled kNN search", e);
                next(queries, queryOf, store, index + 1, computed, failed + 1, listener);
            }
        };
        try {
            compute(queryOf.apply(query), computedListener);
        } catch (Exception e) {
            computedListener.onFailure(e);
        }
    }

    /**
     * The state of the data is read before the exact search, so that if the data changes while it runs the ground truth looks
     * out of date later, and not up to date. If it cannot be read the ground truth is left without one, which is as out of date.
     */
    private void compute(CapturedQuery query, ActionListener<GroundTruth> listener) {
        DataState.read(search, query, new ActionListener<>() {
            @Override
            public void onResponse(DataState state) {
                scan(query, state, listener);
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("failed to read the state of the data before the exact search of a sampled kNN search", e);
                scan(query, null, listener);
            }
        });
    }

    private void scan(CapturedQuery query, @Nullable DataState state, ActionListener<GroundTruth> listener) {
        ActionListener<SearchResponse> exactSearchListener = new ActionListener<>() {
            // a search function may complete the listener and then still throw, only the first outcome counts
            private final AtomicBoolean completed = new AtomicBoolean();

            @Override
            public void onResponse(SearchResponse response) {
                GroundTruth groundTruth;
                try {
                    groundTruth = ExactSearch.parse(response);
                } catch (Exception e) {
                    onFailure(e);
                    return;
                }
                if (completed.compareAndSet(false, true)) {
                    listener.onResponse(new GroundTruth(groundTruth.neighbors(), state));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (completed.compareAndSet(false, true)) {
                    listener.onFailure(e);
                }
            }
        };
        try {
            search.accept(ExactSearch.request(query), exactSearchListener);
        } catch (Exception e) {
            exactSearchListener.onFailure(e);
        }
    }
}
