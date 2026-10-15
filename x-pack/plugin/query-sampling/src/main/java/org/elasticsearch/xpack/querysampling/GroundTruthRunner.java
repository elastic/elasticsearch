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
 * Computes ground truth by running exact searches one after the other, since each scans the index. Searches go
 * through the given function so they run as whoever asked.
 */
public final class GroundTruthRunner {

    private static final Logger logger = LogManager.getLogger(GroundTruthRunner.class);

    public record Result(int computed, int failed) {}

    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> search;

    public GroundTruthRunner(BiConsumer<SearchRequest, ActionListener<SearchResponse>> search) {
        this.search = search;
    }

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

    // the data state is read before the exact search so that a change during it makes the ground truth look stale, not fresh;
    // an unreadable state is stored as null, which also counts as stale
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
