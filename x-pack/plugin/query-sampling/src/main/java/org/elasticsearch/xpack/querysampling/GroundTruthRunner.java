/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;
import org.elasticsearch.xpack.querysampling.groundtruth.ExactSearch;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Computes ground truth by running exact searches one after the other, since each scans the index. Searches go
 * through the given function so they run as whoever asked.
 * <p>
 * Each search is started on the given executor, as a search may be completed by a network thread, which must not start
 * the next scan. A query whose search fails is left without ground truth and the run goes on with the others: their
 * answers do not depend on it, and the exact search does not allow partial results, so a failed shard fails the query
 * and never leaves an incomplete answer behind.
 */
public final class GroundTruthRunner {

    private static final Logger logger = LogManager.getLogger(GroundTruthRunner.class);

    public record Result(int computed, int failed) {}

    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> search;
    private final Executor executor;

    public GroundTruthRunner(BiConsumer<SearchRequest, ActionListener<SearchResponse>> search, Executor executor) {
        this.search = search;
        this.executor = executor;
    }

    public <T> void run(
        List<T> items,
        Function<T, CapturedQuery> query,
        BiConsumer<T, GroundTruth> store,
        ActionListener<Result> listener
    ) {
        resume(listener, () -> next(items, query, store, 0, 0, 0, listener));
    }

    // never runs the rest of the run on the thread that completed the last search
    private void resume(ActionListener<Result> listener, Runnable next) {
        try {
            executor.execute(next);
        } catch (Exception e) {
            listener.onFailure(e);
        }
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
        assert Transports.assertNotTransportThread("the exact searches of a ground truth run must not be started by a network thread");
        if (index == queries.size()) {
            listener.onResponse(new Result(computed, failed));
            return;
        }
        T query = queries.get(index);
        // a search function may complete the listener and then still throw, only the first outcome counts
        ActionListener<GroundTruth> computedListener = ActionListener.notifyOnce(ActionListener.wrap(groundTruth -> {
            store.accept(query, groundTruth);
            resume(listener, () -> next(queries, queryOf, store, index + 1, computed + 1, failed, listener));
        }, e -> {
            logger.warn("failed to compute the ground truth of a sampled kNN search", e);
            resume(listener, () -> next(queries, queryOf, store, index + 1, computed, failed + 1, listener));
        }));
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
                startScan(query, state, listener);
            }

            @Override
            public void onFailure(Exception e) {
                // without it the ground truth is stored without a state, which counts as out of date for good
                logger.warn("failed to read the state of the data before the exact search of a sampled kNN search", e);
                startScan(query, null, listener);
            }
        });
    }

    // the last probe may have been completed by a network thread, which must not start the scan
    private void startScan(CapturedQuery query, @Nullable DataState state, ActionListener<GroundTruth> listener) {
        try {
            executor.execute(ActionRunnable.wrap(listener, l -> scan(query, state, l)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void scan(CapturedQuery query, @Nullable DataState state, ActionListener<GroundTruth> listener) {
        // a search function may complete the listener and then still throw, only the first outcome counts
        ActionListener<SearchResponse> exactSearchListener = ActionListener.notifyOnce(
            ActionListener.wrap(
                response -> listener.onResponse(new GroundTruth(ExactSearch.parse(response).neighbors(), state)),
                listener::onFailure
            )
        );
        try {
            search.accept(ExactSearch.request(query), exactSearchListener);
        } catch (Exception e) {
            exactSearchListener.onFailure(e);
        }
    }
}
