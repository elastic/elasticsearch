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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.querysampling.groundtruth.ExactSearch;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;
import org.elasticsearch.xpack.querysampling.storage.SampledQuery;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

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

    public void run(List<SampledQuery> queries, ActionListener<Result> listener) {
        next(queries, 0, 0, 0, listener);
    }

    private void next(List<SampledQuery> queries, int index, int computed, int failed, ActionListener<Result> listener) {
        if (index == queries.size()) {
            listener.onResponse(new Result(computed, failed));
            return;
        }
        SampledQuery query = queries.get(index);
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
                if (completed.compareAndSet(false, true) == false) {
                    return;
                }
                query.groundTruth(groundTruth);
                next(queries, index + 1, computed + 1, failed, listener);
            }

            @Override
            public void onFailure(Exception e) {
                if (completed.compareAndSet(false, true) == false) {
                    return;
                }
                logger.debug("failed to compute the ground truth of a sampled kNN search", e);
                next(queries, index + 1, computed, failed + 1, listener);
            }
        };
        try {
            search.accept(ExactSearch.request(query.search().query()), exactSearchListener);
        } catch (Exception e) {
            exactSearchListener.onFailure(e);
        }
    }
}
