/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank.context;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;

import java.util.Arrays;
import java.util.Comparator;

import static org.elasticsearch.search.SearchService.DEFAULT_FROM;
import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;

/**
 * {@code RankFeaturePhaseRankCoordinatorContext} is a base class that runs on the coordinating node and is responsible for retrieving
 * {@code rank_window_size} total results from all shards, rank them, and then produce a final paginated response of [from, from+size]
 * results.
 */
public abstract class RankFeaturePhaseRankCoordinatorContext {

    protected final int size;
    protected final int from;
    protected final int rankWindowSize;
    protected final boolean failuresAllowed;

    public RankFeaturePhaseRankCoordinatorContext(int size, int from, int rankWindowSize, boolean failuresAllowed) {
        this.size = size < 0 ? DEFAULT_SIZE : size;
        this.from = from < 0 ? DEFAULT_FROM : from;
        this.rankWindowSize = rankWindowSize;
        this.failuresAllowed = failuresAllowed;
    }

    public boolean failuresAllowed() {
        return failuresAllowed;
    }

    /**
     * Computes the updated scores for a list of features (i.e. document-based data). We also pass along an ActionListener
     * that should be called with the new scores, and will continue execution to the next phase
     */
    protected abstract void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener);

    /**
     * Preprocesses the provided documents: sorts them by score descending.
     *
     * @param originalDocs   documents to process
     * @param rerankedScores {@code true} if the document scores have been reranked
     */
    protected RankFeatureDoc[] preprocess(RankFeatureDoc[] originalDocs, boolean rerankedScores) {
        Arrays.sort(originalDocs, Comparator.comparing((RankFeatureDoc doc) -> doc.score).reversed());
        return originalDocs;
    }

    /**
     * This method is responsible for ranking the global results based on the provided rank feature results from each shard.
     * <p>
     * We first start by extracting ordered feature data through a {@code List<RankFeatureDoc>}
     * from the provided rankSearchResults, and then compute the updated score for each of the documents.
     * Once all the scores have been computed, we sort the results, perform any pagination needed, and then call the `onFinish` consumer
     * with the final array of {@link ScoreDoc} results.
     *
     * @param featureDocs       an array of rank feature results from each shard
     * @param rankListener      a rankListener to handle the global ranking result
     */
    public void computeRankScoresForGlobalResults(RankFeatureDoc[] featureDocs, ActionListener<RankFeatureDoc[]> rankListener) {
        // generate the final `topResults` results, and pass them to fetch phase through the `rankListener`
        computeScores(featureDocs, rankListener.delegateFailureAndWrap((listener, scores) -> {
            for (int i = 0; i < featureDocs.length; i++) {
                featureDocs[i].score = scores[i];
            }
            listener.onResponse(featureDocs);
        }));
    }

    /**
     * Ranks the provided {@link RankFeatureDoc} array and paginates the results based on the `from` and `size` parameters. Filters out
     * documents that have a relevance score less than min_score.
     *
     * @param rankFeatureDocs documents to process
     * @param rerankedScores {@code true} if the document scores have been reranked
     */
    public RankFeatureDoc[] rankAndPaginate(RankFeatureDoc[] rankFeatureDocs, boolean rerankedScores) {
        RankFeatureDoc[] sortedDocs = preprocess(rankFeatureDocs, rerankedScores);
        RankFeatureDoc[] topResults = new RankFeatureDoc[Math.max(0, Math.min(size, sortedDocs.length - from))];
        for (int rank = 0; rank < topResults.length; ++rank) {
            topResults[rank] = sortedDocs[from + rank];
            topResults[rank].rank = from + rank + 1;
        }
        return topResults;
    }
}
