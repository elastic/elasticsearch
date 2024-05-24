/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rerank;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class RerankingRankFeaturePhaseRankCoordinatorContext extends RankFeaturePhaseRankCoordinatorContext {

    public RerankingRankFeaturePhaseRankCoordinatorContext(int size, int from, int rankWindowSize) {
        super(size, from, rankWindowSize);
    }

    /**
     * This method is responsible for computing the updated scores for a list of feature data, and call the {@code onFinish} once done.
     */
    protected abstract void computeScores(RankFeatureDoc[] featureDocs, BiConsumer<Integer, Float> scoreConsumer, Runnable onFinish);

    @Override
    public void rankGlobalResults(List<RankFeatureResult> phaseResultsPerShard, Consumer<ScoreDoc[]> onFinish) {
        // read feature data from all shards
        RankFeatureDoc[] featureDocs = extractFeatures(phaseResultsPerShard);
        // create a consumer to in-place update doc scores based on the computeScores method
        final BiConsumer<Integer, Float> scoreConsumer = (index, score) -> {
            assert index >= 0 && index < featureDocs.length;
            featureDocs[index].score = score;
        };
        computeScores(featureDocs, scoreConsumer, () -> {
            // filter docs marked for discarding due to low relevance score
            RankFeatureDoc[] filteredFeatureDocs = Arrays.stream(featureDocs)
                .filter((doc) -> doc.score > 0.0f)
                .toArray(RankFeatureDoc[]::new);

            Arrays.sort(filteredFeatureDocs, Comparator.comparing((RankFeatureDoc doc) -> doc.score).reversed());
            RankFeatureDoc[] topResults = new RankFeatureDoc[Math.max(0, Math.min(size, filteredFeatureDocs.length - from))];
            for (int rank = 0; rank < topResults.length; ++rank) {
                topResults[rank] = filteredFeatureDocs[from + rank];
                topResults[rank].rank = from + rank + 1;
            }
            onFinish.accept(topResults);
        });
    }

    private RankFeatureDoc[] extractFeatures(List<RankFeatureResult> searchPhaseResults) {
        List<RankFeatureDoc> docFeatures = new ArrayList<>();
        for (SearchPhaseResult searchPhaseResult : searchPhaseResults) {
            RankFeatureShardResult shardResult = searchPhaseResult.rankFeatureResult().shardResult();
            docFeatures.addAll(Arrays.stream(shardResult.rankFeatureDocs).toList());
        }
        return docFeatures.toArray(new RankFeatureDoc[0]);
    }
}
