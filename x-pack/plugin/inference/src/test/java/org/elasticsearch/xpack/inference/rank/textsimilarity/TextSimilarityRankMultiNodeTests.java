/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.rerank.AbstractRerankerIT;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.util.Collection;
import java.util.List;

public class TextSimilarityRankMultiNodeTests extends AbstractRerankerIT {

    private static final String inferenceId = "inference-id";
    private static final String inferenceText = "inference-text";
    private static final float minScore = 0.0f;

    @Override
    protected RankBuilder getRankBuilder(int rankWindowSize, String rankFeatureField) {
        return new TextSimilarityRankBuilder(rankFeatureField, inferenceId, inferenceText, rankWindowSize, minScore);
    }

    @Override
    protected RankBuilder getThrowingRankBuilder(int rankWindowSize, String rankFeatureField, ThrowingRankBuilderType type) {
        return new TextSimilarityTestPlugin.ThrowingMockRequestActionBasedRankBuilder(
            rankWindowSize,
            rankFeatureField,
            inferenceId,
            inferenceText,
            minScore,
            type.name()
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> pluginsNeeded() {
        return List.of(InferencePlugin.class, TextSimilarityTestPlugin.class);
    }

    public void testQueryPhaseShardThrowingAllShardsFail() throws Exception {
        // no-op
    }

    public void testQueryPhaseCoordinatorThrowingAllShardsFail() throws Exception {
        // no-op
    }
}
