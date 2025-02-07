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
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;

public class TextSimilarityRankMultiNodeTests extends AbstractRerankerIT {

    private static final String inferenceId = "inference-id";
    private static final String inferenceText = "inference-text";
    private static final float minScore = 0.0f;

    @Override
    protected RankBuilder getRankBuilder(int rankWindowSize, String rankFeatureField) {
        return new TextSimilarityRankBuilder(rankFeatureField, inferenceId, inferenceText, rankWindowSize, minScore, false);
    }

    @Override
    protected RankBuilder getThrowingRankBuilder(int rankWindowSize, String rankFeatureField, ThrowingRankBuilderType type) {
        return getThrowingRankBuilder(rankWindowSize, rankFeatureField, type, false);
    }

    protected RankBuilder getThrowingRankBuilder(
        int rankWindowSize,
        String rankFeatureField,
        ThrowingRankBuilderType type,
        boolean failuresAllowed
    ) {
        return new TextSimilarityTestPlugin.ThrowingMockRequestActionBasedRankBuilder(
            rankWindowSize,
            rankFeatureField,
            inferenceId,
            inferenceText,
            minScore,
            failuresAllowed,
            type.name()
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> pluginsNeeded() {
        return List.of(LocalStateInferencePlugin.class, TextSimilarityTestPlugin.class);
    }

    public void testQueryPhaseShardThrowingAllShardsFail() throws Exception {
        // no-op
    }

    public void testQueryPhaseCoordinatorThrowingAllShardsFail() throws Exception {
        // no-op
    }

    public void testRerankerAllowedFailureNoExceptions() throws Exception {
        final String indexName = "test_index";
        final String rankFeatureField = "rankFeatureField";
        final String searchField = "searchField";
        final int rankWindowSize = 10;

        createIndex(indexName);
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource(rankFeatureField, 0.1, searchField, "A"),
            prepareIndex(indexName).setId("2").setSource(rankFeatureField, 0.2, searchField, "B"),
            prepareIndex(indexName).setId("3").setSource(rankFeatureField, 0.3, searchField, "C"),
            prepareIndex(indexName).setId("4").setSource(rankFeatureField, 0.4, searchField, "D"),
            prepareIndex(indexName).setId("5").setSource(rankFeatureField, 0.5, searchField, "E")
        );

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(
                    boolQuery().should(matchQuery(searchField, "A"))
                        .should(matchQuery(searchField, "B"))
                        .should(matchQuery(searchField, "C"))
                        .should(matchQuery(searchField, "D"))
                        .should(matchQuery(searchField, "E"))
                )
                .setRankBuilder(
                    getThrowingRankBuilder(
                        rankWindowSize,
                        rankFeatureField,
                        ThrowingRankBuilderType.THROWING_RANK_FEATURE_PHASE_COORDINATOR_CONTEXT,
                        true
                    )
                )
                .addFetchField(searchField)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(true)
                .setSize(10),
            response -> {
                // just check it returns 5 documents, the order will be random due to not getting reranked
                assertHitCount(response, 5L);
            }
        );
        assertNoOpenContext(indexName);
    }

    @Override
    protected boolean shouldCheckScores() {
        return false;
    }
}
