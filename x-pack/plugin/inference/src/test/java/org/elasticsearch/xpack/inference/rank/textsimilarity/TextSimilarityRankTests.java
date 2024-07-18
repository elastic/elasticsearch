/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.rerank.AbstractRerankerIT;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.containsString;

public class TextSimilarityRankTests extends ESSingleNodeTestCase {

    /**
     * {@code TextSimilarityRankBuilder} that simulates an inference call that returns a different number of results as the input.
     */
    public static class InvalidInferenceResultCountProvidingTextSimilarityRankBuilder extends TextSimilarityRankBuilder {

        public InvalidInferenceResultCountProvidingTextSimilarityRankBuilder(
            String field,
            String inferenceId,
            String inferenceText,
            int rankWindowSize,
            Float minScore
        ) {
            super(field, inferenceId, inferenceText, rankWindowSize, minScore);
        }

        @Override
        public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
            return new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
                size,
                from,
                rankWindowSize(),
                client,
                inferenceId,
                inferenceText,
                minScore
            ) {
                @Override
                protected InferenceAction.Request generateRequest(List<String> docFeatures) {
                    return new InferenceAction.Request(
                        TaskType.RERANK,
                        inferenceId,
                        inferenceText,
                        docFeatures,
                        Map.of("invalidInferenceResultCount", true),
                        InputType.SEARCH,
                        InferenceAction.Request.DEFAULT_TIMEOUT
                    );
                }
            };
        }
    }

    private static final String inferenceId = "inference-id";
    private static final String inferenceText = "inference-text";
    private static final float minScore = 0.0f;

    private Client client;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(InferencePlugin.class, TextSimilarityTestPlugin.class);
    }

    @Before
    public void setup() {
        // Initialize index with a few documents
        client = client();
        for (int i = 0; i < 5; i++) {
            client.prepareIndex("my-index").setId(String.valueOf(i)).setSource(Collections.singletonMap("text", String.valueOf(i))).get();
        }
        client.admin().indices().prepareRefresh("my-index").get();
    }

    public void testRerank() {
        ElasticsearchAssertions.assertNoFailuresAndResponse(
            // Execute search with text similarity reranking
            client.prepareSearch()
                .setRankBuilder(new TextSimilarityRankBuilder("text", "my-rerank-model", "my query", 100, 0.0f))
                .setQuery(QueryBuilders.matchAllQuery()),
            response -> {
                // Verify order, rank and score of results
                SearchHit[] hits = response.getHits().getHits();
                assertEquals(5, hits.length);
                assertHitHasRankScoreAndText(hits[0], 1, 4.0f, "4");
                assertHitHasRankScoreAndText(hits[1], 2, 3.0f, "3");
                assertHitHasRankScoreAndText(hits[2], 3, 2.0f, "2");
                assertHitHasRankScoreAndText(hits[3], 4, 1.0f, "1");
                assertHitHasRankScoreAndText(hits[4], 5, 0.0f, "0");
            }
        );
    }

    public void testRerankWithMinScore() {
        ElasticsearchAssertions.assertNoFailuresAndResponse(
            // Execute search with text similarity reranking
            client.prepareSearch()
                .setRankBuilder(new TextSimilarityRankBuilder("text", "my-rerank-model", "my query", 100, 1.5f))
                .setQuery(QueryBuilders.matchAllQuery()),
            response -> {
                // Verify order, rank and score of results
                SearchHit[] hits = response.getHits().getHits();
                assertEquals(3, hits.length);
                assertHitHasRankScoreAndText(hits[0], 1, 4.0f, "4");
                assertHitHasRankScoreAndText(hits[1], 2, 3.0f, "3");
                assertHitHasRankScoreAndText(hits[2], 3, 2.0f, "2");
            }
        );
    }

    public void testRerankInferenceFailure() {
        ElasticsearchAssertions.assertFailures(
            // Execute search with text similarity reranking
            client.prepareSearch()
                .setRankBuilder(
                    new TextSimilarityTestPlugin.ThrowingMockRequestActionBasedRankBuilder(
                        100,
                        "text",
                        "my-rerank-model",
                        "my query",
                        0.7f,
                        AbstractRerankerIT.ThrowingRankBuilderType.THROWING_RANK_FEATURE_PHASE_COORDINATOR_CONTEXT.name()
                    )
                )
                .setQuery(QueryBuilders.matchAllQuery()),
            RestStatus.INTERNAL_SERVER_ERROR,
            containsString("Failed to execute phase [rank-feature], Computing updated ranks for results failed")
        );
    }

    public void testRerankInferenceResultMismatch() {
        ElasticsearchAssertions.assertFailures(
            // Execute search with text similarity reranking
            client.prepareSearch()
                .setRankBuilder(
                    new InvalidInferenceResultCountProvidingTextSimilarityRankBuilder("text", "my-rerank-model", "my query", 100, 1.5f)
                )
                .setQuery(QueryBuilders.matchAllQuery()),
            RestStatus.INTERNAL_SERVER_ERROR,
            containsString("Failed to execute phase [rank-feature], Computing updated ranks for results failed")
        );
    }

    private static void assertHitHasRankScoreAndText(SearchHit hit, int expectedRank, float expectedScore, String expectedText) {
        assertEquals(expectedRank, hit.getRank());
        assertEquals(expectedScore, hit.getScore(), 0.0f);
        assertEquals(expectedText, Objects.requireNonNull(hit.getSourceAsMap()).get("text"));
    }

}
