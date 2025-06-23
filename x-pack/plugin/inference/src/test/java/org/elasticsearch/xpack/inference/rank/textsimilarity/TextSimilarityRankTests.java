/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
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
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasRank;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasScore;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TextSimilarityRankTests extends ESSingleNodeTestCase {

    /**
     * {@code TextSimilarityRankBuilder} that sets top_n in the inference endpoint's task settings.
     * See {@code TextSimilarityTestPlugin -> TestFilter -> handleGetInferenceModelActionRequest} for the logic that extracts the top_n
     * value.
     */
    public static class TopNConfigurationAcceptingTextSimilarityRankBuilder extends TextSimilarityRankBuilder {

        public TopNConfigurationAcceptingTextSimilarityRankBuilder(
            String field,
            String inferenceId,
            String inferenceText,
            int rankWindowSize,
            Float minScore,
            int topN
        ) {
            super(field, inferenceId + "-task-settings-top-" + topN, inferenceText, rankWindowSize, minScore, false);
        }
    }

    /**
     * {@code TextSimilarityRankBuilder} that simulates an inference call returning N results.
     */
    public static class InferenceResultCountAcceptingTextSimilarityRankBuilder extends TextSimilarityRankBuilder {

        private final int inferenceResultCount;

        public InferenceResultCountAcceptingTextSimilarityRankBuilder(
            String field,
            String inferenceId,
            String inferenceText,
            int rankWindowSize,
            Float minScore,
            int inferenceResultCount
        ) {
            super(field, inferenceId, inferenceText, rankWindowSize, minScore, false);
            this.inferenceResultCount = inferenceResultCount;
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
                minScore,
                failuresAllowed()
            ) {
                @Override
                protected InferenceAction.Request generateRequest(List<String> docFeatures) {
                    return new InferenceAction.Request(
                        TaskType.RERANK,
                        this.inferenceId,
                        inferenceText,
                        null,
                        null,
                        docFeatures,
                        Map.of("inferenceResultCount", inferenceResultCount),
                        InputType.INTERNAL_SEARCH,
                        InferenceAction.Request.DEFAULT_TIMEOUT,
                        false
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
        return List.of(LocalStateInferencePlugin.class, TextSimilarityTestPlugin.class);
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
                .setRankBuilder(new TextSimilarityRankBuilder("text", "my-rerank-model", "my query", 100, 0.0f, false))
                .setQuery(QueryBuilders.matchAllQuery()),
            response -> {
                // Verify order, rank and score of results
                assertThat(
                    response.getHits().getHits(),
                    arrayContaining(
                        // add 1 to all expected scores due to the default normalization being applied which shifts positive scores by 1
                        searchHitWith(1, 4.0f + 1f, "4"),
                        searchHitWith(2, 3.0f + 1f, "3"),
                        searchHitWith(3, 2.0f + 1f, "2"),
                        searchHitWith(4, 1.0f + 1f, "1"),
                        searchHitWith(5, 0.0f + 1f, "0")
                    )
                );
            }
        );
    }

    public void testRerankWithMinScore() {
        ElasticsearchAssertions.assertNoFailuresAndResponse(
            // Execute search with text similarity reranking
            client.prepareSearch()
                .setRankBuilder(new TextSimilarityRankBuilder("text", "my-rerank-model", "my query", 100, 1.5f, false))
                .setQuery(QueryBuilders.matchAllQuery()),
            response -> {
                // Verify order, rank and score of results
                assertThat(
                    response.getHits().getHits(),
                    arrayContaining(searchHitWith(1, 4.0f + 1f, "4"), searchHitWith(2, 3.0f + 1f, "3"), searchHitWith(3, 2.0f + 1f, "2"))
                );
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
                        false,
                        AbstractRerankerIT.ThrowingRankBuilderType.THROWING_RANK_FEATURE_PHASE_COORDINATOR_CONTEXT.name()
                    )
                )
                .setQuery(QueryBuilders.matchAllQuery()),
            RestStatus.INTERNAL_SERVER_ERROR,
            containsString("Failed to execute phase [rank-feature], Computing updated ranks for results failed")
        );
    }

    public void testRerankInferenceAllowedFailure() {
        ElasticsearchAssertions.assertNoFailuresAndResponse(
            // Execute search with text similarity reranking that fails, but it is allowed
            client.prepareSearch()
                .setRankBuilder(
                    new TextSimilarityTestPlugin.ThrowingMockRequestActionBasedRankBuilder(
                        100,
                        "text",
                        "my-rerank-model",
                        "my query",
                        null,
                        true,
                        AbstractRerankerIT.ThrowingRankBuilderType.THROWING_RANK_FEATURE_PHASE_COORDINATOR_CONTEXT.name()
                    )
                )
                .setQuery(
                    boolQuery().should(constantScoreQuery(matchQuery("text", "0")).boost(50))
                        .should(constantScoreQuery(matchQuery("text", "1")).boost(40))
                        .should(constantScoreQuery(matchQuery("text", "2")).boost(30))
                        .should(constantScoreQuery(matchQuery("text", "3")).boost(20))
                        .should(constantScoreQuery(matchQuery("text", "4")).boost(10))
                ),
            response -> {
                // these will all have the scores from the constant score clauses
                assertThat(
                    response.getHits().getHits(),
                    arrayContaining(
                        searchHitWith(1, 50, "0"),
                        searchHitWith(2, 40, "1"),
                        searchHitWith(3, 30, "2"),
                        searchHitWith(4, 20, "3"),
                        searchHitWith(5, 10, "4")
                    )
                );
            }
        );
    }

    public void testRerankTopNConfigurationAndRankWindowSizeMismatch() {
        SearchPhaseExecutionException ex = expectThrows(
            SearchPhaseExecutionException.class,
            // Execute search with text similarity reranking
            client.prepareSearch()
                .setRankBuilder(
                    // Simulate reranker configuration with top_n=3 in task_settings, which is different from rank_window_size=10
                    // (Note: top_n comes from inferenceId, there's no other easy way of passing this to the mocked get model request)
                    new TopNConfigurationAcceptingTextSimilarityRankBuilder("text", "my-rerank-model", "my query", 100, 1.5f, 3)
                )
                .setQuery(QueryBuilders.matchAllQuery())
        );
        assertThat(ex.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            ex.getDetailedMessage(),
            containsString("Reduce rank_window_size to be less than or equal to the configured top N value")
        );
    }

    public void testRerankInputSizeAndInferenceResultsMismatch() {
        SearchPhaseExecutionException ex = expectThrows(
            SearchPhaseExecutionException.class,
            // Execute search with text similarity reranking
            client.prepareSearch()
                .setRankBuilder(
                    // Simulate reranker returning different number of results from input
                    new InferenceResultCountAcceptingTextSimilarityRankBuilder("text", "my-rerank-model", "my query", 100, 1.5f, 4)
                )
                .setQuery(QueryBuilders.matchAllQuery())
        );
        assertThat(ex.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(ex.getDetailedMessage(), containsString("Reranker input document count and returned score count mismatch"));
    }

    private static Matcher<SearchHit> searchHitWith(int expectedRank, float expectedScore, String expectedText) {
        return allOf(
            hasRank(expectedRank),
            hasScore(expectedScore),
            transformedMatch(hit -> hit.getSourceAsMap().get("text"), equalTo(expectedText))
        );
    }
}
