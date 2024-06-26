/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;

public class TextSimilarityRankTests extends ESSingleNodeTestCase {

    public static class TestPlugin extends Plugin implements ActionPlugin {

        private final SetOnce<TestFilter> testFilter = new SetOnce<>();

        @Override
        public Collection<?> createComponents(PluginServices services) {
            testFilter.set(new TestFilter());
            return Collections.emptyList();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(testFilter.get());
        }
    }

    /**
     * Action filter that captures the inference action and injects a mock response.
     */
    static class TestFilter implements ActionFilter {

        enum TestFilterRunMode {
            NORMAL, // No error
            INFERENCE_FAILURE, // Simulate failure at inference call
            INFERENCE_RESULT_MISMATCH // Simulate inference call that returns a different number of items than its input
        }

        static TestFilterRunMode runMode = TestFilterRunMode.NORMAL;

        /**
         * Mock response of rerank inference call.
         */
        private static final List<RankedDocsResults.RankedDoc> RANKED_DOCS = List.of(
            new RankedDocsResults.RankedDoc(3, 0.9f, ""),
            new RankedDocsResults.RankedDoc(0, 0.8f, ""),
            new RankedDocsResults.RankedDoc(2, 0.7f, ""),
            new RankedDocsResults.RankedDoc(1, 0.6f, ""),
            new RankedDocsResults.RankedDoc(4, 0.5f, "")
        );

        @Override
        public int order() {
            return Integer.MIN_VALUE;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void apply(
            Task task,
            String action,
            Request request,
            ActionListener<Response> listener,
            ActionFilterChain<Request, Response> chain
        ) {
            // For any other action than inference, execute normally
            if (action.equals(InferenceAction.INSTANCE.name()) == false) {
                chain.proceed(task, action, request, listener);
                return;
            }

            // For inference action respond with rerank results or failure, depending on run mode
            if (runMode == TestFilterRunMode.INFERENCE_FAILURE) {
                listener.onFailure(new ElasticsearchException("rerank inference call failed"));
            } else {
                List<RankedDocsResults.RankedDoc> rankedDocsResults = runMode == TestFilterRunMode.INFERENCE_RESULT_MISMATCH
                    ? RANKED_DOCS.subList(0, RANKED_DOCS.size() - 1)
                    : RANKED_DOCS;
                ActionResponse response = new InferenceAction.Response(new RankedDocsResults(rankedDocsResults));
                listener.onResponse((Response) response);
            }
        }
    }

    private Client client;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(InferencePlugin.class, TestPlugin.class);
    }

    @Before
    public void setup() {
        TestFilter.runMode = TestFilter.TestFilterRunMode.NORMAL;

        // Initialize index with a few documents
        client = client();
        for (int i = 0; i < 5; i++) {
            client.prepareIndex("my-index").setSource(Collections.singletonMap("text", "text " + i)).get();
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
                assertHitHasRankScoreAndText(hits[0], 1, 0.9f, "text 3");
                assertHitHasRankScoreAndText(hits[1], 2, 0.8f, "text 0");
                assertHitHasRankScoreAndText(hits[2], 3, 0.7f, "text 2");
                assertHitHasRankScoreAndText(hits[3], 4, 0.6f, "text 1");
                assertHitHasRankScoreAndText(hits[4], 5, 0.5f, "text 4");
            }
        );
    }

    public void testRerankWithMinScore() {
        ElasticsearchAssertions.assertNoFailuresAndResponse(
            // Execute search with text similarity reranking
            client.prepareSearch()
                .setRankBuilder(new TextSimilarityRankBuilder("text", "my-rerank-model", "my query", 100, 0.7f))
                .setQuery(QueryBuilders.matchAllQuery()),
            response -> {
                // Verify order, rank and score of results
                SearchHit[] hits = response.getHits().getHits();
                assertEquals(3, hits.length);
                assertHitHasRankScoreAndText(hits[0], 1, 0.9f, "text 3");
                assertHitHasRankScoreAndText(hits[1], 2, 0.8f, "text 0");
                assertHitHasRankScoreAndText(hits[2], 3, 0.7f, "text 2");
            }
        );
    }

    public void testRerankInferenceFailure() {
        testAndExpectRankFeaturePhaseFailure(TestFilter.TestFilterRunMode.INFERENCE_FAILURE);
    }

    public void testRerankInferenceResultMismatch() {
        testAndExpectRankFeaturePhaseFailure(TestFilter.TestFilterRunMode.INFERENCE_RESULT_MISMATCH);
    }

    private static void assertHitHasRankScoreAndText(SearchHit hit, int expectedRank, float expectedScore, String expectedText) {
        assertEquals(expectedRank, hit.getRank());
        assertEquals(expectedScore, hit.getScore(), 0.0f);
        assertEquals(expectedText, Objects.requireNonNull(hit.getSourceAsMap()).get("text"));
    }

    private void testAndExpectRankFeaturePhaseFailure(TestFilter.TestFilterRunMode runMode) {
        TestFilter.runMode = runMode;

        ElasticsearchAssertions.assertFailures(
            // Execute search with text similarity reranking
            client.prepareSearch()
                .setRankBuilder(new TextSimilarityRankBuilder("text", "my-rerank-model", "my query", 100, 0.7f))
                .setQuery(QueryBuilders.matchAllQuery()),
            RestStatus.INTERNAL_SERVER_ERROR,
            containsString("Failed to execute phase [rank-feature], Computing updated ranks for results failed")
        );
    }


}
