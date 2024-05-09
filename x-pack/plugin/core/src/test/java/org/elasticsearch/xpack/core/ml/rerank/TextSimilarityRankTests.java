/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.rerank;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class TextSimilarityRankTests extends ESSingleNodeTestCase {

    public static class TestPlugin extends Plugin implements ActionPlugin {

        private final SetOnce<TextSimilarityRankTests.TestFilter> testFilter = new SetOnce<>();

        @Override
        public Collection<?> createComponents(PluginServices services) {
            testFilter.set(new TextSimilarityRankTests.TestFilter());
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

        /**
         * Mock response of rerank inference call.
         */
        private static final List<RankedDocsResults.RankedDoc> RANKED_DOCS = List.of(
            new RankedDocsResults.RankedDoc(3, 0.9f, ""),
            new RankedDocsResults.RankedDoc(0, 0.8f, ""),
            new RankedDocsResults.RankedDoc(2, 0.7f, ""),
            new RankedDocsResults.RankedDoc(1, 0.6f, ""),
            new RankedDocsResults.RankedDoc(4, 0.5f, ""));

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

            // Discard the result of the inference action and respond with rerank results
            chain.proceed(task, action, request, listener.delegateResponse((actionListener, exception) -> {
                ActionResponse response = new InferenceAction.Response(new RankedDocsResults(RANKED_DOCS));
                actionListener.onResponse((Response) response);
            }));
        }
    }

    private Client client;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(InferencePlugin.class, TestPlugin.class);
    }

    @Before
    public void setup() {
        // Initialize index with a few documents
        client = client();
        for (int i = 0; i < 5; i++) {
            client.prepareIndex("my-index").setSource(Collections.singletonMap("text", "text " + i)).get();
        }
        client.admin().indices().prepareRefresh("my-index").get();
    }

    public void testRerank() throws Exception {
        // Execute search with text similarity reranking
        SearchRequest request = client.prepareSearch()
            .setRankBuilder(new TextSimilarityRankBuilder("text", "my-rerank-model", "my query", 100, 0.0f))
            .setQuery(QueryBuilders.matchAllQuery())
            .request();

        // Verify order, rank and score of results
        SearchResponse response = client.search(request).get();
        SearchHit[] hits = response.getHits().getHits();
        assertEquals(5, hits.length);
        assertHitHasRankScoreAndText(hits[0], 1, 0.9f, "text 3");
        assertHitHasRankScoreAndText(hits[1], 2, 0.8f, "text 0");
        assertHitHasRankScoreAndText(hits[2], 3, 0.7f, "text 2");
        assertHitHasRankScoreAndText(hits[3], 4, 0.6f, "text 1");
        assertHitHasRankScoreAndText(hits[4], 5, 0.5f, "text 4");
    }

    public void testRerankWithMinScore() throws Exception {
        // Execute search with text similarity reranking
        SearchRequest request = client.prepareSearch()
            .setRankBuilder(new TextSimilarityRankBuilder("text", "my-rerank-model", "my query", 100, 0.7f))
            .setQuery(QueryBuilders.matchAllQuery())
            .request();

        // Verify order, rank and score of results
        SearchResponse response = client.search(request).get();
        SearchHit[] hits = response.getHits().getHits();
        assertEquals(3, hits.length);
        assertHitHasRankScoreAndText(hits[0], 1, 0.9f, "text 3");
        assertHitHasRankScoreAndText(hits[1], 2, 0.8f, "text 0");
        assertHitHasRankScoreAndText(hits[2], 3, 0.7f, "text 2");
    }

    private static void assertHitHasRankScoreAndText(SearchHit hit, int expectedRank, float expectedScore, String expectedText) {
        assertEquals(expectedRank, hit.getRank());
        assertEquals(expectedScore, hit.getScore(), 0.0f);
        assertEquals(expectedText, Objects.requireNonNull(hit.getSourceAsMap()).get("text"));
    }

}
