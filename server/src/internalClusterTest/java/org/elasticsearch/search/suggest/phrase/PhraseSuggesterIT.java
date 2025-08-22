/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.search.suggest.SuggestBuilders.phraseSuggestion;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class PhraseSuggesterIT extends ESIntegTestCase {

    /**
     * Reproduces the IllegalArgumentException: "At least one unigram is required but all tokens were ngrams"
     *
     * This happens when:
     * 1. A phrase suggester is configured to use an analyzer that only produces n-grams (no unigrams)
     * 2. The NoisyChannelSpellChecker is created with requireUnigram=true (which is the default)
     * 3. The input text is analyzed and all resulting tokens are marked as n-grams
     * 4. The NoisyChannelSpellChecker throws an IllegalArgumentException because it expects at least one unigram
     */
    public void testPhraseSuggestionWithNgramOnlyAnalyzerThrowsException() throws IOException {
        createIndexAndDocs(false);

        // Create a phrase suggestion that uses the ngram-only field
        // This should trigger the IllegalArgumentException because:
        // 1. The "text.ngrams" field uses an analyzer that only produces n-grams
        // 2. When "hello world" is analyzed, it produces only n-grams, but no unigrams
        // 3. The DirectCandidateGenerator.analyze() method sets anyTokens=true but anyUnigram=false
        // 4. NoisyChannelSpellChecker.end() throws IllegalArgumentException
        SearchRequestBuilder searchBuilder = createSuggesterSearch("text.ngrams");

        try {
            assertResponse(searchBuilder, response -> {
                // We didn't fail all shards - we get a response with failed shards
                assertThat(response.status(), equalTo(RestStatus.OK));
                assertThat(response.getFailedShards(), greaterThan(0));
                assertThat(response.getShardFailures().length, greaterThan(0));
                checkShardFailures(response.getShardFailures());
            });
        } catch (SearchPhaseExecutionException e) {
            // If all shards fail, we get a SearchPhaseExecutionException
            checkShardFailures(e.shardFailures());
        }
    }

    private static void checkShardFailures(ShardSearchFailure[] shardFailures) {
        for (ShardSearchFailure shardFailure : shardFailures) {
            assertTrue(shardFailure.getCause() instanceof IllegalArgumentException);
            assertEquals("At least one unigram is required but all tokens were ngrams", shardFailure.getCause().getMessage());
        }
    }

    private static SearchRequestBuilder createSuggesterSearch(String fieldName) {
        PhraseSuggestionBuilder phraseSuggestion = phraseSuggestion(fieldName).text("hello world")
            .addCandidateGenerator(new DirectCandidateGeneratorBuilder("text").suggestMode("always").minWordLength(1).maxEdits(2));

        SearchRequestBuilder searchBuilder = prepareSearch("test").setSize(0)
            .suggest(new SuggestBuilder().addSuggestion("test_suggestion", phraseSuggestion));
        return searchBuilder;
    }

    /**
     * Demonstrates that the same configuration works fine when using a different field that produces unigrams
     */
    public void testPhraseSuggestionWithUnigramFieldWorks() throws IOException {
        createIndexAndDocs(false);

        // Use the main "text" field instead of "text.ngrams" - this should work fine
        // because the standard analyzer produces unigrams
        SearchRequestBuilder searchRequestBuilder = createSuggesterSearch("text");

        // This should NOT throw an exception
        assertNoFailuresAndResponse(searchRequestBuilder, response -> {
            // Just verify we get a response without exceptions
            assertNotNull(response.getSuggest());
        });
    }

    /**
     * Test showing the same ngram-only configuration works when shingle filter allows output_unigrams=true
     */
    public void testPhraseSuggestionWithNgramsAndUnigramsWorks() throws IOException {
        createIndexAndDocs(true);

        // Use the ngrams field, but this time it should work because the analyzer produces unigrams too
        SearchRequestBuilder searchRequestBuilder = createSuggesterSearch("text.ngrams");

        // This should NOT throw an exception because unigrams are available
        assertNoFailuresAndResponse(searchRequestBuilder, response -> { assertNotNull(response.getSuggest()); });
    }

    private void createIndexAndDocs(boolean outputUnigrams) throws IOException {
        // Create an index with a shingle analyzer that outputs unigrams or not
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
                    .put("index.analysis.analyzer.ngram_only.tokenizer", "standard")
                    .putList("index.analysis.analyzer.ngram_only.filter", "my_shingle", "lowercase")
                    .put("index.analysis.filter.my_shingle.type", "shingle")
                    .put("index.analysis.filter.my_shingle.output_unigrams", outputUnigrams)
                    .put("index.analysis.filter.my_shingle.min_shingle_size", 2)
                    .put("index.analysis.filter.my_shingle.max_shingle_size", 3)
            )
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject("text")
                        .field("type", "text")
                        .field("analyzer", "standard")
                        .startObject("fields")
                        .startObject("ngrams")
                        .field("type", "text")
                        .field("analyzer", "ngram_only") // Use our ngram-only analyzer for suggestions
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        ensureGreen();

        // Index some test documents
        indexDoc("test", "1", "text", "hello world test");
        indexDoc("test", "2", "text", "another test phrase");
        indexDoc("test", "3", "text", "some more content");
        refresh();
    }

}
