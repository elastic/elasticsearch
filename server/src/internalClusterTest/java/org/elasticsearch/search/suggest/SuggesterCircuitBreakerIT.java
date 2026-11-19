/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.suggest;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder.SuggestMode;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.elasticsearch.search.suggest.SuggestBuilders.phraseSuggestion;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/** Verifies that oversized term and phrase suggester queues are rejected before they can exhaust the shard heap. */
public class SuggesterCircuitBreakerIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("indices.breaker.request.type", "memory")
            .put("indices.breaker.request.limit", "100mb")
            .build();
    }

    /**
     * DirectSpellChecker derives its candidate inspection bound from {@code shard_size}. A value close to
     * {@link Integer#MAX_VALUE} can drive its growable candidate queue toward billions of entries, so the request
     * breaker must reject the configuration before candidate collection begins.
     */
    public void testTermSuggestShardSizeTripsCircuitBreaker() throws IOException {
        createTestIndex();
        assertCircuitBreaks(
            "term",
            termSuggestion("body").text("the quik brown").suggestMode(SuggestMode.ALWAYS).shardSize(Integer.MAX_VALUE - 17)
        );
    }

    /**
     * CandidateScorer eagerly allocates a Lucene priority queue sized to {@code shard_size}. The request breaker must
     * trip before an oversized backing array is allocated.
     */
    public void testPhraseSuggestShardSizeTripsCircuitBreaker() throws IOException {
        createTestIndex();
        assertCircuitBreaks(
            "phrase",
            phraseSuggestion("body").text("the quik brown")
                .addCandidateGenerator(new DirectCandidateGeneratorBuilder("body").suggestMode("always"))
                .shardSize(Integer.MAX_VALUE - 17)
        );
    }

    /**
     * A huge direct-generator size can drive DirectSpellChecker's growable candidate queue toward billions of entries.
     * The request breaker must reject it before candidate collection begins.
     */
    public void testPhraseSuggestDirectGeneratorSizeTripsCircuitBreaker() throws IOException {
        createTestIndex();
        assertCircuitBreaks(
            "phrase",
            phraseSuggestion("body").text("the quik brown")
                .addCandidateGenerator(new DirectCandidateGeneratorBuilder("body").suggestMode("always").size(Integer.MAX_VALUE - 17))
        );
    }

    private void createTestIndex() throws IOException {
        assertAcked(prepareCreate("test").setMapping("body", "type=text"));
        ensureGreen();
        indexDoc("test", "1", "body", "the quick brown fox jumps over the lazy dog");
        refresh();
    }

    private void assertCircuitBreaks(String name, SuggestionBuilder<?> suggestion) {
        Exception exception = expectThrows(
            Exception.class,
            () -> prepareSearch("test").setAllowPartialSearchResults(false)
                .setSize(0)
                .suggest(new SuggestBuilder().addSuggestion(name, suggestion))
                .get()
        );
        CircuitBreakingException circuitBreakingException = (CircuitBreakingException) ExceptionsHelper.unwrap(
            exception,
            CircuitBreakingException.class
        );
        assertThat(circuitBreakingException, notNullValue());
        assertThat(circuitBreakingException.getMessage(), containsString(name + "-suggest-collector"));
    }
}
