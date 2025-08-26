/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.snippets;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;

public class SnippetScorerTests extends ESTestCase {

    public void testScoreSnippets() throws IOException {
        SnippetScorer scorer = new SnippetScorer();

        List<String> snippets = Arrays.asList(
            "Cats like to sleep all day and play with mice",
            "Dogs are loyal companions and great pets",
            "The weather today is very sunny and warm",
            "Dogs love to play with toys and go for walks",
            "Elasticsearch is a great search engine"
        );

        String inferenceText = "dogs play walk";
        int maxResults = 3;

        List<SnippetScorer.ScoredSnippet> scoredSnippets = scorer.scoreSnippets(snippets, inferenceText, maxResults);

        assertEquals(maxResults, scoredSnippets.size());

        // The snippets about dogs should score highest, followed by the snippet about cats
        SnippetScorer.ScoredSnippet snippet = scoredSnippets.getFirst();
        assertTrue(snippet.content().equalsIgnoreCase("Dogs love to play with toys and go for walks"));
        assertThat(snippet.score(), greaterThan(0f));

        snippet = scoredSnippets.get(1);
        assertTrue(snippet.content().equalsIgnoreCase("Dogs are loyal companions and great pets"));
        assertThat(snippet.score(), greaterThan(0f));

        snippet = scoredSnippets.get(2);
        assertTrue(snippet.content().equalsIgnoreCase("Cats like to sleep all day and play with mice"));
        assertThat(snippet.score(), greaterThan(0f));

        // Scores should be in descending order
        for (int i = 1; i < scoredSnippets.size(); i++) {
            assertTrue(scoredSnippets.get(i - 1).score() >= scoredSnippets.get(i).score());
        }
    }
}
