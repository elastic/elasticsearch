/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;
import static org.hamcrest.Matchers.instanceOf;

public class RerankRequestChunkerTests extends ESTestCase {
    private final String TEST_SENTENCE = "This is a test sentence that has ten total words. ";

    public void testGetChunkedInput_EmptyInput() {
        var chunker = new RerankRequestChunker(TEST_SENTENCE, List.of(), null);
        assertTrue(chunker.getChunkedInputs().isEmpty());
    }

    public void testGetChunkedInput_SingleInputWithoutChunkingRequired() {
        var inputs = List.of(generateTestText(10));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, randomBoolean() ? null : randomIntBetween(1, 10));
        assertEquals(inputs, chunker.getChunkedInputs());
    }

    public void testGetChunkedInput_SingleInputWithChunkingRequired() {
        var inputs = List.of(generateTestText(100));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(3, chunkedInputs.size());
    }

    public void testGetChunkedInput_SingleInputWithChunkingRequiredWithMaxChunksPerDocLessThanTotalChunksGenerated() {
        var inputs = List.of(generateTestText(100));
        var maxChunksPerDoc = randomIntBetween(1, 2);
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, maxChunksPerDoc);
        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(maxChunksPerDoc, chunkedInputs.size());
    }

    public void testGetChunkedInput_SingleInputWithChunkingRequiredWithMaxChunksPerDocGreaterThanTotalChunksGenerated() {
        var inputs = List.of(generateTestText(100));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, randomIntBetween(4, 10));
        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(3, chunkedInputs.size());
    }

    public void testGetChunkedInput_MultipleInputsWithoutChunkingRequired() {
        var inputs = List.of(generateTestText(10), generateTestText(10));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, randomBoolean() ? null : randomIntBetween(1, 10));
        assertEquals(inputs, chunker.getChunkedInputs());
    }

    public void testGetChunkedInput_MultipleInputsWithSomeChunkingRequired() {
        var inputs = List.of(generateTestText(10), generateTestText(100));
        var chunker = new RerankRequestChunker(randomAlphaOfLength(10), inputs, null);
        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(4, chunkedInputs.size());
    }

    public void testGetChunkedInput_MultipleInputsWithSomeChunkingRequiredWithMaxChunksPerDocLessThanTotalChunksGenerated() {
        var inputs = List.of(generateTestText(10), generateTestText(100));
        var maxChunksPerDoc = randomIntBetween(1, 2);
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, maxChunksPerDoc);
        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(1 + maxChunksPerDoc, chunkedInputs.size());
    }

    public void testGetChunkedInput_MultipleInputsWithSomeChunkingRequiredWithMaxChunksPerDocGreaterThanTotalChunksGenerated() {
        var inputs = List.of(generateTestText(10), generateTestText(100));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, randomIntBetween(3, 10));
        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(4, chunkedInputs.size());
    }

    public void testGetChunkedInput_MultipleInputsWithAllRequiringChunking() {
        var inputs = List.of(generateTestText(100), generateTestText(100));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(6, chunkedInputs.size());
    }

    public void testGetChunkedInput_MultipleInputsWithAllRequiringChunkingWithMaxChunksPerDocLessThanTotalChunksGenerated() {
        var inputs = List.of(generateTestText(100), generateTestText(100));
        var maxChunksPerDoc = randomIntBetween(1, 2);
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, maxChunksPerDoc);
        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(2 * maxChunksPerDoc, chunkedInputs.size());
    }

    public void testGetChunkedInput_MultipleInputsWithAllRequiringChunkingWithMaxChunksPerDocGreaterThanTotalChunksGenerated() {
        var inputs = List.of(generateTestText(100), generateTestText(100));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, randomIntBetween(4, 10));
        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(6, chunkedInputs.size());
    }

    public void testParseChunkedRerankResultsListener_NonRankedDocsResults() {
        var inputs = List.of(generateTestText(10), generateTestText(100));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var listener = chunker.parseChunkedRerankResultsListener(
            ActionListener.wrap(
                results -> fail("Expected failure but got: " + results.getClass()),
                e -> assertTrue(e instanceof IllegalArgumentException && e.getMessage().contains("Expected RankedDocsResults"))
            )
        );

        listener.onResponse(new InferenceServiceResults() {
        });
    }

    public void testParseChunkedRerankResultsListener_EmptyInput() {
        var chunker = new RerankRequestChunker(TEST_SENTENCE, List.of(), null);
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            assertEquals(0, rankedDocResults.getRankedDocs().size());
        }, e -> fail("Expected successful parsing but got failure: " + e)));
        listener.onResponse(new RankedDocsResults(List.of()));
    }

    public void testParseChunkedRerankResultsListener_SingleInputWithoutChunking() {
        var inputs = List.of(generateTestText(10));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            assertEquals(1, rankedDocResults.getRankedDocs().size());
        }, e -> fail("Expected successful parsing but got failure: " + e)));

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(1, chunkedInputs.size());
        listener.onResponse(new RankedDocsResults(List.of(new RankedDocsResults.RankedDoc(0, 1.0f, chunkedInputs.get(0)))));
    }

    public void testParseChunkedRerankResultsListener_SingleInputWithChunking() {
        var inputs = List.of(generateTestText(100));
        var relevanceScore1 = randomFloatBetween(0, 1, true);
        var relevanceScore2 = randomFloatBetween(0, 1, true);
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            assertEquals(1, rankedDocResults.getRankedDocs().size());
            var expectedRankedDocs = List.of(new RankedDocsResults.RankedDoc(0, max(relevanceScore1, relevanceScore2), inputs.get(0)));
            assertEquals(expectedRankedDocs, rankedDocResults.getRankedDocs());
        }, e -> fail("Expected successful parsing but got failure: " + e)));

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(3, chunkedInputs.size());
        var rankedDocsResults = List.of(
            new RankedDocsResults.RankedDoc(0, relevanceScore1, chunkedInputs.get(0)),
            new RankedDocsResults.RankedDoc(1, relevanceScore2, chunkedInputs.get(1))
        );
        // TODO: Sort this so that the assumption that the results are in order holds
        listener.onResponse(new RankedDocsResults(rankedDocsResults));
    }

    public void testParseChunkedRerankResultsListener_MultipleInputsWithoutChunking() {
        var inputs = List.of(generateTestText(10), generateTestText(10));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            assertEquals(2, rankedDocResults.getRankedDocs().size());
            var sortedResults = new ArrayList<>(rankedDocResults.getRankedDocs());
            sortedResults.sort((r1, r2) -> Float.compare(r2.relevanceScore(), r1.relevanceScore()));
            assertEquals(sortedResults, rankedDocResults.getRankedDocs());
        }, e -> fail("Expected successful parsing but got failure: " + e)));

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(2, chunkedInputs.size());
        listener.onResponse(
            new RankedDocsResults(
                List.of(
                    new RankedDocsResults.RankedDoc(0, randomFloatBetween(0, 1, true), chunkedInputs.get(0)),
                    new RankedDocsResults.RankedDoc(1, randomFloatBetween(0, 1, true), chunkedInputs.get(1))
                )
            )
        );
    }

    public void testParseChunkedRerankResultsListener_MultipleInputsWithSomeChunking() {
        var inputs = List.of(generateTestText(10), generateTestText(100));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            assertEquals(2, rankedDocResults.getRankedDocs().size());
            var sortedResults = new ArrayList<>(rankedDocResults.getRankedDocs());
            sortedResults.sort((r1, r2) -> Float.compare(r2.relevanceScore(), r1.relevanceScore()));
            assertEquals(sortedResults, rankedDocResults.getRankedDocs());
        }, e -> fail("Expected successful parsing but got failure: " + e)));

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(4, chunkedInputs.size());
        listener.onResponse(
            new RankedDocsResults(
                List.of(
                    new RankedDocsResults.RankedDoc(0, randomFloatBetween(0, 1, true), chunkedInputs.get(0)),
                    new RankedDocsResults.RankedDoc(1, randomFloatBetween(0, 1, true), chunkedInputs.get(1)),
                    new RankedDocsResults.RankedDoc(2, randomFloatBetween(0, 1, true), chunkedInputs.get(2))
                )
            )
        );
    }

    public void testParseChunkedRerankResultsListener_MultipleInputsWithAllRequiringChunking() {
        var inputs = List.of(generateTestText(100), generateTestText(100));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            assertEquals(2, rankedDocResults.getRankedDocs().size());
            var sortedResults = new ArrayList<>(rankedDocResults.getRankedDocs());
            sortedResults.sort((r1, r2) -> Float.compare(r2.relevanceScore(), r1.relevanceScore()));
            assertEquals(sortedResults, rankedDocResults.getRankedDocs());
        }, e -> fail("Expected successful parsing but got failure: " + e)));

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(6, chunkedInputs.size());
        listener.onResponse(
            new RankedDocsResults(
                List.of(
                    new RankedDocsResults.RankedDoc(0, randomFloatBetween(0, 1, true), chunkedInputs.get(0)),
                    new RankedDocsResults.RankedDoc(1, randomFloatBetween(0, 1, true), chunkedInputs.get(1)),
                    new RankedDocsResults.RankedDoc(2, randomFloatBetween(0, 1, true), chunkedInputs.get(2)),
                    new RankedDocsResults.RankedDoc(3, randomFloatBetween(0, 1, true), chunkedInputs.get(3))
                )
            )
        );
    }

    private String generateTestText(int numSentences) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numSentences; i++) {
            sb.append(TEST_SENTENCE);
        }
        return sb.toString();
    }
}
