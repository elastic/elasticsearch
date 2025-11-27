/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.chunking;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.ArrayList;
import java.util.Collections;
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
            ),
            randomBoolean()
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
        }, e -> fail("Expected successful parsing but got failure: " + e)), randomBoolean());
        listener.onResponse(new RankedDocsResults(List.of()));
    }

    public void testParseChunkedRerankResultsListener_SingleInputWithoutChunking() {
        var inputs = List.of(generateTestText(10));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var returnDocuments = randomBoolean();
        var relevanceScores = generateRelevanceScores(1);
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            var expectedRankedDocs = List.of(
                new RankedDocsResults.RankedDoc(0, relevanceScores.get(0), returnDocuments ? inputs.get(0) : null)
            );
            assertEquals(expectedRankedDocs, rankedDocResults.getRankedDocs());
        }, e -> fail("Expected successful parsing but got failure: " + e)), returnDocuments);

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(1, chunkedInputs.size());
        listener.onResponse(
            new RankedDocsResults(List.of(new RankedDocsResults.RankedDoc(0, relevanceScores.get(0), chunkedInputs.get(0))))
        );
    }

    public void testParseChunkedRerankResultsListener_SingleInputWithChunkingWithFirstChunkRelevanceScoreHighest() {
        var inputs = List.of(generateTestText(100));
        var relevanceScores = List.of(1f, 0.5f, 0.3f);
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var returnDocuments = randomBoolean();
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            var expectedRankedDocs = List.of(
                new RankedDocsResults.RankedDoc(0, relevanceScores.get(0), returnDocuments ? inputs.get(0) : null)
            );
            assertEquals(expectedRankedDocs, rankedDocResults.getRankedDocs());
        }, e -> fail("Expected successful parsing but got failure: " + e)), returnDocuments);

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(3, chunkedInputs.size());
        listener.onResponse(new RankedDocsResults(generateRankedDocs(relevanceScores, chunkedInputs)));
    }

    public void testParseChunkedRerankResultsListener_SingleInputWithChunkingWithMiddleChunkRelevanceScoreHighest() {
        var inputs = List.of(generateTestText(100));
        var relevanceScores = List.of(0.5f, 1f, 0.5f);
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var returnDocuments = randomBoolean();
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            var expectedRankedDocs = List.of(
                new RankedDocsResults.RankedDoc(0, relevanceScores.get(1), returnDocuments ? inputs.get(0) : null)
            );
            assertEquals(expectedRankedDocs, rankedDocResults.getRankedDocs());
        }, e -> fail("Expected successful parsing but got failure: " + e)), returnDocuments);

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(3, chunkedInputs.size());
        listener.onResponse(new RankedDocsResults(generateRankedDocs(relevanceScores, chunkedInputs)));
    }

    public void testParseChunkedRerankResultsListener_SingleInputWithChunkingWithLastChunkRelevanceScoreHighest() {
        var inputs = List.of(generateTestText(100));
        var relevanceScores = List.of(0.5f, 0.3f, 1f);
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var returnDocuments = randomBoolean();
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            var expectedRankedDocs = List.of(
                new RankedDocsResults.RankedDoc(0, relevanceScores.get(2), returnDocuments ? inputs.get(0) : null)
            );
            assertEquals(expectedRankedDocs, rankedDocResults.getRankedDocs());
        }, e -> fail("Expected successful parsing but got failure: " + e)), returnDocuments);

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(3, chunkedInputs.size());
        listener.onResponse(new RankedDocsResults(generateRankedDocs(relevanceScores, chunkedInputs)));
    }

    public void testParseChunkedRerankResultsListener_MultipleInputsWithoutChunking() {
        var inputs = List.of(generateTestText(10), generateTestText(20));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var returnDocuments = randomBoolean();
        var relevanceScores = generateRelevanceScores(2);
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            var expectedRankedDocs = new ArrayList<RankedDocsResults.RankedDoc>();
            expectedRankedDocs.add(new RankedDocsResults.RankedDoc(0, relevanceScores.get(0), returnDocuments ? inputs.get(0) : null));
            expectedRankedDocs.add(new RankedDocsResults.RankedDoc(1, relevanceScores.get(1), returnDocuments ? inputs.get(1) : null));
            expectedRankedDocs.sort((r1, r2) -> Float.compare(r2.relevanceScore(), r1.relevanceScore()));
            assertEquals(expectedRankedDocs, rankedDocResults.getRankedDocs());
        }, e -> fail("Expected successful parsing but got failure: " + e)), returnDocuments);

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(2, chunkedInputs.size());
        listener.onResponse(new RankedDocsResults(generateRankedDocs(relevanceScores, chunkedInputs)));
    }

    public void testParseChunkedRerankResultsListener_MultipleInputsWithSomeChunking() {
        var inputs = List.of(generateTestText(10), generateTestText(100));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var returnDocuments = randomBoolean();
        var relevanceScores = generateRelevanceScores(4);
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            var expectedRankedDocs = new ArrayList<RankedDocsResults.RankedDoc>();
            expectedRankedDocs.add(new RankedDocsResults.RankedDoc(0, relevanceScores.get(0), returnDocuments ? inputs.get(0) : null));
            expectedRankedDocs.add(
                new RankedDocsResults.RankedDoc(1, Collections.max(relevanceScores.subList(1, 4)), returnDocuments ? inputs.get(1) : null)
            );
            expectedRankedDocs.sort((r1, r2) -> Float.compare(r2.relevanceScore(), r1.relevanceScore()));
            assertEquals(expectedRankedDocs, rankedDocResults.getRankedDocs());
        }, e -> fail("Expected successful parsing but got failure: " + e)), returnDocuments);

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(4, chunkedInputs.size());
        listener.onResponse(new RankedDocsResults(generateRankedDocs(relevanceScores, chunkedInputs)));
    }

    public void testParseChunkedRerankResultsListener_MultipleInputsWithAllRequiringChunking() {
        var inputs = List.of(generateTestText(100), generateTestText(105));
        var chunker = new RerankRequestChunker(TEST_SENTENCE, inputs, null);
        var returnDocuments = randomBoolean();
        var relevanceScores = generateRelevanceScores(6);
        var listener = chunker.parseChunkedRerankResultsListener(ActionListener.wrap(results -> {
            assertThat(results, instanceOf(RankedDocsResults.class));
            var rankedDocResults = (RankedDocsResults) results;
            var expectedRankedDocs = new ArrayList<RankedDocsResults.RankedDoc>();
            expectedRankedDocs.add(
                new RankedDocsResults.RankedDoc(0, Collections.max(relevanceScores.subList(0, 3)), returnDocuments ? inputs.get(0) : null)
            );
            expectedRankedDocs.add(
                new RankedDocsResults.RankedDoc(1, Collections.max(relevanceScores.subList(3, 6)), returnDocuments ? inputs.get(1) : null)
            );
            expectedRankedDocs.sort((r1, r2) -> Float.compare(r2.relevanceScore(), r1.relevanceScore()));
            assertEquals(expectedRankedDocs, rankedDocResults.getRankedDocs());
        }, e -> fail("Expected successful parsing but got failure: " + e)), returnDocuments);

        var chunkedInputs = chunker.getChunkedInputs();
        assertEquals(6, chunkedInputs.size());
        listener.onResponse(new RankedDocsResults(generateRankedDocs(relevanceScores, chunkedInputs)));
    }

    private String generateTestText(int numSentences) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numSentences; i++) {
            sb.append(TEST_SENTENCE);
        }
        return sb.toString();
    }

    private List<Float> generateRelevanceScores(int numScores) {
        List<Float> scores = new ArrayList<>();
        for (int i = 0; i < numScores; i++) {
            scores.add(randomValueOtherThanMany(scores::contains, () -> randomFloatBetween(0, 1, true)));
        }
        return scores;
    }

    private List<RankedDocsResults.RankedDoc> generateRankedDocs(List<Float> relevanceScores, List<String> chunkedInputs) {
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
        for (int i = 0; i < max(relevanceScores.size(), chunkedInputs.size()); i++) {
            rankedDocs.add(new RankedDocsResults.RankedDoc(i, relevanceScores.get(i), chunkedInputs.get(i)));
        }
        return rankedDocs;
    }
}
