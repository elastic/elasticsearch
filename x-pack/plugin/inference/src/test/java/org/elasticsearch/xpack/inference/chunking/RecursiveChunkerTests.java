/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

public class RecursiveChunkerTests extends ESTestCase {

    private final List<String> TEST_SEPARATORS = List.of("\n\n", "\n", "\f", "\t", "#");
    private final String TEST_SENTENCE = "This is a test sentence that has ten total words. ";

    public void testChunkWithInvalidChunkingSettings() {
        RecursiveChunker chunker = new RecursiveChunker();
        ChunkingSettings invalidSettings = new SentenceBoundaryChunkingSettings(randomIntBetween(20, 300), randomIntBetween(0, 1));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            chunker.chunk(randomAlphaOfLength(100), invalidSettings);
        });

        assertEquals("RecursiveChunker can't use ChunkingSettings with strategy [sentence]", exception.getMessage());
    }

    public void testChunkEmptyInput() {
        RecursiveChunkingSettings settings = generateChunkingSettings(randomIntBetween(10, 300), generateRandomSeparators());
        assertExpectedChunksGenerated("", settings, List.of(new Chunker.ChunkOffset(0, 0)));
    }

    public void testChunkSingleCharacterInput() {
        RecursiveChunkingSettings settings = generateChunkingSettings(randomIntBetween(10, 300), generateRandomSeparators());
        assertExpectedChunksGenerated(randomAlphaOfLength(1), settings, List.of(new Chunker.ChunkOffset(0, 1)));
    }

    public void testChunkInputShorterThanMaxChunkSize() {
        var maxChunkSize = randomIntBetween(10, 300);
        var input = randomAlphaOfLength(maxChunkSize - 1);
        RecursiveChunkingSettings settings = generateChunkingSettings(maxChunkSize, generateRandomSeparators());
        assertExpectedChunksGenerated(input, settings, List.of(new Chunker.ChunkOffset(0, input.length())));
    }

    public void testChunkInputRequiresOneSplit() {
        List<String> separators = generateRandomSeparators();
        RecursiveChunkingSettings settings = generateChunkingSettings(10, separators);
        String input = generateTestText(2, List.of(separators.getFirst()));

        assertExpectedChunksGenerated(
            input,
            settings,
            List.of(new Chunker.ChunkOffset(0, TEST_SENTENCE.length()), new Chunker.ChunkOffset(TEST_SENTENCE.length(), input.length()))
        );
    }

    public void testChunkInputRequiresMultipleSplits() {
        var separators = generateRandomSeparators();
        RecursiveChunkingSettings settings = generateChunkingSettings(15, separators);
        String input = generateTestText(4, List.of(separators.get(1), separators.getFirst(), separators.get(1)));

        var expectedFirstChunkOffsetEnd = TEST_SENTENCE.length();
        var expectedSecondChunkOffsetEnd = TEST_SENTENCE.length() * 2 + separators.get(1).length();
        var expectedThirdChunkOffsetEnd = TEST_SENTENCE.length() * 3 + separators.getFirst().length() + separators.get(1).length();
        assertExpectedChunksGenerated(
            input,
            settings,
            List.of(
                new Chunker.ChunkOffset(0, expectedFirstChunkOffsetEnd),
                new Chunker.ChunkOffset(expectedFirstChunkOffsetEnd, expectedSecondChunkOffsetEnd),
                new Chunker.ChunkOffset(expectedSecondChunkOffsetEnd, expectedThirdChunkOffsetEnd),
                new Chunker.ChunkOffset(expectedThirdChunkOffsetEnd, input.length())
            )
        );
    }

    public void testChunkInputRequiresBackupChunkingStrategy() {
        var separators = generateRandomSeparators();
        RecursiveChunkingSettings settings = generateChunkingSettings(10, separators);
        String input = generateTestText(4, List.of("", separators.getFirst(), ""));

        var expectedFirstChunkOffsetEnd = TEST_SENTENCE.length();
        var expectedSecondChunkOffsetEnd = TEST_SENTENCE.length() * 2;
        var expectedThirdChunkOffsetEnd = TEST_SENTENCE.length() * 3 + separators.getFirst().length();
        assertExpectedChunksGenerated(
            input,
            settings,
            List.of(
                new Chunker.ChunkOffset(0, expectedFirstChunkOffsetEnd),
                new Chunker.ChunkOffset(expectedFirstChunkOffsetEnd, expectedSecondChunkOffsetEnd),
                new Chunker.ChunkOffset(expectedSecondChunkOffsetEnd, expectedThirdChunkOffsetEnd),
                new Chunker.ChunkOffset(expectedThirdChunkOffsetEnd, input.length())
            )
        );
    }

    public void testChunkWithRegexSeparator() {
        var separators = List.of("(?<!\\n)\\n(?!\\n)", "(?<!\\n)\\n\\n(?!\\n)");
        RecursiveChunkingSettings settings = generateChunkingSettings(10, separators);
        String input = generateTestText(4, List.of("\n", "\n", "\n\n"));

        var expectedFirstChunkOffsetEnd = TEST_SENTENCE.length();
        var expectedSecondChunkOffsetEnd = TEST_SENTENCE.length() * 2 + "\n".length();
        var expectedThirdChunkOffsetEnd = TEST_SENTENCE.length() * 3 + "\n".length() * 2;
        assertExpectedChunksGenerated(
            input,
            settings,
            List.of(
                new Chunker.ChunkOffset(0, expectedFirstChunkOffsetEnd),
                new Chunker.ChunkOffset(expectedFirstChunkOffsetEnd, expectedSecondChunkOffsetEnd),
                new Chunker.ChunkOffset(expectedSecondChunkOffsetEnd, expectedThirdChunkOffsetEnd),
                new Chunker.ChunkOffset(expectedThirdChunkOffsetEnd, input.length())
            )
        );
    }

    public void testChunkLongDocument() {
        int numSentences = randomIntBetween(50, 100);
        List<String> separators = generateRandomSeparators();
        List<String> splittersAfterSentences = new ArrayList<>();
        for (int i = 0; i < numSentences - 1; i++) {
            splittersAfterSentences.add(randomFrom(separators));
        }
        RecursiveChunkingSettings settings = generateChunkingSettings(15, separators);
        String input = generateTestText(numSentences, splittersAfterSentences);

        List<Chunker.ChunkOffset> expectedChunks = new ArrayList<>();
        int currentOffset = 0;
        for (int i = 0; i < numSentences; i++) {
            int chunkLength = TEST_SENTENCE.length();
            if (i > 0) {
                chunkLength += splittersAfterSentences.get(i - 1).length();
            }
            expectedChunks.add(new Chunker.ChunkOffset(currentOffset, currentOffset + chunkLength));
            currentOffset += chunkLength;
        }

        assertExpectedChunksGenerated(input, settings, expectedChunks);
    }

    private void assertExpectedChunksGenerated(String input, RecursiveChunkingSettings settings, List<Chunker.ChunkOffset> expectedChunks) {
        RecursiveChunker chunker = new RecursiveChunker();
        List<Chunker.ChunkOffset> chunks = chunker.chunk(input, settings);
        assertEquals(expectedChunks, chunks);
    }

    private String generateTestText(int numSentences, List<String> splittersAfterSentences) {
        assert (splittersAfterSentences.size() == numSentences - 1);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numSentences; i++) {
            sb.append(TEST_SENTENCE);
            if (i < numSentences - 1) {
                sb.append(splittersAfterSentences.get(i));
            }
        }
        return sb.toString();
    }

    private List<String> generateRandomSeparators() {
        return randomSubsetOf(randomIntBetween(2, 3), TEST_SEPARATORS);
    }

    private RecursiveChunkingSettings generateChunkingSettings(int maxChunkSize, List<String> separators) {
        return new RecursiveChunkingSettings(maxChunkSize, separators);
    }
}
