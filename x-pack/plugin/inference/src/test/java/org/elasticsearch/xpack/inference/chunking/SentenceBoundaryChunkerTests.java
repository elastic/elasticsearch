/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import com.ibm.icu.text.BreakIterator;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.xpack.inference.chunking.WordBoundaryChunkerTests.TEST_TEXT;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SentenceBoundaryChunkerTests extends ESTestCase {

    public void testChunkSplitLargeChunkSizes() {
        for (int maxWordsPerChunk : new int[] { 100, 200 }) {
            var chunker = new SentenceBoundaryChunker();
            var chunks = chunker.chunk(TEST_TEXT, maxWordsPerChunk);

            int numChunks = expectedNumberOfChunks(sentenceSizes(TEST_TEXT), maxWordsPerChunk);
            assertThat("words per chunk " + maxWordsPerChunk, chunks, hasSize(numChunks));

            for (var chunk : chunks) {
                assertTrue(Character.isUpperCase(chunk.charAt(0)));
                var trailingWhiteSpaceRemoved = chunk.strip();
                var lastChar = trailingWhiteSpaceRemoved.charAt(trailingWhiteSpaceRemoved.length() - 1);
                assertThat(lastChar, Matchers.is('.'));
            }
        }
    }

    public void testChunk_ChunkSizeLargerThanText() {
        int maxWordsPerChunk = 500;
        var chunker = new SentenceBoundaryChunker();
        var chunks = chunker.chunk(TEST_TEXT, maxWordsPerChunk);

        assertEquals(chunks.get(0), TEST_TEXT);
    }

    public void testChunkSplit_SentencesLongerThanChunkSize() {
        var chunkSizes = new int[] { 10, 30, 50 };
        var expectedNumberOFChunks = new int[] { 21, 7, 4 };

        for (int i = 0; i < chunkSizes.length; i++) {
            int maxWordsPerChunk = chunkSizes[i];
            var chunker = new SentenceBoundaryChunker();
            var chunks = chunker.chunk(TEST_TEXT, maxWordsPerChunk);

            assertThat("words per chunk " + maxWordsPerChunk, chunks, hasSize(expectedNumberOFChunks[i]));
            for (var chunk : chunks) {
                // count whitespaced words
                // strip out the '=' signs as they are not counted as words by ICU
                var trimmed = chunk.trim().replace("=", "");
                // split by hyphen or whitespace to match the way
                // the ICU break iterator counts words
                var split = trimmed.split("[\\s\\-]+");
                int numWhiteSpacedWords = (int) Arrays.stream(split).filter(s -> s.isEmpty() == false).count();
                if (chunk.trim().endsWith(".")) {
                    // End of sentence, may be less than maxWordsPerChunk
                    assertThat(Arrays.toString(split), numWhiteSpacedWords, lessThanOrEqualTo(maxWordsPerChunk));
                } else {
                    // splitting inside a sentence so should have max words
                    assertEquals(Arrays.toString(split), maxWordsPerChunk, numWhiteSpacedWords);
                }
            }
        }
    }

    public void testCountWords() {
        // Test word count matches the whitespace separated word count.
        var splitByWhiteSpaceSentenceSizes = sentenceSizes(TEST_TEXT);

        var sentenceIterator = BreakIterator.getSentenceInstance(Locale.ROOT);
        sentenceIterator.setText(TEST_TEXT);

        var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
        wordIterator.setText(TEST_TEXT);

        int start = 0;
        int end = sentenceIterator.next();
        assertEquals(splitByWhiteSpaceSentenceSizes[0], SentenceBoundaryChunker.countWords(start, end, wordIterator));
        start = end;
        end = sentenceIterator.next();
        assertEquals(splitByWhiteSpaceSentenceSizes[1], SentenceBoundaryChunker.countWords(start, end, wordIterator));
        start = end;
        end = sentenceIterator.next();
        assertEquals(splitByWhiteSpaceSentenceSizes[2], SentenceBoundaryChunker.countWords(start, end, wordIterator));
        start = end;
        end = sentenceIterator.next();
        assertEquals(splitByWhiteSpaceSentenceSizes[3], SentenceBoundaryChunker.countWords(start, end, wordIterator));

        assertEquals(BreakIterator.DONE, sentenceIterator.next());
    }

    public void testCountWords_short() {
        // Test word count matches the whitespace separated word count.
        var text = "This is a short sentence. Followed by another.";

        var sentenceIterator = BreakIterator.getSentenceInstance(Locale.ROOT);
        sentenceIterator.setText(text);

        var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
        wordIterator.setText(text);

        int start = 0;
        int end = sentenceIterator.next();
        assertEquals(5, SentenceBoundaryChunker.countWords(0, end, wordIterator));
        start = end;
        end = sentenceIterator.next();
        assertEquals(3, SentenceBoundaryChunker.countWords(start, end, wordIterator));
        assertEquals(BreakIterator.DONE, sentenceIterator.next());
    }

    public void testCountWords_WithSymbols() {
        {
            var text = "foo != bar";
            var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
            wordIterator.setText(text);
            // "foo", "bar" - "!=" is not counted
            assertEquals(2, SentenceBoundaryChunker.countWords(0, text.length(), wordIterator));
        }
        {
            var text = "foo & bar";
            var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
            wordIterator.setText(text);
            // "foo", "bar" - the & is not counted
            assertEquals(2, SentenceBoundaryChunker.countWords(0, text.length(), wordIterator));
        }
        {
            var text = "m&s";
            var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
            wordIterator.setText(text);
            // "m", "s" - the & is not counted
            assertEquals(2, SentenceBoundaryChunker.countWords(0, text.length(), wordIterator));
        }
    }

    public void testChunkSplitLargeChunkSizesWithChunkingSettings() {
        for (int maxWordsPerChunk : new int[] { 100, 200 }) {
            var chunker = new SentenceBoundaryChunker();
            SentenceBoundaryChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(maxWordsPerChunk);
            var chunks = chunker.chunk(TEST_TEXT, chunkingSettings);

            int numChunks = expectedNumberOfChunks(sentenceSizes(TEST_TEXT), maxWordsPerChunk);
            assertThat("words per chunk " + maxWordsPerChunk, chunks, hasSize(numChunks));

            for (var chunk : chunks) {
                assertTrue(Character.isUpperCase(chunk.charAt(0)));
                var trailingWhiteSpaceRemoved = chunk.strip();
                var lastChar = trailingWhiteSpaceRemoved.charAt(trailingWhiteSpaceRemoved.length() - 1);
                assertThat(lastChar, Matchers.is('.'));
            }
        }
    }

    public void testInvalidChunkingSettingsProvided() {
        ChunkingSettings chunkingSettings = new WordBoundaryChunkingSettings(randomNonNegativeInt(), randomNonNegativeInt());
        assertThrows(IllegalArgumentException.class, () -> { new SentenceBoundaryChunker().chunk(TEST_TEXT, chunkingSettings); });
    }

    private int[] sentenceSizes(String text) {
        var sentences = text.split("\\.\\s+");
        var lengths = new int[sentences.length];
        for (int i = 0; i < sentences.length; i++) {
            // strip out the '=' signs as they are not counted as words by ICU
            sentences[i] = sentences[i].replace("=", "");
            // split by hyphen or whitespace to match the way
            // the ICU break iterator counts words
            lengths[i] = sentences[i].split("[ \\-]+").length;
        }
        return lengths;
    }

    private int expectedNumberOfChunks(int[] sentenceLengths, int maxWordsPerChunk) {
        int numChunks = 1;
        int runningWordCount = 0;
        for (int i = 0; i < sentenceLengths.length; i++) {
            if (runningWordCount + sentenceLengths[i] > maxWordsPerChunk) {
                numChunks++;
                runningWordCount = sentenceLengths[i];
            } else {
                runningWordCount += sentenceLengths[i];
            }
        }
        return numChunks;
    }
}
