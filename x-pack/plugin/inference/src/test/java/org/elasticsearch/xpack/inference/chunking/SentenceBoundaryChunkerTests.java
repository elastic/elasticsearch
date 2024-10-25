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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.xpack.inference.chunking.WordBoundaryChunkerTests.TEST_TEXT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class SentenceBoundaryChunkerTests extends ESTestCase {

    public void testChunkSplitLargeChunkSizes() {
        for (int maxWordsPerChunk : new int[] { 100, 200 }) {
            var chunker = new SentenceBoundaryChunker();
            var chunks = chunker.chunk(TEST_TEXT, maxWordsPerChunk, false);

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

    public void testChunkSplitLargeChunkSizes_withOverlap() {
        boolean overlap = true;
        for (int maxWordsPerChunk : new int[] { 70, 80, 100, 120, 150, 200 }) {
            var chunker = new SentenceBoundaryChunker();
            var chunks = chunker.chunk(TEST_TEXT, maxWordsPerChunk, overlap);

            int[] overlaps = chunkOverlaps(sentenceSizes(TEST_TEXT), maxWordsPerChunk, overlap);
            assertThat("words per chunk " + maxWordsPerChunk, chunks, hasSize(overlaps.length));

            assertTrue(Character.isUpperCase(chunks.get(0).charAt(0)));

            for (int i = 0; i < overlaps.length; i++) {
                if (overlaps[i] == 0) {
                    // start of a sentence
                    assertTrue(Character.isUpperCase(chunks.get(i).charAt(0)));
                } else {
                    // The start of this chunk should contain some text from the end of the previous
                    var previousChunk = chunks.get(i - 1);
                    assertThat(chunks.get(i), containsString(previousChunk.substring(previousChunk.length() - 20)));
                }
            }

            var trailingWhiteSpaceRemoved = chunks.get(0).strip();
            var lastChar = trailingWhiteSpaceRemoved.charAt(trailingWhiteSpaceRemoved.length() - 1);
            assertThat(lastChar, Matchers.is('.'));
            trailingWhiteSpaceRemoved = chunks.get(chunks.size() - 1).strip();
            lastChar = trailingWhiteSpaceRemoved.charAt(trailingWhiteSpaceRemoved.length() - 1);
            assertThat(lastChar, Matchers.is('.'));
        }
    }

    public void testWithOverlap_SentencesFitInChunks() {
        int numChunks = 4;
        int chunkSize = 100;

        var sb = new StringBuilder();

        int[] sentenceStartIndexes = new int[numChunks];
        sentenceStartIndexes[0] = 0;

        int numSentences = randomIntBetween(2, 5);
        int sentenceIndex = 0;
        int lastSentenceSize = 0;
        int roughSentenceSize = (chunkSize / numSentences) - 1;
        for (int j = 0; j < numSentences; j++) {
            sb.append(makeSentence(roughSentenceSize, sentenceIndex++));
            lastSentenceSize = roughSentenceSize;
        }

        for (int i = 1; i < numChunks; i++) {
            sentenceStartIndexes[i] = sentenceIndex - 1;

            roughSentenceSize = (chunkSize / numSentences) - 1;
            int wordCount = lastSentenceSize;

            while (wordCount + roughSentenceSize < chunkSize) {
                sb.append(makeSentence(roughSentenceSize, sentenceIndex++));
                lastSentenceSize = roughSentenceSize;
                wordCount += roughSentenceSize;
            }
        }

        var chunker = new SentenceBoundaryChunker();
        var chunks = chunker.chunk(sb.toString(), chunkSize, true);
        assertThat(chunks, hasSize(numChunks));
        for (int i = 0; i < numChunks; i++) {
            assertThat("num sentences " + numSentences, chunks.get(i), startsWith("SStart" + sentenceStartIndexes[i]));
            assertThat("num sentences " + numSentences, chunks.get(i).trim(), endsWith("."));
        }
    }

    private String makeSentence(int numWords, int sentenceIndex) {
        StringBuilder sb = new StringBuilder();
        sb.append("SStart").append(sentenceIndex).append(' ');
        for (int i = 1; i < numWords - 1; i++) {
            sb.append(i).append(' ');
        }
        sb.append(numWords - 1).append(". ");
        return sb.toString();
    }

    public void testChunk_ChunkSizeLargerThanText() {
        int maxWordsPerChunk = 500;
        var chunker = new SentenceBoundaryChunker();
        var chunks = chunker.chunk(TEST_TEXT, maxWordsPerChunk, false);
        assertEquals(chunks.get(0), TEST_TEXT);

        chunks = chunker.chunk(TEST_TEXT, maxWordsPerChunk, true);
        assertEquals(chunks.get(0), TEST_TEXT);
    }

    public void testChunkSplit_SentencesLongerThanChunkSize() {
        var chunkSizes = new int[] { 10, 30, 50 };
        var expectedNumberOFChunks = new int[] { 21, 7, 4 };

        for (int i = 0; i < chunkSizes.length; i++) {
            int maxWordsPerChunk = chunkSizes[i];
            var chunker = new SentenceBoundaryChunker();
            var chunks = chunker.chunk(TEST_TEXT, maxWordsPerChunk, false);

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

    public void testChunkSplit_SentencesLongerThanChunkSize_WithOverlap() {
        var chunkSizes = new int[] { 10, 30, 50 };

        // Chunk sizes are shorter the sentences most of the sentences will be split.
        for (int i = 0; i < chunkSizes.length; i++) {
            int maxWordsPerChunk = chunkSizes[i];
            var chunker = new SentenceBoundaryChunker();
            var chunks = chunker.chunk(TEST_TEXT, maxWordsPerChunk, true);
            assertThat(chunks.get(0), containsString("Word segmentation is the problem of dividing"));
            assertThat(chunks.get(chunks.size() - 1), containsString(", with solidification being a stronger norm."));
        }
    }

    public void testShortLongShortSentences_WithOverlap() {
        int maxWordsPerChunk = 40;
        var sb = new StringBuilder();
        int[] sentenceLengths = new int[] { 15, 30, 20, 5 };
        for (int l = 0; l < sentenceLengths.length; l++) {
            sb.append("SStart").append(l).append(" ");
            for (int i = 1; i < sentenceLengths[l] - 1; i++) {
                sb.append(i).append(' ');
            }
            sb.append(sentenceLengths[l] - 1).append(". ");
        }

        var chunker = new SentenceBoundaryChunker();
        var chunks = chunker.chunk(sb.toString(), maxWordsPerChunk, true);
        assertThat(chunks, hasSize(5));
        assertTrue(chunks.get(0).trim().startsWith("SStart0"));  // Entire sentence
        assertTrue(chunks.get(0).trim().endsWith("."));  // Entire sentence

        assertTrue(chunks.get(1).trim().startsWith("SStart0"));  // contains previous sentence
        assertFalse(chunks.get(1).trim().endsWith("."));   // not a full sentence(s)

        assertTrue(chunks.get(2).trim().endsWith("."));
        assertTrue(chunks.get(3).trim().endsWith("."));

        assertTrue(chunks.get(4).trim().startsWith("SStart2"));  // contains previous sentence
        assertThat(chunks.get(4), containsString("SStart3"));   // last chunk contains 2 sentences
        assertTrue(chunks.get(4).trim().endsWith("."));   // full sentence(s)
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

    public void testSkipWords() {
        int numWords = 50;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numWords; i++) {
            sb.append("word").append(i).append(" ");
        }
        var text = sb.toString();

        var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
        wordIterator.setText(text);

        int start = 0;
        int pos = SentenceBoundaryChunker.skipWords(start, 3, wordIterator);
        assertThat(text.substring(pos), startsWith("word3 "));
        pos = SentenceBoundaryChunker.skipWords(pos + 1, 1, wordIterator);
        assertThat(text.substring(pos), startsWith("word4 "));
        pos = SentenceBoundaryChunker.skipWords(pos + 1, 5, wordIterator);
        assertThat(text.substring(pos), startsWith("word9 "));

        // past the end of the input
        pos = SentenceBoundaryChunker.skipWords(0, numWords + 10, wordIterator);
        assertThat(pos, greaterThan(0));
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
            SentenceBoundaryChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(maxWordsPerChunk, 0);
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
        var maxChunkSize = randomIntBetween(10, 300);
        ChunkingSettings chunkingSettings = new WordBoundaryChunkingSettings(maxChunkSize, randomIntBetween(1, maxChunkSize / 2));
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
        return chunkOverlaps(sentenceLengths, maxWordsPerChunk, false).length;
    }

    private int[] chunkOverlaps(int[] sentenceLengths, int maxWordsPerChunk, boolean includeSingleSentenceOverlap) {
        int maxOverlap = SentenceBoundaryChunker.maxWordsInOverlap(maxWordsPerChunk);

        var overlaps = new ArrayList<Integer>();
        overlaps.add(0);
        int runningWordCount = 0;
        for (int i = 0; i < sentenceLengths.length; i++) {
            if (runningWordCount + sentenceLengths[i] > maxWordsPerChunk) {
                runningWordCount = sentenceLengths[i];
                if (includeSingleSentenceOverlap && i > 0) {
                    // include what is carried over from the previous
                    int overlap = Math.min(maxOverlap, sentenceLengths[i - 1]);
                    overlaps.add(overlap);
                    runningWordCount += overlap;
                } else {
                    overlaps.add(0);
                }
            } else {
                runningWordCount += sentenceLengths[i];
            }
        }
        return overlaps.stream().mapToInt(Integer::intValue).toArray();
    }
}
