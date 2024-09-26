/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import com.ibm.icu.text.BreakIterator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.ChunkingSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Split text into chunks aligned on sentence boundaries.
 * The maximum chunk size is measured in words and controlled
 * by {@code maxNumberWordsPerChunk}. Sentences are combined
 * greedily until adding the next sentence would exceed
 * {@code maxNumberWordsPerChunk}, at which point a new chunk
 * is created. If an individual sentence is longer than
 * {@code maxNumberWordsPerChunk} it is split on word boundary with
 * overlap.
 */
public class SentenceBoundaryChunker implements Chunker {

    private final BreakIterator sentenceIterator;
    private final BreakIterator wordIterator;

    public SentenceBoundaryChunker() {
        sentenceIterator = BreakIterator.getSentenceInstance(Locale.ROOT);
        wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
    }

    /**
     * Break the input text into small chunks on sentence boundaries.
     *
     * @param input Text to chunk
     * @param chunkingSettings Chunking settings that define maxNumberWordsPerChunk
     * @return The input text chunked
     */
    @Override
    public List<String> chunk(String input, ChunkingSettings chunkingSettings) {
        if (chunkingSettings instanceof SentenceBoundaryChunkingSettings sentenceBoundaryChunkingSettings) {
            return chunk(input, sentenceBoundaryChunkingSettings.maxChunkSize);
        } else {
            throw new IllegalArgumentException(
                Strings.format(
                    "SentenceBoundaryChunker can't use ChunkingSettings with strategy [%s]",
                    chunkingSettings.getChunkingStrategy()
                )
            );
        }
    }

    /**
     * Break the input text into small chunks on sentence boundaries.
     *
     * @param input Text to chunk
     * @param maxNumberWordsPerChunk Maximum size of the chunk
     * @return The input text chunked
     */
    public List<String> chunk(String input, int maxNumberWordsPerChunk) {
        var chunks = new ArrayList<String>();

        sentenceIterator.setText(input);
        wordIterator.setText(input);

        int chunkStart = 0;
        int chunkEnd = 0;
        int sentenceStart = 0;
        int chunkWordCount = 0;

        int boundary = sentenceIterator.next();

        while (boundary != BreakIterator.DONE) {
            int sentenceEnd = sentenceIterator.current();
            int countWordsInSentence = countWords(sentenceStart, sentenceEnd);

            if (chunkWordCount + countWordsInSentence > maxNumberWordsPerChunk) {
                // over the max chunk size, roll back to the last sentence

                if (chunkWordCount > 0) {
                    // add a new chunk containing all the input up to this sentence
                    chunks.add(input.substring(chunkStart, chunkEnd));
                    chunkStart = chunkEnd;
                    chunkWordCount = countWordsInSentence; // the next chunk will contain this sentence
                }

                if (countWordsInSentence > maxNumberWordsPerChunk) {
                    // This sentence is bigger than the max chunk size.
                    // Split the sentence on the word boundary
                    var sentenceSplits = splitLongSentence(
                        input.substring(chunkStart, sentenceEnd),
                        maxNumberWordsPerChunk,
                        overlapForChunkSize(maxNumberWordsPerChunk)
                    );

                    int i = 0;
                    for (; i < sentenceSplits.size() - 1; i++) {
                        // Because the substring was passed to splitLongSentence()
                        // the returned positions need to be offset by chunkStart
                        chunks.add(input.substring(chunkStart + sentenceSplits.get(i).start(), chunkStart + sentenceSplits.get(i).end()));
                    }
                    // The final split is partially filled.
                    // Set the next chunk start to the beginning of the
                    // final split of the long sentence.
                    chunkStart = chunkStart + sentenceSplits.get(i).start();  // start pos needs to be offset by chunkStart
                    chunkWordCount = sentenceSplits.get(i).wordCount();
                }
            } else {
                chunkWordCount += countWordsInSentence;
            }

            sentenceStart = sentenceEnd;
            chunkEnd = sentenceEnd;

            boundary = sentenceIterator.next();
        }

        if (chunkWordCount > 0) {
            chunks.add(input.substring(chunkStart));
        }

        return chunks;
    }

    static List<WordBoundaryChunker.ChunkPosition> splitLongSentence(String text, int maxNumberOfWords, int overlap) {
        return new WordBoundaryChunker().chunkPositions(text, maxNumberOfWords, overlap);
    }

    private int countWords(int start, int end) {
        return countWords(start, end, this.wordIterator);
    }

    // Exposed for testing. wordIterator should have had
    // setText() applied before using this function.
    static int countWords(int start, int end, BreakIterator wordIterator) {
        assert start < end;
        wordIterator.preceding(start); // start of the current word

        int boundary = wordIterator.current();
        int wordCount = 0;
        while (boundary != BreakIterator.DONE && boundary <= end) {
            int wordStatus = wordIterator.getRuleStatus();
            if (wordStatus != BreakIterator.WORD_NONE) {
                wordCount++;
            }
            boundary = wordIterator.next();
        }

        return wordCount;
    }

    private static int overlapForChunkSize(int chunkSize) {
        return (chunkSize - 1) / 2;
    }
}
