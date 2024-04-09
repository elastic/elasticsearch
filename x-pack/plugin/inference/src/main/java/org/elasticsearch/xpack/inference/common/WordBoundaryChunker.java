/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import com.ibm.icu.text.BreakIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Breaks text into smaller strings or chunks on Word boundaries.
 * Whitespace is preserved and included in the start of the
 * following chunk not the end of the chunk. If the chunk ends
 * on a punctuation mark the punctuation is including in the
 * next chunk.
 *
 * The overlap value must be > (chunkSize /2) to avoid the
 * complexity of tracking the start positions of multiple
 * chunks within the chunk.
 */
public class WordBoundaryChunker {

    private BreakIterator wordIterator;

    public WordBoundaryChunker() {
        wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
    }

    /**
     * Break the input text into small chunks as dictated
     * by the chunking parameters
     * @param input Text to chunk
     * @param chunkSize The number of words in each chunk
     * @param overlap The number of words to overlap each chunk.
     *                Can be 0 but must be non-negative.
     * @return List of chunked text
     */
    public List<String> chunk(String input, int chunkSize, int overlap) {
        if (overlap > 0 && overlap > chunkSize / 2) {
            throw new IllegalArgumentException(
                "Invalid chunking parameters, overlap ["
                    + overlap
                    + "] must be < chunk size / 2 ["
                    + chunkSize
                    + " / 2 = "
                    + chunkSize / 2
                    + "]"
            );
        }

        if (overlap < 0) {
            throw new IllegalArgumentException("Invalid chunking parameters, overlap [" + overlap + "] must be >= 0");
        }

        if (input.isEmpty()) {
            return List.of("");
        }

        var chunks = new ArrayList<String>();

        final int notOverlappingSize = chunkSize - overlap;
        int wordInChunkCount = 0;
        int nextWindowStart = 0;

        wordIterator.setText(input);
        int boundary = wordIterator.next();

        // The first chunk is 0..chunksize. The following chunks
        // incorporate overlap and calculate the start offset differently
        while (boundary != BreakIterator.DONE) {
            if (wordIterator.getRuleStatus() != BreakIterator.WORD_NONE) {
                wordInChunkCount++;

                if (wordInChunkCount == notOverlappingSize) {
                    nextWindowStart = boundary;
                }
                if (wordInChunkCount >= chunkSize) {
                    chunks.add(input.substring(0, boundary));
                    break;
                }
            }
            boundary = wordIterator.next();
        }

        if (boundary == BreakIterator.DONE && wordInChunkCount < chunkSize) {
            chunks.add(input);
            return chunks;
        }

        wordInChunkCount = 0;
        int windowStart = nextWindowStart;
        boundary = wordIterator.next();

        // Split the remaining text into chunkSize strings.
        // Due to overlap the next chunk starting point is inside
        // the current chunk and must be track
        while (boundary != BreakIterator.DONE) {
            // this check is explicitly for the case where overlap == chunkSize / 2
            // meaning the next window start occurs before any new words are seen
            if (notOverlappingSize - overlap == 0 && wordInChunkCount == 0) {
                nextWindowStart = boundary;
            }

            if (wordIterator.getRuleStatus() != BreakIterator.WORD_NONE) {
                wordInChunkCount++;

                if (wordInChunkCount == notOverlappingSize - overlap) {
                    nextWindowStart = boundary;
                }
                if (wordInChunkCount == notOverlappingSize) {
                    chunks.add(input.substring(windowStart, boundary));
                    wordInChunkCount = 0;
                    windowStart = nextWindowStart;
                }
            }

            boundary = wordIterator.next();
        }

        if (wordInChunkCount > 0) {
            chunks.add(input.substring(windowStart));
        }
        return chunks;
    }
}
