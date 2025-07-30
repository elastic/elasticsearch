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
import java.util.stream.Collectors;

/**
 * Breaks text into smaller strings or chunks on Word boundaries.
 * Whitespace is preserved and included in the start of the
 * following chunk not the end of the chunk. If the chunk ends
 * on a punctuation mark the punctuation is included in the
 * next chunk.
 *
 * The overlap value must be > (chunkSize /2) to avoid the
 * complexity of tracking the start positions of multiple
 * chunks within the chunk.
 */
public class WordBoundaryChunker implements Chunker {

    private BreakIterator wordIterator;

    public WordBoundaryChunker() {
        wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
    }

    record ChunkPosition(ChunkOffset offsets, int wordCount) {}

    /**
     * Break the input text into small chunks as dictated
     * by the chunking parameters
     * @param input Text to chunk
     * @param chunkingSettings The chunking settings that configure chunkSize and overlap
     * @return List of chunked text
     */
    @Override
    public List<ChunkOffset> chunk(String input, ChunkingSettings chunkingSettings) {
        if (chunkingSettings instanceof WordBoundaryChunkingSettings wordBoundaryChunkerSettings) {
            return chunk(input, wordBoundaryChunkerSettings.maxChunkSize, wordBoundaryChunkerSettings.overlap);
        } else {
            throw new IllegalArgumentException(
                Strings.format("WordBoundaryChunker can't use ChunkingSettings with strategy [%s]", chunkingSettings.getChunkingStrategy())
            );
        }
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
    public List<ChunkOffset> chunk(String input, int chunkSize, int overlap) {
        var chunkPositions = chunkPositions(input, chunkSize, overlap);
        return chunkPositions.stream().map(ChunkPosition::offsets).collect(Collectors.toList());
    }

    /**
     * Chunk using the same strategy as {@link #chunk(String, int, int)}
     * but return the chunk start and end offsets in the {@code input} string
     * @param input Text to chunk
     * @param chunkSize The number of words in each chunk
     * @param overlap The number of words to overlap each chunk.
     *                Can be 0 but must be non-negative.
     * @return List of chunked text positions
     */
    List<ChunkPosition> chunkPositions(String input, int chunkSize, int overlap) {
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

        var chunkPositions = new ArrayList<ChunkPosition>();

        // This position in the chunk is where the next overlapping chunk will start
        final int chunkSizeLessOverlap = chunkSize - overlap;
        // includes the count of words from the overlap portion in the previous chunk
        int wordsInChunkCountIncludingOverlap = 0;
        int nextWindowStart = 0;
        int windowStart = 0;
        int wordsSinceStartWindowWasMarked = 0;

        wordIterator.setText(input);
        int boundary = wordIterator.next();

        while (boundary != BreakIterator.DONE) {
            if (wordIterator.getRuleStatus() != BreakIterator.WORD_NONE) {
                wordsInChunkCountIncludingOverlap++;
                wordsSinceStartWindowWasMarked++;

                if (wordsInChunkCountIncludingOverlap >= chunkSize) {
                    chunkPositions.add(new ChunkPosition(new ChunkOffset(windowStart, boundary), wordsInChunkCountIncludingOverlap));
                    wordsInChunkCountIncludingOverlap = overlap;

                    if (overlap == 0) {
                        nextWindowStart = boundary;
                    }

                    windowStart = nextWindowStart;
                }

                if (wordsSinceStartWindowWasMarked == chunkSizeLessOverlap) {
                    nextWindowStart = boundary;
                    wordsSinceStartWindowWasMarked = 0;
                }
            }
            boundary = wordIterator.next();
        }

        // Get the last chunk that was shorter than the required chunk size
        // if it ends on a boundary than the count should equal overlap in which case
        // we can ignore it, unless this is the first chunk in which case we want to add it
        if (wordsInChunkCountIncludingOverlap > overlap || chunkPositions.isEmpty()) {
            chunkPositions.add(new ChunkPosition(new ChunkOffset(windowStart, input.length()), wordsInChunkCountIncludingOverlap));
        }

        return chunkPositions;
    }
}
