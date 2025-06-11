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
import java.util.regex.Pattern;

/**
 * Split text into chunks recursively based on a list of separator regex strings.
 * The maximum chunk size is measured in words and controlled
 * by {@code maxNumberWordsPerChunk}. For each separator the chunker will go through the following process:
 * 1. Split the text on each regex match of the separator.
 * 2. Merge consecutive chunks when it is possible to do so without exceeding the max chunk size.
 * 3. For each chunk after the merge:
 *     1. Return it if it is within the maximum chunk size.
 *     2. Repeat the process using the next separator in the list if the chunk exceeds the maximum chunk size.
 *     If there are no more separators left to try, run the {@code SentenceBoundaryChunker} with the provided
 *     max chunk size and no overlaps.
 */
public class RecursiveChunker implements Chunker {
    private final BreakIterator wordIterator;

    public RecursiveChunker() {
        wordIterator = BreakIterator.getWordInstance();
    }

    @Override
    public List<ChunkOffset> chunk(String input, ChunkingSettings chunkingSettings) {
        if (chunkingSettings instanceof RecursiveChunkingSettings recursiveChunkingSettings) {
            return chunk(input, recursiveChunkingSettings.getSeparators(), recursiveChunkingSettings.getMaxChunkSize(), 0, 0);
        } else {
            throw new IllegalArgumentException(
                Strings.format("RecursiveChunker can't use ChunkingSettings with strategy [%s]", chunkingSettings.getChunkingStrategy())
            );
        }
    }

    private List<ChunkOffset> chunk(String input, List<String> separators, int maxChunkSize, int separatorIndex, int chunkOffset) {
        if (input.length() < 2 || isChunkWithinMaxSize(buildChunkOffsetAndCount(input, 0, input.length()), maxChunkSize)) {
            return List.of(new ChunkOffset(chunkOffset, chunkOffset + input.length()));
        }

        if (separatorIndex > separators.size() - 1) {
            return chunkWithBackupChunker(input, maxChunkSize, chunkOffset);
        }

        var potentialChunks = splitTextBySeparatorRegex(input, separators.get(separatorIndex));
        var actualChunks = new ArrayList<ChunkOffset>();
        for (var potentialChunk : potentialChunks) {
            if (isChunkWithinMaxSize(potentialChunk, maxChunkSize)) {
                actualChunks.add(
                    new ChunkOffset(chunkOffset + potentialChunk.chunkOffset.start(), chunkOffset + potentialChunk.chunkOffset.end())
                );
            } else {
                actualChunks.addAll(
                    chunk(
                        input.substring(potentialChunk.chunkOffset.start(), potentialChunk.chunkOffset.end()),
                        separators,
                        maxChunkSize,
                        separatorIndex + 1,
                        chunkOffset + potentialChunk.chunkOffset.start()
                    )
                );
            }
        }

        return actualChunks;
    }

    private boolean isChunkWithinMaxSize(ChunkOffsetAndCount chunkOffsetAndCount, int maxChunkSize) {
        return chunkOffsetAndCount.wordCount <= maxChunkSize;
    }

    private ChunkOffsetAndCount buildChunkOffsetAndCount(String fullText, int chunkStart, int chunkEnd) {
        var chunkOffset = new ChunkOffset(chunkStart, chunkEnd);

        wordIterator.setText(fullText);
        return new ChunkOffsetAndCount(chunkOffset, ChunkerUtils.countWords(chunkStart, chunkEnd, wordIterator));
    }

    private List<ChunkOffsetAndCount> splitTextBySeparatorRegex(String input, String separatorRegex) {
        var pattern = Pattern.compile(separatorRegex);
        var matcher = pattern.matcher(input);

        var chunkOffsets = new ArrayList<ChunkOffsetAndCount>();
        int chunkStart = 0;
        while (matcher.find()) {
            var chunkEnd = matcher.start();
            if (chunkStart < chunkEnd) {
                chunkOffsets.add(buildChunkOffsetAndCount(input, chunkStart, chunkEnd));
            }
            chunkStart = matcher.start();
        }

        if (chunkStart < input.length()) {
            chunkOffsets.add(buildChunkOffsetAndCount(input, chunkStart, input.length()));
        }

        return chunkOffsets;
    }

    private List<ChunkOffset> chunkWithBackupChunker(String input, int maxChunkSize, int chunkOffset) {
        var chunks = new SentenceBoundaryChunker().chunk(input, new SentenceBoundaryChunkingSettings(maxChunkSize, 0));
        var chunksWithOffsets = new ArrayList<ChunkOffset>();
        for (var chunk : chunks) {
            chunksWithOffsets.add(new ChunkOffset(chunk.start() + chunkOffset, chunk.end() + chunkOffset));
        }
        return chunksWithOffsets;
    }

    private record ChunkOffsetAndCount(ChunkOffset chunkOffset, int wordCount) {}
}
