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
 * 2. For each chunk after the merge:
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
            return chunk(
                input,
                new ChunkOffset(0, input.length()),
                recursiveChunkingSettings.getSeparators(),
                recursiveChunkingSettings.getMaxChunkSize(),
                0
            );
        } else {
            throw new IllegalArgumentException(
                Strings.format("RecursiveChunker can't use ChunkingSettings with strategy [%s]", chunkingSettings.getChunkingStrategy())
            );
        }
    }

    private List<ChunkOffset> chunk(String input, ChunkOffset offset, List<String> separators, int maxChunkSize, int separatorIndex) {
        if (offset.start() == offset.end() || isChunkWithinMaxSize(buildChunkOffsetAndCount(input, offset), maxChunkSize)) {
            return List.of(offset);
        }

        if (separatorIndex > separators.size() - 1) {
            return chunkWithBackupChunker(input, offset, maxChunkSize);
        }

        var potentialChunks = splitTextBySeparatorRegex(input, offset, separators.get(separatorIndex));
        var actualChunks = new ArrayList<ChunkOffset>();
        for (var potentialChunk : potentialChunks) {
            if (isChunkWithinMaxSize(potentialChunk, maxChunkSize)) {
                actualChunks.add(potentialChunk.chunkOffset());
            } else {
                actualChunks.addAll(chunk(input, potentialChunk.chunkOffset(), separators, maxChunkSize, separatorIndex + 1));
            }
        }

        return actualChunks;
    }

    private boolean isChunkWithinMaxSize(ChunkOffsetAndCount chunkOffsetAndCount, int maxChunkSize) {
        return chunkOffsetAndCount.wordCount <= maxChunkSize;
    }

    private ChunkOffsetAndCount buildChunkOffsetAndCount(String fullText, ChunkOffset offset) {
        wordIterator.setText(fullText);
        return new ChunkOffsetAndCount(offset, ChunkerUtils.countWords(offset.start(), offset.end(), wordIterator));
    }

    private List<ChunkOffsetAndCount> splitTextBySeparatorRegex(String input, ChunkOffset offset, String separatorRegex) {
        var pattern = Pattern.compile(separatorRegex, Pattern.MULTILINE);
        var matcher = pattern.matcher(input);

        var chunkOffsets = new ArrayList<ChunkOffsetAndCount>();
        int chunkStart = offset.start();
        int searchStart = offset.start();
        while (matcher.find(searchStart)) {
            var chunkEnd = matcher.start();
            if (chunkEnd >= offset.end()) {
                break; // No more matches within the chunk offset
            }

            if (chunkStart < chunkEnd) {
                chunkOffsets.add(buildChunkOffsetAndCount(input, new ChunkOffset(chunkStart, chunkEnd)));
            }
            chunkStart = chunkEnd;
            searchStart = matcher.end();
        }

        if (chunkStart < offset.end()) {
            chunkOffsets.add(buildChunkOffsetAndCount(input, new ChunkOffset(chunkStart, offset.end())));
        }

        return chunkOffsets;
    }

    private List<ChunkOffset> chunkWithBackupChunker(String input, ChunkOffset offset, int maxChunkSize) {
        var chunks = new SentenceBoundaryChunker().chunk(
            input.substring(offset.start(), offset.end()),
            new SentenceBoundaryChunkingSettings(maxChunkSize, 0)
        );
        var chunksWithOffsets = new ArrayList<ChunkOffset>();
        for (var chunk : chunks) {
            chunksWithOffsets.add(new ChunkOffset(chunk.start() + offset.start(), chunk.end() + offset.start()));
        }
        return chunksWithOffsets;
    }

    private record ChunkOffsetAndCount(ChunkOffset chunkOffset, int wordCount) {}
}
