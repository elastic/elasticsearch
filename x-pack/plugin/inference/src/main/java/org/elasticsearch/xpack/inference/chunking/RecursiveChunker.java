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
        if (input.length() < 2 || isChunkWithinMaxSize(input, new ChunkOffset(0, input.length()), maxChunkSize)) {
            return List.of(new ChunkOffset(chunkOffset, chunkOffset + input.length()));
        }

        if (separatorIndex > separators.size() - 1) {
            return chunkWithBackupChunker(input, maxChunkSize, chunkOffset);
        }

        var potentialChunks = mergeChunkOffsetsUpToMaxChunkSize(
            input,
            splitTextBySeparatorRegex(input, separators.get(separatorIndex)),
            maxChunkSize
        );
        var actualChunks = new ArrayList<ChunkOffset>();
        for (var potentialChunk : potentialChunks) {
            if (isChunkWithinMaxSize(input, potentialChunk, maxChunkSize)) {
                actualChunks.add(new ChunkOffset(chunkOffset + potentialChunk.start(), chunkOffset + potentialChunk.end()));
            } else {
                actualChunks.addAll(
                    chunk(
                        input.substring(potentialChunk.start(), potentialChunk.end()),
                        separators,
                        maxChunkSize,
                        separatorIndex + 1,
                        chunkOffset + potentialChunk.start()
                    )
                );
            }
        }

        return actualChunks;
    }

    private boolean isChunkWithinMaxSize(String fullText, ChunkOffset chunk, int maxChunkSize) {
        wordIterator.setText(fullText);
        return ChunkerUtils.countWords(chunk.start(), chunk.end(), wordIterator) <= maxChunkSize;
    }

    private List<ChunkOffset> splitTextBySeparatorRegex(String input, String separatorRegex) {
        var pattern = Pattern.compile(separatorRegex);
        var matcher = pattern.matcher(input);

        var chunkOffsets = new ArrayList<ChunkOffset>();
        int chunkStart = 0;
        int searchStart = 0;
        while (matcher.find(searchStart)) {
            var chunkEnd = matcher.start();
            if (chunkStart < chunkEnd) {
                chunkOffsets.add(new ChunkOffset(chunkStart, chunkEnd));
            }
            chunkStart = matcher.start();
            searchStart = matcher.end();
        }

        if (chunkStart < input.length()) {
            chunkOffsets.add(new ChunkOffset(chunkStart, input.length()));
        }

        return chunkOffsets;
    }

    private List<ChunkOffset> mergeChunkOffsetsUpToMaxChunkSize(String input, List<ChunkOffset> chunkOffsets, int maxChunkSize) {
        if (chunkOffsets.size() < 2) {
            return chunkOffsets;
        }

        List<ChunkOffset> mergedOffsets = new ArrayList<>();
        var mergedChunk = chunkOffsets.getFirst();
        for (int i = 1; i < chunkOffsets.size(); i++) {
            var potentialMergedChunk = new ChunkOffset(mergedChunk.start(), chunkOffsets.get(i).end());
            if (isChunkWithinMaxSize(input, potentialMergedChunk, maxChunkSize)) {
                mergedChunk = potentialMergedChunk;
            } else {
                mergedOffsets.add(mergedChunk);
                mergedChunk = chunkOffsets.get(i);
            }

            if (i == chunkOffsets.size() - 1) {
                mergedOffsets.add(mergedChunk);
            }
        }
        return mergedOffsets;
    }

    private List<ChunkOffset> chunkWithBackupChunker(String input, int maxChunkSize, int chunkOffset) {
        var chunks = new SentenceBoundaryChunker().chunk(input, new SentenceBoundaryChunkingSettings(maxChunkSize, 0));
        var chunksWithOffsets = new ArrayList<ChunkOffset>();
        for (var chunk : chunks) {
            chunksWithOffsets.add(new ChunkOffset(chunk.start() + chunkOffset, chunk.end() + chunkOffset));
        }
        return chunksWithOffsets;
    }
}
