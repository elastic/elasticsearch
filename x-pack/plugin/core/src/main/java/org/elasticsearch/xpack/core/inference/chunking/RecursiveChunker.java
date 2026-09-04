/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.chunking;

import com.ibm.icu.text.BreakIterator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.ChunkingSettings;

import java.util.ArrayDeque;
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
                recursiveChunkingSettings.maxChunkSize()
            );
        } else {
            throw new IllegalArgumentException(
                Strings.format("RecursiveChunker can't use ChunkingSettings with strategy [%s]", chunkingSettings.getChunkingStrategy())
            );
        }
    }

    /**
     * Iteratively splits {@code initialOffset} into chunks that each fit within {@code maxChunkSize},
     * trying separators in list order and falling back to the sentence boundary chunker when all
     * separators are exhausted.
     * <p>
     * An explicit worklist ({@link ArrayDeque}) replaces what was previously a recursive call,
     * so the JVM call-stack depth is constant regardless of the number of separators.
     * Chunks that already fit are emitted immediately; only oversized chunks are pushed
     * back onto the worklist for further splitting with the next separator.
     * Oversized chunks are pushed in reverse order so that they are popped (and therefore
     * emitted) in document order.
     */
    private List<ChunkOffset> chunk(String input, ChunkOffset initialOffset, List<String> separators, int maxChunkSize) {
        if (initialOffset.start() == initialOffset.end()) {
            return List.of(initialOffset);
        }

        var initialChunk = buildChunkOffsetAndCount(input, initialOffset);
        if (isChunkWithinMaxSize(initialChunk, maxChunkSize)) {
            return List.of(initialOffset);
        }

        var chunks = new ArrayList<ChunkOffset>();
        var worklist = new ArrayDeque<PendingChunk>();
        worklist.push(new PendingChunk(initialChunk, 0));

        while (worklist.isEmpty() == false) {
            var pending = worklist.pop();
            var chunkOffsetAndCount = pending.chunkOffsetAndCount();
            var offset = chunkOffsetAndCount.chunkOffset();
            int separatorIndex = pending.separatorIndex();

            // Emit directly if the chunk is empty or already fits within the limit.
            if (offset.start() == offset.end() || isChunkWithinMaxSize(chunkOffsetAndCount, maxChunkSize)) {
                chunks.add(offset);
                continue;
            }

            if (separatorIndex >= separators.size()) {
                chunks.addAll(chunkWithBackupChunker(input, offset, maxChunkSize));
                continue;
            }

            var potentialChunks = mergeChunkOffsetsUpToMaxChunkSize(
                splitTextBySeparatorRegex(input, offset, separators.get(separatorIndex)),
                maxChunkSize
            );

            // Emit fit chunks immediately; push oversized chunks in reverse so they pop in document order.
            for (int i = 0; i < potentialChunks.size(); i++) {
                var potentialChunk = potentialChunks.get(i);
                if (isChunkWithinMaxSize(potentialChunk, maxChunkSize)) {
                    chunks.add(potentialChunk.chunkOffset());
                } else {
                    // All remaining chunks from this split need further processing.
                    // Push them in reverse order so the earliest chunk is popped first.
                    for (int j = potentialChunks.size() - 1; j >= i; j--) {
                        worklist.push(new PendingChunk(potentialChunks.get(j), separatorIndex + 1));
                    }
                    break;
                }
            }
        }

        return chunks;
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
        var matcher = pattern.matcher(input).region(offset.start(), offset.end());

        var chunkOffsets = new ArrayList<ChunkOffsetAndCount>();
        int chunkStart = offset.start();
        while (matcher.find()) {
            var chunkEnd = matcher.start();

            if (chunkStart < chunkEnd) {
                chunkOffsets.add(buildChunkOffsetAndCount(input, new ChunkOffset(chunkStart, chunkEnd)));
            }
            chunkStart = chunkEnd;
        }

        if (chunkStart < offset.end()) {
            chunkOffsets.add(buildChunkOffsetAndCount(input, new ChunkOffset(chunkStart, offset.end())));
        }

        return chunkOffsets;
    }

    private List<ChunkOffsetAndCount> mergeChunkOffsetsUpToMaxChunkSize(List<ChunkOffsetAndCount> chunkOffsets, int maxChunkSize) {
        if (chunkOffsets.size() < 2) {
            return chunkOffsets;
        }

        List<ChunkOffsetAndCount> mergedOffsetsAndCounts = new ArrayList<>();
        var mergedChunk = chunkOffsets.getFirst();
        for (int i = 1; i < chunkOffsets.size(); i++) {
            var chunkOffsetAndCountToMerge = chunkOffsets.get(i);
            var potentialMergedChunk = new ChunkOffsetAndCount(
                new ChunkOffset(mergedChunk.chunkOffset.start(), chunkOffsetAndCountToMerge.chunkOffset.end()),
                mergedChunk.wordCount + chunkOffsetAndCountToMerge.wordCount
            );
            if (isChunkWithinMaxSize(potentialMergedChunk, maxChunkSize)) {
                mergedChunk = potentialMergedChunk;
            } else {
                mergedOffsetsAndCounts.add(mergedChunk);
                mergedChunk = chunkOffsets.get(i);
            }

            if (i == chunkOffsets.size() - 1) {
                mergedOffsetsAndCounts.add(mergedChunk);
            }
        }
        return mergedOffsetsAndCounts;
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

    /**
     * A chunk that still needs to be checked or further split, together with the index of the
     * next separator to try if it turns out to be too large. The word count is carried alongside
     * the offset so that fitness checks on pop do not require re-counting.
     */
    private record PendingChunk(ChunkOffsetAndCount chunkOffsetAndCount, int separatorIndex) {}
}
