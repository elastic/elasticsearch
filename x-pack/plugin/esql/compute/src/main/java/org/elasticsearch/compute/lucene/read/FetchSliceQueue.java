/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.lucene.query.LuceneSliceQueue;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * The documents of a fetch request cut into slices that the drivers of the request take as they go, like the drivers of
 * the query phase take slices from a {@link LuceneSliceQueue}. A driver that finished takes the slices that are left, so
 * several drivers load a shard that holds most of the documents, and no driver waits on another.
 * <p>
 * A slice holds at most {@link #SLICE_DOCS} documents of one segment of one shard, the rows of one page. The documents of
 * a segment go in as few slices as that allows, of sizes that differ by at most one, so no slice holds a small leftover.
 * The slices are numbered in request order, so their rows, one slice after the other, are the rows of the request.
 * <p>
 * A driver keeps to the slice after the one it loaded, so it stays in its segment and keeps its readers. When another
 * driver took that slice, it starts on a shard no driver started, then on a segment no driver started, and only then
 * takes any slice that is left.
 */
public final class FetchSliceQueue {
    /**
     * The most documents in a slice. Smaller slices spread a shard over more drivers. Larger ones build readers for fewer
     * pages. Smaller slices for documents that read their stored fields at random measured no faster, so the size is the
     * same for all documents.
     */
    public static final int SLICE_DOCS = 64;

    /**
     * Documents of one segment of one shard, sorted by doc, without duplicates. The {@code docs} array belongs to this slice
     * alone and nothing writes it, so the page of the slice wraps it without a copy.
     *
     * @param position the place of the slice in request order
     * @param shard    the slot of the shard in the shard references the pages take
     */
    public record Slice(int position, int shard, int segment, int[] docs) {
        @Override
        public String toString() {
            return "Slice[position=" + position + ", shard=" + shard + ", segment=" + segment + ", docs=" + docs.length + "]";
        }
    }

    private final AtomicReferenceArray<Slice> slices;
    private final int docCount;
    /**
     * The first slice of each shard. A driver without a next slice starts on one of these first, so the drivers spread
     * over the shards.
     */
    private final Queue<Integer> shardHeads = ConcurrentCollections.newQueue();
    /**
     * The first slice of each segment, except the first segment of a shard.
     */
    private final Queue<Integer> segmentHeads = ConcurrentCollections.newQueue();
    /**
     * Every other slice. A driver takes one of these only when no shard or segment is left to start on.
     */
    private final Queue<Integer> otherSlices = ConcurrentCollections.newQueue();

    private FetchSliceQueue(
        List<Slice> slices,
        int docCount,
        List<Integer> shardHeads,
        List<Integer> segmentHeads,
        List<Integer> otherSlices
    ) {
        this.slices = new AtomicReferenceArray<>(slices.toArray(Slice[]::new));
        this.docCount = docCount;
        this.shardHeads.addAll(shardHeads);
        this.segmentHeads.addAll(segmentHeads);
        this.otherSlices.addAll(otherSlices);
    }

    /**
     * Cuts the documents of a request into slices.
     *
     * @param shards the documents of each shard, in request order
     */
    public static FetchSliceQueue create(List<FetchDocsSourceOperator.ShardDocs> shards) {
        List<Slice> slices = new ArrayList<>();
        int docCount = 0;
        List<Integer> shardHeads = new ArrayList<>();
        List<Integer> segmentHeads = new ArrayList<>();
        List<Integer> otherSlices = new ArrayList<>();
        for (FetchDocsSourceOperator.ShardDocs shard : shards) {
            int[] segments = shard.segments();
            int[] docs = shard.docs();
            docCount += docs.length;
            int runStart = 0;
            while (runStart < docs.length) {
                int runEnd = runStart + 1;
                while (runEnd < docs.length && segments[runEnd] == segments[runStart]) {
                    runEnd++;
                }
                int runDocs = runEnd - runStart;
                int runSlices = Math.ceilDiv(runDocs, SLICE_DOCS);
                for (int s = 0; s < runSlices; s++) {
                    int from = runStart + (int) ((long) runDocs * s / runSlices);
                    int to = runStart + (int) ((long) runDocs * (s + 1) / runSlices);
                    int position = slices.size();
                    (from == 0 ? shardHeads : s == 0 ? segmentHeads : otherSlices).add(position);
                    slices.add(new Slice(position, shard.shard(), segments[runStart], Arrays.copyOfRange(docs, from, to)));
                }
                runStart = runEnd;
            }
        }
        return new FetchSliceQueue(slices, docCount, shardHeads, segmentHeads, otherSlices);
    }

    /**
     * The next slice for a driver that loaded {@code prev}, or {@code null} once every slice went to a driver. Each slice
     * goes to one driver only.
     *
     * @param prev the slice the driver loaded last, {@code null} for its first
     */
    @Nullable
    public Slice nextSlice(@Nullable Slice prev) {
        if (prev != null && prev.position() + 1 < slices.length()) {
            Slice next = slices.getAndSet(prev.position() + 1, null);
            if (next != null) {
                return next;
            }
        }
        Slice slice = take(shardHeads);
        if (slice == null) {
            slice = take(segmentHeads);
        }
        if (slice == null) {
            slice = take(otherSlices);
        }
        return slice;
    }

    /**
     * The first slice of {@code positions} that no driver took yet, or {@code null}.
     */
    @Nullable
    private Slice take(Queue<Integer> positions) {
        Integer position;
        while ((position = positions.poll()) != null) {
            Slice slice = slices.getAndSet(position, null);
            if (slice != null) {
                return slice;
            }
        }
        return null;
    }

    public int sliceCount() {
        return slices.length();
    }

    /**
     * The documents of all slices.
     */
    public int docCount() {
        return docCount;
    }
}
