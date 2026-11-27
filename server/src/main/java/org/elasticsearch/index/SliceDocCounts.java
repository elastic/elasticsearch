/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * How many documents the largest slices of an index hold.
 * <p>
 * Counts are doc ids held, including documents deleted but not yet merged away, because that is what
 * {@link IndexWriter#MAX_DOCS} counts; {@link #liveDocs} answers how many of a slice's documents remain. A count is
 * a lower bound, short by the documents a slice has in the ranges straddling its boundaries, at most twice the skip
 * interval per segment.
 */
public final class SliceDocCounts {

    /** Documents {@link #liveDocCount} counts at a time, so its allocation does not grow with the slice. */
    private static final int LIVE_DOCS_WINDOW = 1 << 16;

    private SliceDocCounts() {}

    /** Doc ids a slice holds across an index. */
    public record Entry(BytesRef sliceKey, long heldDocs) {}

    /**
     * The slices holding at least {@code minFraction} of the index's doc ids, largest first. Costs two passes over
     * each segment's skip index and holds at most {@code 1 / minFraction} candidates per segment, so neither depends
     * on how many slices the index has. A slice within a skip interval of that fraction may be left out, for the same
     * reason its count is a lower bound.
     */
    public static List<Entry> largest(IndexReader reader, String field, double minFraction) throws IOException {
        if (minFraction <= 0.0 || minFraction > 1.0) {
            throw new IllegalArgumentException("[minFraction] must be in (0, 1] but was [" + minFraction + "]");
        }
        final List<BytesRef> candidates = candidates(reader, field, minFraction);
        if (candidates.isEmpty()) {
            return List.of();
        }
        final long[] heldDocs = new long[candidates.size()];
        long totalDocs = 0L;
        for (LeafReaderContext context : reader.leaves()) {
            totalDocs += context.reader().maxDoc();
            accumulate(context.reader(), field, candidates, heldDocs);
        }
        final long required = (long) Math.ceil(minFraction * totalDocs);
        final List<Entry> entries = new ArrayList<>(candidates.size());
        for (int i = 0; i < candidates.size(); i++) {
            if (heldDocs[i] >= required) {
                entries.add(new Entry(candidates.get(i), heldDocs[i]));
            }
        }
        entries.sort(Comparator.comparingLong(Entry::heldDocs).reversed());
        return entries;
    }

    /**
     * The slices that could hold {@code minFraction} of the index: one holding that fraction overall must hold it in
     * some segment, or it would fall short in every segment and so in their sum, and fewer than
     * {@code 1 / minFraction} slices can hold that much of one segment.
     */
    private static List<BytesRef> candidates(IndexReader reader, String field, double minFraction) throws IOException {
        final TreeSet<BytesRef> keys = new TreeSet<>();
        for (LeafReaderContext context : reader.leaves()) {
            final LeafReader leaf = context.reader();
            final long leafRequired = (long) Math.ceil(minFraction * leaf.maxDoc());
            final List<Integer> sliceIds = new ArrayList<>();
            SliceRanges.scan(leaf, field, range -> {
                if (range.isSingleSlice() && range.docCount() >= leafRequired) {
                    sliceIds.add(range.minSliceId());
                }
            });
            if (sliceIds.isEmpty()) {
                continue;
            }
            final SliceRanges.Slices slices = SliceRanges.Slices.of(leaf, field);
            if (slices == null) {
                continue;
            }
            for (int sliceId : sliceIds) {
                keys.add(BytesRef.deepCopyOf(slices.key(sliceId)));
            }
        }
        return new ArrayList<>(keys);
    }

    /** Adds the doc ids each candidate holds in {@code leaf} to {@code heldDocs}. */
    private static void accumulate(LeafReader leaf, String field, List<BytesRef> candidates, long[] heldDocs) throws IOException {
        final SliceRanges.Slices slices = SliceRanges.Slices.of(leaf, field);
        if (slices == null) {
            return;
        }
        // Slice ids are segment local, so the candidates are resolved against this segment.
        final int[] wanted = new int[candidates.size()];
        boolean any = false;
        for (int i = 0; i < candidates.size(); i++) {
            wanted[i] = slices.id(candidates.get(i));
            any |= wanted[i] >= 0;
        }
        if (any == false) {
            return;
        }
        SliceRanges.scan(leaf, field, range -> {
            if (range.isSingleSlice() == false) {
                return;
            }
            for (int i = 0; i < wanted.length; i++) {
                if (wanted[i] == range.minSliceId()) {
                    heldDocs[i] += range.docCount();
                    break;
                }
            }
        });
    }

    /**
     * How many of a slice's documents are not deleted. Reads the live docs, at a cost proportional to the doc ids the
     * slice holds, so it is meant for slices a caller has already singled out.
     */
    public static long liveDocs(IndexReader reader, String field, BytesRef sliceKey) throws IOException {
        long live = 0L;
        for (LeafReaderContext context : reader.leaves()) {
            final LeafReader leaf = context.reader();
            final SliceRanges.Slices slices = SliceRanges.Slices.of(leaf, field);
            if (slices == null) {
                continue;
            }
            final int sliceId = slices.id(sliceKey);
            if (sliceId < 0) {
                continue;
            }
            final long[] leafLive = new long[1];
            SliceRanges.scan(leaf, field, range -> {
                if (range.isSingleSlice() && range.minSliceId() == sliceId) {
                    leafLive[0] += liveDocCount(leaf, range.minDoc(), range.maxDoc());
                }
            });
            live += leafLive[0];
        }
        return live;
    }

    /** How many documents in {@code [minDoc, maxDoc]} of {@code leaf} are not deleted. */
    static long liveDocCount(LeafReader leaf, int minDoc, int maxDoc) {
        final Bits liveDocs = leaf.getLiveDocs();
        if (liveDocs == null) {
            return maxDoc - minDoc + 1L;
        }
        final FixedBitSet window = new FixedBitSet(LIVE_DOCS_WINDOW);
        long live = 0L;
        for (int from = minDoc; from <= maxDoc; from += LIVE_DOCS_WINDOW) {
            final int length = Math.min(LIVE_DOCS_WINDOW, maxDoc - from + 1);
            window.clear();
            window.set(0, length);
            liveDocs.applyMask(window, from);
            live += window.cardinality();
        }
        return live;
    }
}
