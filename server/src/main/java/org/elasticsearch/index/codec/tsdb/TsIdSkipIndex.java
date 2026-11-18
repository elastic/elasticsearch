/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Arrays;

/**
 * An in-heap copy of the level 0 intervals of the {@code _tsid} doc values skipper of one segment.
 * <p>
 * The documents of a time series segment are sorted by {@code _tsid} ordinal, so the maximum {@code _tsid} ordinal of the level 0
 * intervals never decreases. This lets {@link #intervalFor} find the first interval that can hold a given ordinal with a binary search
 * on the heap. Without this copy, every such search creates a new {@link DocValuesSkipper} and reads the skip index of the segment
 * from its start, which was a large part of the cost of synthetic {@code _id} lookups while indexing.
 * <p>
 * <b>Why not reuse a skipper of the segment?</b>
 * <ul>
 *     <li>A {@link DocValuesSkipper} is a forward-only cursor. A skipper that we keep between lookups helps only when the next
 *     ordinal is higher than its current position. The ordinals of the documents that we index are random, so about half of the
 *     lookups would need a new skipper, and a new skipper reads the skip index from the start of the segment.</li>
 *     <li>All reads of a skipper go through the input of the segment, and so also through the store metrics wrapper, which adds a
 *     cost to each small read.</li>
 *     <li>A skipper is not thread-safe, so we would need one for each thread and each segment.</li>
 * </ul>
 * This class holds the same data as the skipper, but as arrays on the heap with random access and no I/O. It is shared by all the
 * threads that read the segment. It also gives the end of the interval, which bounds the bisection of the ordinals.
 * <p>
 * <b>Costs</b>
 * <ul>
 *     <li>The copy takes 12 bytes per interval, which is about 3 KB per 1M documents with the default interval size.</li>
 *     <li>The first lookup on a segment reads the whole level 0 skip index to build the copy. Each new segment, for example after a
 *     refresh, builds its own copy, so the first lookup on it is slower than the next ones.</li>
 *     <li>The copy is only correct if the ordinals never decrease from one interval to the next. {@link #build} checks this and
 *     returns {@code null} if it is not true, and the caller then uses the skipper directly.</li>
 * </ul>
 */
final class TsIdSkipIndex {

    /** Only sizes the first arrays; they grow if the real interval size is smaller. */
    private static final int INITIAL_INTERVAL_SIZE_GUESS = 4096;

    private final int[] minDocIDs;
    private final int[] maxDocIDs;
    private final int[] maxOrds;
    private final long minOrd;
    private final long maxOrd;

    private TsIdSkipIndex(int[] minDocIDs, int[] maxDocIDs, int[] maxOrds, long minOrd, long maxOrd) {
        this.minDocIDs = minDocIDs;
        this.maxDocIDs = maxDocIDs;
        this.maxOrds = maxOrds;
        this.minOrd = minOrd;
        this.maxOrd = maxOrd;
    }

    /**
     * Returns the position of the first interval whose maximum ordinal is at least {@code tsIdOrd}, or {@code -1} if the ordinal is
     * outside the range of ordinals of the segment. This is the interval that {@link DocValuesSkipper#advance(long, long)} with
     * {@code (tsIdOrd, Long.MAX_VALUE)} stops on.
     */
    int intervalFor(long tsIdOrd) {
        if (tsIdOrd < minOrd || tsIdOrd > maxOrd) {
            return -1;
        }
        int lo = 0;
        int hi = maxOrds.length - 1;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (maxOrds[mid] < tsIdOrd) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /** The first document of the interval. */
    int minDocID(int interval) {
        return minDocIDs[interval];
    }

    /** The last document of the interval. */
    int maxDocID(int interval) {
        return maxDocIDs[interval];
    }

    /**
     * Reads the level 0 intervals of {@code skipper}. Returns {@code null} when the intervals do not have the expected layout, for
     * example if their maximum ordinals decrease. Callers then must not use an index for this segment.
     */
    @Nullable
    static TsIdSkipIndex build(DocValuesSkipper skipper, int maxDocs) throws IOException {
        if (skipper.maxValue() > Integer.MAX_VALUE || skipper.minValue() < 0) {
            return null;
        }
        int size = 0;
        int[] minDocIDs = new int[Math.max(16, maxDocs / INITIAL_INTERVAL_SIZE_GUESS + 1)];
        int[] maxDocIDs = new int[minDocIDs.length];
        int[] maxOrds = new int[minDocIDs.length];
        skipper.advance(0);
        while (skipper.minDocID(0) != DocIdSetIterator.NO_MORE_DOCS) {
            final long intervalMaxOrd = skipper.maxValue(0);
            if (intervalMaxOrd > Integer.MAX_VALUE || (size > 0 && intervalMaxOrd < maxOrds[size - 1])) {
                return null;
            }
            if (size == minDocIDs.length) {
                minDocIDs = ArrayUtil.grow(minDocIDs, size + 1);
                maxDocIDs = ArrayUtil.grow(maxDocIDs, size + 1);
                maxOrds = ArrayUtil.grow(maxOrds, size + 1);
            }
            minDocIDs[size] = skipper.minDocID(0);
            maxDocIDs[size] = skipper.maxDocID(0);
            maxOrds[size] = (int) intervalMaxOrd;
            size++;
            if (skipper.maxDocID(0) == Integer.MAX_VALUE) {
                break;
            }
            skipper.advance(skipper.maxDocID(0) + 1);
        }
        if (size == 0) {
            return null;
        }
        return new TsIdSkipIndex(
            Arrays.copyOf(minDocIDs, size),
            Arrays.copyOf(maxDocIDs, size),
            Arrays.copyOf(maxOrds, size),
            skipper.minValue(),
            skipper.maxValue()
        );
    }

    /**
     * Builds a {@link TsIdSkipIndex} the first time it is needed and shares it between all the readers of a segment. The index is
     * immutable, and building it twice at the same time is harmless, so no lock is used.
     */
    static final class Lazy {
        private static final TsIdSkipIndex UNSUPPORTED = new TsIdSkipIndex(new int[0], new int[0], new int[0], 0, -1);

        private final DocValuesProducer docValuesProducer;
        private final FieldInfos fieldInfos;
        private final int maxDocs;
        private volatile TsIdSkipIndex index;

        Lazy(DocValuesProducer docValuesProducer, FieldInfos fieldInfos, int maxDocs) {
            this.docValuesProducer = docValuesProducer;
            this.fieldInfos = fieldInfos;
            this.maxDocs = maxDocs;
        }

        /** Returns the index, or {@code null} if the segment has no usable {@code _tsid} skipper. */
        @Nullable
        TsIdSkipIndex get() throws IOException {
            TsIdSkipIndex result = index;
            if (result == null) {
                result = buildIndex();
                index = result;
            }
            return result == UNSUPPORTED ? null : result;
        }

        private TsIdSkipIndex buildIndex() throws IOException {
            final FieldInfo tsIdFieldInfo = fieldInfos.fieldInfo(TSDBSyntheticIdPostingsFormat.TS_ID);
            if (tsIdFieldInfo == null) {
                return UNSUPPORTED;
            }
            final DocValuesSkipper skipper = docValuesProducer.getSkipper(tsIdFieldInfo);
            if (skipper == null) {
                return UNSUPPORTED;
            }
            final TsIdSkipIndex built = build(skipper, maxDocs);
            return built == null ? UNSUPPORTED : built;
        }
    }
}
