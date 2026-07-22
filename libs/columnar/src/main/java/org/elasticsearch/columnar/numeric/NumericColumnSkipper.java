/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Reads the skip index written by {@link MultiLevelSkipIndexCodec}. Each {@link #advance} moves to the
 * interval tree entry covering the target document and exposes its per-level value bounds and doc-id
 * ranges; a range query uses the coarsest level whose bounds miss the query to skip a whole subtree.
 * The skip region is read on demand from the mapped input — nothing is held on the heap beyond the
 * fixed per-level scratch arrays.
 */
public final class NumericColumnSkipper extends DocValuesSkipper {

    private static final long INTERVAL_BYTES = 29L;

    private final NumericColumnMetadata.Skipper meta;
    private final IndexInput input;
    private final long[] jumpLengths = jumpLengths();

    private final int[] minDocID = new int[MultiLevelSkipIndexCodec.MAX_LEVEL];
    private final int[] maxDocID = new int[MultiLevelSkipIndexCodec.MAX_LEVEL];
    private final long[] minValue = new long[MultiLevelSkipIndexCodec.MAX_LEVEL];
    private final long[] maxValue = new long[MultiLevelSkipIndexCodec.MAX_LEVEL];
    private final int[] docCount = new int[MultiLevelSkipIndexCodec.MAX_LEVEL];
    private int levels = 1;

    /**
     * A skipper over a column with no values. Starts unpositioned ({@code maxDocID(0) == -1}, as the
     * skipper contract requires) and runs off the end on the first {@link #advance}.
     */
    public static DocValuesSkipper empty() {
        return new DocValuesSkipper() {
            private int docId = -1;

            @Override
            public void advance(int target) {
                docId = DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int numLevels() {
                return 1;
            }

            @Override
            public int minDocID(int level) {
                return docId;
            }

            @Override
            public int maxDocID(int level) {
                return docId;
            }

            @Override
            public long minValue(int level) {
                return Long.MAX_VALUE;
            }

            @Override
            public long maxValue(int level) {
                return Long.MIN_VALUE;
            }

            @Override
            public int docCount(int level) {
                return 0;
            }

            @Override
            public long minValue() {
                return Long.MAX_VALUE;
            }

            @Override
            public long maxValue() {
                return Long.MIN_VALUE;
            }

            @Override
            public int docCount() {
                return 0;
            }

            @Override
            public int maxValueCount() {
                return 0;
            }
        };
    }

    public NumericColumnSkipper(NumericColumnMetadata.Skipper meta, IndexInput data) throws IOException {
        this.meta = meta;
        this.input = data.slice("columnar skipper", meta.dataOffset(), meta.dataLength());
        for (int i = 0; i < MultiLevelSkipIndexCodec.MAX_LEVEL; i++) {
            minDocID[i] = maxDocID[i] = -1;
        }
    }

    @Override
    public void advance(int target) throws IOException {
        if (target > meta.maxDocId()) {
            for (int i = 0; i < MultiLevelSkipIndexCodec.MAX_LEVEL; i++) {
                minDocID[i] = maxDocID[i] = DocIdSetIterator.NO_MORE_DOCS;
            }
            return;
        }
        assert target > maxDocID[0] : "target must be beyond the current interval";
        while (true) {
            levels = input.readByte();
            assert levels > 0 && levels <= MultiLevelSkipIndexCodec.MAX_LEVEL : "level out of range [" + levels + "]";
            boolean valid = true;
            for (int level = levels - 1; level >= 0; level--) {
                if ((maxDocID[level] = input.readInt()) < target) {
                    input.skipBytes(jumpLengths[level]);
                    valid = false;
                    break;
                }
                minDocID[level] = input.readInt();
                maxValue[level] = input.readLong();
                minValue[level] = input.readLong();
                docCount[level] = input.readInt();
            }
            if (valid) {
                while (levels < MultiLevelSkipIndexCodec.MAX_LEVEL && maxDocID[levels] >= target) {
                    levels++;
                }
                return;
            }
        }
    }

    @Override
    public int numLevels() {
        return levels;
    }

    @Override
    public int minDocID(int level) {
        return minDocID[level];
    }

    @Override
    public int maxDocID(int level) {
        return maxDocID[level];
    }

    @Override
    public long minValue(int level) {
        return minValue[level];
    }

    @Override
    public long maxValue(int level) {
        return maxValue[level];
    }

    @Override
    public int docCount(int level) {
        return docCount[level];
    }

    @Override
    public long minValue() {
        return meta.minValue();
    }

    @Override
    public long maxValue() {
        return meta.maxValue();
    }

    @Override
    public int docCount() {
        return meta.docCount();
    }

    @Override
    public int maxValueCount() {
        return meta.maxValueCount();
    }

    /** Bytes to skip past a below-target entry and its subtree, per level. Mirrors the 29-byte layout. */
    private static long[] jumpLengths() {
        final long[] lengths = new long[MultiLevelSkipIndexCodec.MAX_LEVEL];
        lengths[0] = INTERVAL_BYTES - 5L; // already read the level byte (1) and this level's maxDocID (4)
        for (int level = 1; level < MultiLevelSkipIndexCodec.MAX_LEVEL; level++) {
            lengths[level] = lengths[level - 1];
            lengths[level] += (1L << (level * MultiLevelSkipIndexCodec.LEVEL_SHIFT)) * INTERVAL_BYTES;
            lengths[level] -= (1L << ((level - 1) * MultiLevelSkipIndexCodec.LEVEL_SHIFT));
        }
        return lengths;
    }
}
