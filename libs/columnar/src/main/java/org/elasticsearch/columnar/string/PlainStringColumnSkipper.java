/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.io.IOException;

/**
 * Reads the skip region written by {@link PlainSkipIndexCodec}, shaped as the numeric
 * {@code NumericColumnSkipper} is. Each {@link #advance} moves forward to the entry covering the target
 * document and exposes its per-level doc-id range and term bounds. A subtree that ends before the target is
 * skipped by seeking past the group length its level record carries, so the terms of intervals that are
 * passed over are never read. Nothing is held on the heap beyond the fixed per-level scratch.
 */
final class PlainStringColumnSkipper {

    private final StringColumnMetadata.Plain.Skipper meta;
    private final int maxLevel;
    private final IndexInput input;

    private final int[] minDocID;
    private final int[] maxDocID;
    private final int[] docCount;
    private final BytesRefBuilder[] minTerm;
    private final BytesRefBuilder[] maxTerm;
    private int levels = 1;

    PlainStringColumnSkipper(StringColumnMetadata.Plain.Skipper meta, IndexInput skipIndex) throws IOException {
        this.meta = meta;
        this.maxLevel = meta.maxLevel();
        this.input = skipIndex.slice("columnar string skipper", meta.dataOffset(), meta.dataLength());
        if (maxLevel < 1 || maxLevel > Byte.MAX_VALUE) {
            throw new CorruptIndexException("skip region records [" + maxLevel + "] levels", input.toString());
        }
        if (meta.maxBoundBytes() < 1) {
            throw new CorruptIndexException("skip region keeps bounds of [" + meta.maxBoundBytes() + "] bytes", input.toString());
        }
        this.minDocID = new int[maxLevel];
        this.maxDocID = new int[maxLevel];
        this.docCount = new int[maxLevel];
        this.minTerm = new BytesRefBuilder[maxLevel];
        this.maxTerm = new BytesRefBuilder[maxLevel];
        for (int level = 0; level < maxLevel; level++) {
            minDocID[level] = maxDocID[level] = -1;
            minTerm[level] = new BytesRefBuilder();
            maxTerm[level] = new BytesRefBuilder();
        }
    }

    /** Moves to the entry covering {@code target}, which has to be beyond the current level-0 interval. */
    void advance(int target) throws IOException {
        if (target > meta.maxDocId()) {
            for (int level = 0; level < maxLevel; level++) {
                minDocID[level] = maxDocID[level] = DocIdSetIterator.NO_MORE_DOCS;
            }
            return;
        }
        assert target > maxDocID[0] : "target must be beyond the current interval";
        while (true) {
            final long entryStart = input.getFilePointer();
            levels = input.readByte();
            if (levels < 1 || levels > maxLevel) {
                throw new CorruptIndexException("skip entry at [" + entryStart + "] holds [" + levels + "] levels", input.toString());
            }
            boolean valid = true;
            for (int level = levels - 1; level >= 0; level--) {
                maxDocID[level] = input.readInt();
                final long subtreeByteLength = input.readInt();
                if (maxDocID[level] < target) {
                    final long nextEntryStart = entryStart + subtreeByteLength;
                    if (nextEntryStart < input.getFilePointer() || nextEntryStart > input.length()) {
                        throw new CorruptIndexException(
                            "skip entry at ["
                                + entryStart
                                + "] jumps ["
                                + subtreeByteLength
                                + "] bytes in a region of ["
                                + input.length()
                                + "]",
                            input.toString()
                        );
                    }
                    input.seek(nextEntryStart);
                    valid = false;
                    break;
                }
                minDocID[level] = input.readInt();
                docCount[level] = input.readInt();
                readTerm(minTerm[level]);
                readTerm(maxTerm[level]);
            }
            if (valid) {
                while (levels < maxLevel && maxDocID[levels] >= target) {
                    levels++;
                }
                return;
            }
        }
    }

    /** The levels whose bounds cover the current interval, level 0 first. */
    int numLevels() {
        return levels;
    }

    int minDocID(int level) {
        return minDocID[level];
    }

    int maxDocID(int level) {
        return maxDocID[level];
    }

    int docCount(int level) {
        return docCount[level];
    }

    BytesRef minTerm(int level) {
        return minTerm[level].get();
    }

    BytesRef maxTerm(int level) {
        return maxTerm[level].get();
    }

    private void readTerm(BytesRefBuilder term) throws IOException {
        final int length = input.readVInt();
        final long bytesLeft = input.length() - input.getFilePointer();
        if (length < 0 || length > bytesLeft) {
            throw new CorruptIndexException(
                "skip term of [" + length + "] bytes with [" + bytesLeft + "] left in the region",
                input.toString()
            );
        }
        term.grow(length);
        input.readBytes(term.bytes(), 0, length);
        term.setLength(length);
    }
}
