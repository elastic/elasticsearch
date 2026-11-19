/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.store;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;

/**
 * The ranks of a batch of documents in a column: the position of each among the documents that have a value. A dense
 * column's documents are their own ranks; a sparse column finds them through its presence, an {@link IndexedDISI}.
 */
public final class DocRanks {

    private final IndexInput data;
    private final long offset;
    private final long length;
    private final int jumpTableEntryCount;
    private final byte denseRankPower;
    private final long cost;

    private IndexedDISI disi;
    private long[] ranks = new long[0];
    private int count;

    private DocRanks(IndexInput data, long offset, long length, int jumpTableEntryCount, byte denseRankPower, long cost) {
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.jumpTableEntryCount = jumpTableEntryCount;
        this.denseRankPower = denseRankPower;
        this.cost = cost;
    }

    /** The ranks of a column where every document has a value. */
    public static DocRanks dense() {
        return new DocRanks(null, -1, 0, 0, (byte) -1, 0);
    }

    /** The ranks of a column whose presence is the {@link IndexedDISI} at {@code [offset, offset + length)} of {@code data}. */
    public static DocRanks sparse(IndexInput data, long offset, long length, int jumpTableEntryCount, byte denseRankPower, long cost) {
        return new DocRanks(data, offset, length, jumpTableEntryCount, denseRankPower, cost);
    }

    /**
     * Prefetches the presence of a sparse column, whole.
     *
     * @return whether it prefetched anything
     */
    public boolean prefetchPresence() throws IOException {
        if (data == null) {
            return false;
        }
        data.prefetch(offset, length);
        return true;
    }

    /** Finds the ranks of the documents of {@code docs[from, to)} that have a value, reading the presence of a sparse column. */
    public void resolve(int[] docs, int from, int to) throws IOException {
        ranks = ArrayUtil.growNoCopy(ranks, to - from);
        count = 0;
        if (data == null) {
            for (int i = from; i < to; i++) {
                ranks[count++] = docs[i];
            }
            return;
        }
        if (disi == null || disi.docID() > docs[from]) {
            disi = new IndexedDISI(data, offset, length, jumpTableEntryCount, denseRankPower, cost);
        }
        for (int i = from; i < to; i++) {
            if (disi.advanceExact(docs[i])) {
                ranks[count++] = disi.index();
            }
        }
    }

    /** The ranks {@link #resolve} found, in ascending order, in {@code [0, count())}. */
    public long[] ranks() {
        return ranks;
    }

    /** How many documents of the batch have a value. */
    public int count() {
        return count;
    }
}
