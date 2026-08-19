/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

/**
 * Iterates the documents that have a value for a field, and for the current document exposes its
 * value ordinal via {@link #index()} — the 0-based position among all documents that have a value.
 * The value layer uses that ordinal to locate the document's bytes in the substrate.
 *
 * <p>This is a {@link DocIdSetIterator}, so range and dense-loading code paths (including
 * {@link #intoBitSet}) consume it directly. Concrete shapes are chosen by the reader from
 * {@link ColumnIteratorMetadata}; callers only see this type.
 */
public abstract class ColumnIterator extends DocIdSetIterator {

    /** The current document's value ordinal. */
    public abstract int index();

    /**
     * Whether every document has a value, so a document id equals its own value ordinal. The
     * vectorized range and bulk-read fast paths gate on this: only a dense column maps a value block
     * directly onto a contiguous doc-id window.
     */
    public boolean isDense() {
        return false;
    }

    /**
     * Positions exactly at {@code target} and returns whether {@code target} has a value. Targets
     * must not decrease across calls, matching Lucene's per-document doc-values access.
     */
    public abstract boolean advanceExact(int target) throws IOException;

    static ColumnIterator emptyColumn() {
        return new Empty();
    }

    static ColumnIterator dense(int maxDoc) {
        return new Dense(maxDoc);
    }

    static ColumnIterator sparse(IndexedDISI disi) {
        return new Sparse(disi);
    }

    /** No document has a value. Starts unpositioned at {@code -1}, like any doc-values iterator. */
    private static final class Empty extends ColumnIterator {
        private int doc = -1;

        @Override
        public int index() {
            return -1;
        }

        @Override
        public boolean advanceExact(int target) {
            return false;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return doc = NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            return doc = NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return 0;
        }
    }

    /**
     * Every document has a value, so the document id is its own value ordinal and no per-document
     * data is stored. {@link #intoBitSet} fills the requested range in one shot.
     */
    private static final class Dense extends ColumnIterator {
        private final int maxDoc;
        private int doc = -1;

        Dense(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public boolean isDense() {
            return true;
        }

        @Override
        public int index() {
            return doc;
        }

        @Override
        public boolean advanceExact(int target) {
            doc = target;
            return target < maxDoc;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            return doc = target >= maxDoc ? NO_MORE_DOCS : target;
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) {
            if (doc >= upTo) {
                return;
            }
            int to = Math.min(upTo, maxDoc);
            if (doc < to) {
                bitSet.set(doc - offset, to - offset);
            }
            doc = upTo >= maxDoc ? NO_MORE_DOCS : upTo;
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }

    /** A sparse subset, addressed by a Lucene {@link IndexedDISI} that this instance owns. */
    private static final class Sparse extends ColumnIterator {
        private final IndexedDISI disi;

        Sparse(IndexedDISI disi) {
            this.disi = disi;
        }

        @Override
        public int index() {
            return disi.index();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return disi.advanceExact(target);
        }

        @Override
        public int docID() {
            return disi.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return disi.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return disi.advance(target);
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            disi.intoBitSet(upTo, bitSet, offset);
        }

        @Override
        public long cost() {
            return disi.cost();
        }
    }
}
