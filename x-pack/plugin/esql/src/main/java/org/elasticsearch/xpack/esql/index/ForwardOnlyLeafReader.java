/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterBinaryDocValues;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.FilterSortedDocValues;
import org.apache.lucene.index.FilterSortedNumericDocValues;
import org.apache.lucene.index.FilterSortedSetDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * The reader of a segment that Lucene is flushing, whose doc values can only move to the next document and which doesn't
 * know its number of documents. Moves the doc values forward document by document when asked to advance, and reports the
 * number of documents that Lucene gives to the index sort. Like any doc values, they then can't move backward.
 */
final class ForwardOnlyLeafReader extends FilterLeafReader {
    private final int maxDoc;

    ForwardOnlyLeafReader(LeafReader in, int maxDoc) {
        super(in);
        this.maxDoc = maxDoc;
    }

    @Override
    public int maxDoc() {
        return maxDoc;
    }

    @Override
    public int numDocs() {
        return maxDoc;
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        NumericDocValues values = in.getNumericDocValues(field);
        return values == null ? null : new ForwardNumeric(values);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        BinaryDocValues values = in.getBinaryDocValues(field);
        return values == null ? null : new ForwardBinary(values);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        SortedDocValues values = in.getSortedDocValues(field);
        return values == null ? null : new ForwardSorted(values);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        SortedNumericDocValues values = in.getSortedNumericDocValues(field);
        if (values == null) {
            return null;
        }
        NumericDocValues single = DocValues.unwrapSingleton(values);
        return single != null ? DocValues.singleton(new ForwardNumeric(single)) : new ForwardSortedNumeric(values);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        SortedSetDocValues values = in.getSortedSetDocValues(field);
        if (values == null) {
            return null;
        }
        SortedDocValues single = DocValues.unwrapSingleton(values);
        return single != null ? DocValues.singleton(new ForwardSorted(single)) : new ForwardSortedSet(values);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return null;
    }

    /**
     * Moves doc values to a target by moving to the next document. Keeps the target as the current document after
     * {@link #advanceExact}, even when the doc values moved past it.
     */
    private static final class Cursor {
        private final DocIdSetIterator in;
        private int doc = -1;

        Cursor(DocIdSetIterator in) {
            this.in = in;
        }

        boolean advanceExact(int target) throws IOException {
            boolean exists = moveTo(target) == target;
            doc = target;
            return exists;
        }

        int nextDoc() throws IOException {
            return doc = moveTo(doc + 1);
        }

        int advance(int target) throws IOException {
            return doc = moveTo(target);
        }

        private int moveTo(int target) throws IOException {
            int current = in.docID();
            while (current < target) {
                current = in.nextDoc();
            }
            return current;
        }
    }

    private static final class ForwardNumeric extends FilterNumericDocValues {
        private final Cursor cursor;

        ForwardNumeric(NumericDocValues in) {
            super(in);
            this.cursor = new Cursor(in);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return cursor.advanceExact(target);
        }

        @Override
        public int docID() {
            return cursor.doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return cursor.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return cursor.advance(target);
        }
    }

    private static final class ForwardBinary extends FilterBinaryDocValues {
        private final Cursor cursor;

        ForwardBinary(BinaryDocValues in) {
            super(in);
            this.cursor = new Cursor(in);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return cursor.advanceExact(target);
        }

        @Override
        public int docID() {
            return cursor.doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return cursor.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return cursor.advance(target);
        }
    }

    private static final class ForwardSorted extends FilterSortedDocValues {
        private final Cursor cursor;

        ForwardSorted(SortedDocValues in) {
            super(in);
            this.cursor = new Cursor(in);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return cursor.advanceExact(target);
        }

        @Override
        public int docID() {
            return cursor.doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return cursor.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return cursor.advance(target);
        }
    }

    private static final class ForwardSortedNumeric extends FilterSortedNumericDocValues {
        private final Cursor cursor;

        ForwardSortedNumeric(SortedNumericDocValues in) {
            super(in);
            this.cursor = new Cursor(in);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return cursor.advanceExact(target);
        }

        @Override
        public int docID() {
            return cursor.doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return cursor.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return cursor.advance(target);
        }
    }

    private static final class ForwardSortedSet extends FilterSortedSetDocValues {
        private final Cursor cursor;

        ForwardSortedSet(SortedSetDocValues in) {
            super(in);
            this.cursor = new Cursor(in);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return cursor.advanceExact(target);
        }

        @Override
        public int docID() {
            return cursor.doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return cursor.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return cursor.advance(target);
        }
    }
}
