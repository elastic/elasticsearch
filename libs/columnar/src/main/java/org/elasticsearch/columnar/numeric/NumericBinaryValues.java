/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * Each document's values of a binary doc values field that holds a {@link NumericBinaryPayload} per document, as longs in
 * the order they were written. The doc values come either from the ColumNAR codec, which reads the values straight out of
 * the column, or from somewhere else, such as the indexing buffer a segment is sorted from at flush, whose payloads are
 * decoded. Readers of such a field go through this class so that neither source needs a branch of its own in each of them.
 *
 * <p>Only documents that have a value are visited: a document without one writes no payload.
 */
public abstract class NumericBinaryValues extends DocIdSetIterator {

    /** The values of {@code binary}, which must hold a {@link NumericBinaryPayload} per document. */
    public static NumericBinaryValues of(BinaryDocValues binary) {
        return binary instanceof ColumnarNumericBinaryDocValues columnar ? new Column(columnar) : new Payloads(binary);
    }

    private final BinaryDocValues in;

    private NumericBinaryValues(BinaryDocValues in) {
        this.in = in;
    }

    /** Positions on {@code target} and returns whether it has a value. */
    public final boolean advanceExact(int target) throws IOException {
        return in.advanceExact(target);
    }

    /** The number of values the current document holds; at least one. */
    public abstract int docValueCount() throws IOException;

    /** The current document's {@code index}-th value, in the order the values were written. */
    public abstract long valueAt(int index) throws IOException;

    /**
     * Whether every document holds exactly one value. Payloads that are not a column say nothing of the segment as a whole,
     * so they answer {@code false}.
     */
    public abstract boolean isSingleValued();

    @Override
    public final int docID() {
        return in.docID();
    }

    @Override
    public final int nextDoc() throws IOException {
        return in.nextDoc();
    }

    @Override
    public final int advance(int target) throws IOException {
        return in.advance(target);
    }

    @Override
    public final long cost() {
        return in.cost();
    }

    /** Reads the values straight out of the column, without building the payload. */
    private static final class Column extends NumericBinaryValues {
        private final ColumnarNumericBinaryDocValues column;

        Column(ColumnarNumericBinaryDocValues column) {
            super(column);
            this.column = column;
        }

        @Override
        public int docValueCount() {
            return column.docValueCount();
        }

        @Override
        public long valueAt(int index) throws IOException {
            return column.valueAt(index);
        }

        @Override
        public boolean isSingleValued() {
            return column.isSingleValued();
        }
    }

    /** Decodes each document's payload the first time one of its values is asked for. */
    private static final class Payloads extends NumericBinaryValues {
        private final BinaryDocValues binary;
        private final long[][] decoded = { new long[8] };
        private int decodedDoc = -1;
        private int count;

        Payloads(BinaryDocValues binary) {
            super(binary);
            this.binary = binary;
        }

        @Override
        public int docValueCount() throws IOException {
            decode();
            return count;
        }

        @Override
        public long valueAt(int index) throws IOException {
            decode();
            assert index >= 0 && index < count : "value " + index + " of a document holding " + count;
            return decoded[0][index];
        }

        @Override
        public boolean isSingleValued() {
            return false;
        }

        private void decode() throws IOException {
            final int doc = binary.docID();
            if (doc != decodedDoc) {
                count = NumericBinaryPayload.decode(binary.binaryValue(), decoded);
                assert count > 0 : "a document without a value writes no payload";
                decodedDoc = doc;
            }
        }
    }
}
