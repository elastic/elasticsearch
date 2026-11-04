/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;

import java.io.IOException;

/**
 * Reads the doc values of a numeric field as {@link SortedNumericDocValues}, whether they are stored as {@code SORTED_NUMERIC}
 * or as the binary {@link NumericBinaryPayload}s the ColumNAR codec stores numeric columns as.
 *
 * <p>A ColumNAR payload holds a document's values sorted, duplicates kept, exactly as {@code SORTED_NUMERIC} would (see
 * {@code ColumnarNumericDocValuesField}), so it is served unchanged through the same interface and every reader of numeric
 * doc values reads both layouts. This is interim: once the codec keeps a document's values in the order they were written,
 * readers will read the column natively rather than through this adapter.
 */
public final class ColumnarNumericDocValues {

    private ColumnarNumericDocValues() {}

    /**
     * Like {@link DocValues#getSortedNumeric(LeafReader, String)}, but also serves a numeric field whose doc values the
     * ColumNAR codec stores. Only call this for a numeric field: binary doc values are read as numeric payloads.
     */
    public static SortedNumericDocValues getSortedNumeric(LeafReader reader, String field) throws IOException {
        final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
            final BinaryDocValues binary = reader.getBinaryDocValues(field);
            return binary == null ? DocValues.emptySortedNumeric() : adapt(binary);
        }
        return DocValues.getSortedNumeric(reader, field);
    }

    /**
     * Like {@link LeafReader#getSortedNumericDocValues(String)}, returning {@code null} rather than empty doc values for a field
     * without any, but also serves a numeric field whose doc values the ColumNAR codec stores.
     */
    public static SortedNumericDocValues getSortedNumericOrNull(LeafReader reader, String field) throws IOException {
        final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
            final BinaryDocValues binary = reader.getBinaryDocValues(field);
            return binary == null ? null : adapt(binary);
        }
        return reader.getSortedNumericDocValues(field);
    }

    /**
     * A column whose documents all hold one value is served as a singleton, matching an optimization Lucene applies to a
     * {@code SORTED_NUMERIC} field whose segment has at most one value per document. It isn't needed for correctness, since every
     * reader also handles the multi-valued form, but readers branch on {@link DocValues#unwrapSingleton} for their single-valued
     * fast paths (for example the ES|QL block loaders and {@code SingleValueMatchQuery}), and tests assert on which path ran, such
     * as the {@code ...FromDocValues.Singleton} loader names that ES|QL profiles report. Serving the column the same way keeps
     * those paths and expectations the same whichever layout stores the field.
     */
    private static SortedNumericDocValues adapt(BinaryDocValues binary) {
        if (binary instanceof ColumnarNumericBinaryDocValues column && column.singleValued()) {
            return DocValues.singleton(new PayloadNumericDocValues(column));
        }
        return new PayloadSortedNumericDocValues(binary);
    }

    /** A single-valued column's payloads, each a single value. */
    private static final class PayloadNumericDocValues extends NumericDocValues {
        private final BinaryDocValues in;
        private final long[][] value = new long[][] { new long[1] };

        PayloadNumericDocValues(BinaryDocValues in) {
            this.in = in;
        }

        @Override
        public long longValue() throws IOException {
            final int count = NumericBinaryPayload.decode(in.binaryValue(), value);
            assert count == 1 : "a single-valued column holds one value a document, not " + count;
            return value[0][0];
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return in.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return in.advance(target);
        }

        @Override
        public long cost() {
            return in.cost();
        }
    }

    /**
     * A document's payload decoded into the values {@code SORTED_NUMERIC} would hold for it. Decoded when the iterator lands on
     * a document, which is when a reader of {@link SortedNumericDocValues} may first ask for {@link #docValueCount()}.
     */
    private static final class PayloadSortedNumericDocValues extends SortedNumericDocValues {
        private final BinaryDocValues in;
        private final long[][] values = new long[][] { new long[1] };
        private int count;
        private int upto;

        PayloadSortedNumericDocValues(BinaryDocValues in) {
            this.in = in;
        }

        private int decode(int doc) throws IOException {
            if (doc != NO_MORE_DOCS) {
                count = NumericBinaryPayload.decode(in.binaryValue(), values);
                assert count > 0 : "a document with doc values holds at least one value";
                upto = 0;
            }
            return doc;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (in.advanceExact(target)) {
                decode(target);
                return true;
            }
            return false;
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return decode(in.nextDoc());
        }

        @Override
        public int advance(int target) throws IOException {
            return decode(in.advance(target));
        }

        @Override
        public long cost() {
            return in.cost();
        }

        @Override
        public long nextValue() {
            return values[0][upto++];
        }

        @Override
        public int docValueCount() {
            return count;
        }
    }
}
