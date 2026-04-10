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
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;

import java.io.IOException;

/**
 * Wrapper around {@link BinaryDocValues} to decode the typical multivalued encoding
 */
public abstract class MultiValuedSortedBinaryDocValues extends SortedBinaryDocValues {

    final ByteArrayStreamInput in = new ByteArrayStreamInput();
    final BytesRef scratch = new BytesRef();
    final BinaryDocValues values;
    int count;

    private MultiValuedSortedBinaryDocValues(BinaryDocValues values) {
        this.values = values;
    }

    public static MultiValuedSortedBinaryDocValues from(LeafReader leafReader, String valuesFieldName) throws IOException {
        BinaryDocValues values = DocValues.getBinary(leafReader, valuesFieldName);

        // Obtain counts directly from leafReader so that null is returned rather than an empty doc values.
        // Whether counts is null allows us to determine which multivalued format was used.
        return from(leafReader, valuesFieldName, values);
    }

    public static MultiValuedSortedBinaryDocValues from(LeafReader leafReader, String valuesFieldName, BinaryDocValues values)
        throws IOException {
        String countsFieldName = valuesFieldName + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;
        NumericDocValues counts = leafReader.getNumericDocValues(countsFieldName);
        if (counts == null) {
            return new IntegratedCounts(values);
        } else {
            Sparsity sparsity = Sparsity.UNKNOWN;
            ValueMode valueMode = ValueMode.UNKNOWN;

            DocValuesSkipper countsSkipper = leafReader.getDocValuesSkipper(countsFieldName);
            if (countsSkipper != null) {
                sparsity = countsSkipper.docCount() == leafReader.maxDoc() ? Sparsity.DENSE : Sparsity.SPARSE;
                valueMode = countsSkipper.maxValue() == 1 ? ValueMode.SINGLE_VALUED : ValueMode.MULTI_VALUED;
            }

            return new SeparateCounts(values, counts, sparsity, valueMode);
        }
    }

    @Override
    public int docValueCount() {
        return count;
    }

    @Override
    public abstract boolean advanceExact(int doc) throws IOException;

    @Override
    public abstract BytesRef nextValue() throws IOException;

    /**
     * Multivalued binary doc values encoded by {@link org.elasticsearch.index.mapper.BinaryFieldMapper.CustomBinaryDocValuesField}.
     * These have the form: [doc value count][length of value 1][value 1][length of value 2][value 2]...
     */
    private static class IntegratedCounts extends MultiValuedSortedBinaryDocValues {
        IntegratedCounts(BinaryDocValues values) {
            super(values);
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            if (values.advanceExact(doc)) {
                final BytesRef bytes = values.binaryValue();
                assert bytes.length > 0;
                in.reset(bytes.bytes, bytes.offset, bytes.length);
                count = in.readVInt();
                scratch.bytes = bytes.bytes;
                return true;
            } else {
                count = 0;
                return false;
            }
        }

        @Override
        public BytesRef nextValue() throws IOException {
            scratch.length = in.readVInt();
            scratch.offset = in.getPosition();
            in.setPosition(scratch.offset + scratch.length);
            return scratch;
        }
    }

    /**
     * Multivalued binary doc values with counts stored in a separate numeric doc values.
     * If a binary value contains a single value, payload is of the form: [value 1]
     * If a binary value contains multiple values, payload is of the form: [length of value 1][value 1][length of value 2][value 2]...
     */
    private static class SeparateCounts extends MultiValuedSortedBinaryDocValues {
        private final NumericDocValues counts;
        private final Sparsity sparsity;
        private final ValueMode valueMode;

        SeparateCounts(BinaryDocValues values, NumericDocValues counts, Sparsity sparsity, ValueMode valueMode) {
            super(values);
            this.counts = counts;
            this.sparsity = sparsity;
            this.valueMode = valueMode;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            if (values.advanceExact(doc)) {
                boolean advanced = counts.advanceExact(doc);
                assert advanced;
                count = Math.toIntExact(counts.longValue());
                scratch.bytes = null;
                return true;
            } else {
                count = 0;
                return false;
            }
        }

        @Override
        public BytesRef nextValue() throws IOException {
            if (scratch.bytes == null) {
                final BytesRef bytes = values.binaryValue();
                scratch.bytes = bytes.bytes;
                if (count == 1) {
                    scratch.offset = bytes.offset;
                    scratch.length = bytes.length;
                    return scratch;
                } else {
                    in.reset(bytes.bytes, bytes.offset, bytes.length);
                }
            }

            // multiple values
            scratch.length = in.readVInt();
            scratch.offset = in.getPosition();
            in.setPosition(scratch.offset + scratch.length);
            return scratch;
        }

        @Override
        public Sparsity getSparsity() {
            return sparsity;
        }

        @Override
        public ValueMode getValueMode() {
            return valueMode;
        }
    }
}
