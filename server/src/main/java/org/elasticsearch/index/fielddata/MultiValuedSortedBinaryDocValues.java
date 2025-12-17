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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;

import java.io.IOException;

/**
 * Wrapper around {@link BinaryDocValues} to decode the typical multivalued encoding
 */
public abstract class MultiValuedSortedBinaryDocValues extends SortedBinaryDocValues {

    protected final ByteArrayStreamInput in = new ByteArrayStreamInput();
    protected final BytesRef scratch = new BytesRef();

    protected final BinaryDocValues values;
    protected int count;

    private MultiValuedSortedBinaryDocValues(BinaryDocValues values) {
        this.values = values;
    }

    public static MultiValuedSortedBinaryDocValues from(LeafReader leafReader, String valuesFieldName, String countsFieldName)
        throws IOException {
        BinaryDocValues values = DocValues.getBinary(leafReader, valuesFieldName);
        // Obtain counts directly from leafReader so that null is returned rather than an empty doc values.
        // Whether counts is null allows us to determine which multivalued format was used.
        NumericDocValues counts = leafReader.getNumericDocValues(countsFieldName);
        return from(values, counts);
    }

    public static MultiValuedSortedBinaryDocValues from(BinaryDocValues values, NumericDocValues counts) throws IOException {
        if (counts == null) {
            return new IntegratedCounts(values);
        } else {
            return new SeparateCounts(values, counts);
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

        SeparateCounts(BinaryDocValues values, NumericDocValues counts) {
            super(values);
            this.counts = counts;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            if (values.advanceExact(doc)) {
                assert counts.advanceExact(doc);
                count = Math.toIntExact(counts.longValue());

                final BytesRef bytes = values.binaryValue();

                if (count == 1) {
                    scratch.bytes = bytes.bytes;
                    scratch.offset = bytes.offset;
                    scratch.length = bytes.length;
                } else {
                    in.reset(bytes.bytes, bytes.offset, bytes.length);
                    scratch.bytes = bytes.bytes;
                }
                return true;
            } else {
                count = 0;
                return false;
            }
        }

        @Override
        public BytesRef nextValue() throws IOException {
            if (count != 1) {
                scratch.length = in.readVInt();
                scratch.offset = in.getPosition();
                in.setPosition(scratch.offset + scratch.length);
            }
            return scratch;
        }
    }
}
