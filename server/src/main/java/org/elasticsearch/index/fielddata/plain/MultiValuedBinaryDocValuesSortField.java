/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterBinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.search.BinarySortField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;

import java.io.IOException;

/**
 * A {@link BinarySortField} for keyword fields stored in the
 * {@link MultiValuedBinaryDocValuesField.SeparateCount} binary format.
 *
 * <p>For single-valued documents the binary payload is the raw term bytes and no decoding is
 * needed. For multi-valued documents the payload is a VInt-length-prefixed concatenation of the
 * sorted values; this class extracts either the first (minimum) or last (maximum) value as the
 * sort key, consistent with how {@code SortedSetSortField} behaves with a {@code MIN} or
 * {@code MAX} selector.
 */
public final class MultiValuedBinaryDocValuesSortField extends BinarySortField {

    public static final String PROVIDER_NAME = "MultiValuedBinaryDocValuesSortField";

    private final boolean maxMode;

    /** Returns {@code true} when this field uses the maximum (last) value for multi-valued documents. */
    boolean isMaxMode() {
        return maxMode;
    }

    public MultiValuedBinaryDocValuesSortField(String field, boolean reverse, Object missingValue, boolean maxMode) {
        super(field, reverse, missingValue, PROVIDER_NAME);
        this.maxMode = maxMode;
    }

    @Override
    protected BinaryDocValues getSortKeyDocValues(LeafReader reader) throws IOException {
        BinaryDocValues values = DocValues.getBinary(reader, getField());
        NumericDocValues counts = reader.getNumericDocValues(getField() + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX);
        if (counts == null) {
            // PlainBinary (single-valued field): raw bytes are the sort key.
            return values;
        }
        return new MinMaxBinaryDocValues(values, counts, maxMode);
    }

    /**
     * Wraps binary doc values in the {@code SeparateCounts} format, returning either the first
     * (minimum) or last (maximum) value — since values are stored sorted — as the sort key.
     */
    private static final class MinMaxBinaryDocValues extends FilterBinaryDocValues {
        private final NumericDocValues counts;
        private final boolean maxMode;
        private final ByteArrayStreamInput stream = new ByteArrayStreamInput();
        private final BytesRef selectedValue = new BytesRef();

        MinMaxBinaryDocValues(BinaryDocValues values, NumericDocValues counts, boolean maxMode) {
            super(values);
            this.counts = counts;
            this.maxMode = maxMode;
        }

        @Override
        public int nextDoc() throws IOException {
            int doc = in.nextDoc();
            if (doc != NO_MORE_DOCS) {
                counts.advanceExact(doc);
            }
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc = in.advance(target);
            if (doc != NO_MORE_DOCS) {
                counts.advanceExact(doc);
            }
            return doc;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            boolean found = in.advanceExact(target);
            if (found) {
                counts.advanceExact(target);
            }
            return found;
        }

        @Override
        public BytesRef binaryValue() throws IOException {
            BytesRef raw = in.binaryValue();
            if (counts.longValue() <= 1) {
                // count=1: raw bytes are the sort key, no decoding needed.
                return raw;
            }
            // count>1: VInt(len_1)+bytes_1+VInt(len_2)+bytes_2+... (values stored sorted)
            stream.reset(raw.bytes, raw.offset, raw.length);
            if (maxMode == false) {
                // First value = minimum.
                selectedValue.length = stream.readVInt();
                selectedValue.bytes = raw.bytes;
                selectedValue.offset = stream.getPosition();
            } else {
                // Last value = maximum: iterate through all entries.
                int endPos = raw.offset + raw.length;
                selectedValue.bytes = raw.bytes;
                while (stream.getPosition() < endPos) {
                    selectedValue.length = stream.readVInt();
                    selectedValue.offset = stream.getPosition();
                    stream.setPosition(selectedValue.offset + selectedValue.length);
                }
            }
            return selectedValue;
        }
    }

    /** SPI provider so this sort field can be serialized to and deserialized from segment info. */
    public static final class Provider extends SortFieldProvider {

        /** The name under which this provider is registered. */
        public static final String NAME = PROVIDER_NAME;

        /** Public no-arg constructor required by the SPI mechanism. */
        public Provider() {
            super(NAME);
        }

        @Override
        public SortField readSortField(DataInput in) throws IOException {
            String field = in.readString();
            boolean reverse = in.readInt() == 1;
            Object missingValue = switch (in.readInt()) {
                case 1 -> SortField.STRING_FIRST;
                case 2 -> SortField.STRING_LAST;
                default -> null;
            };
            boolean maxMode = in.readInt() == 1;
            return new MultiValuedBinaryDocValuesSortField(field, reverse, missingValue, maxMode);
        }

        @Override
        public void writeSortField(SortField sf, DataOutput out) throws IOException {
            assert sf instanceof MultiValuedBinaryDocValuesSortField;
            MultiValuedBinaryDocValuesSortField msf = (MultiValuedBinaryDocValuesSortField) sf;
            out.writeString(sf.getField());
            out.writeInt(sf.getReverse() ? 1 : 0);
            Object mv = sf.getMissingValue();
            if (mv == SortField.STRING_FIRST) {
                out.writeInt(1);
            } else if (mv == SortField.STRING_LAST) {
                out.writeInt(2);
            } else {
                out.writeInt(0);
            }
            out.writeInt(msf.maxMode ? 1 : 0);
        }
    }
}
