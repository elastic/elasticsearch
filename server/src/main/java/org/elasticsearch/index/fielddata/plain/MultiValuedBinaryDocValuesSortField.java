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
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FilterBinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.search.BinarySortField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;

import java.io.IOException;

/**
 * A {@link BinarySortField} for keyword/IP fields stored as high-cardinality binary doc values, in either the
 * {@link MultiValuedBinaryDocValuesField.SeparateCount} format (values deduplicated and stored sorted) or the
 * {@link MultiValuedBinaryDocValuesField.ArrayOrderInlineNull} format (values stored in document order, with inline
 * nulls) — see {@link #isArrayOrder()}.
 *
 * <p>For single-valued documents the binary payload is the raw term bytes and no decoding is needed in either
 * format. For multi-valued documents this class extracts either the minimum or maximum value as the sort key,
 * consistent with how {@code SortedSetSortField} behaves with a {@code MIN} or {@code MAX} selector.
 */
public final class MultiValuedBinaryDocValuesSortField extends BinarySortField {

    public static final String PROVIDER_NAME = "MultiValuedBinaryDocValuesSortField";

    private final boolean maxMode;
    private final boolean arrayOrder;

    /** Returns {@code true} when this field uses the maximum (last) value for multi-valued documents. */
    boolean isMaxMode() {
        return maxMode;
    }

    /**
     * Returns {@code true} when this field's binary doc values use the {@code ArrayOrderInlineNull} encoding
     * (document order, inline nulls) rather than {@code SeparateCount} (deduplicated, sorted).
     */
    public boolean isArrayOrder() {
        return arrayOrder;
    }

    public MultiValuedBinaryDocValuesSortField(String field, boolean reverse, Object missingValue, boolean maxMode) {
        this(field, reverse, missingValue, maxMode, false);
    }

    public MultiValuedBinaryDocValuesSortField(String field, boolean reverse, Object missingValue, boolean maxMode, boolean arrayOrder) {
        super(field, reverse, missingValue, PROVIDER_NAME);
        this.maxMode = maxMode;
        this.arrayOrder = arrayOrder;
    }

    @Override
    protected BinaryDocValues getSortKeyDocValues(LeafReader reader) throws IOException {
        BinaryDocValues values = DocValues.getBinary(reader, getField());
        String countsFieldName = getField() + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;
        NumericDocValues counts = reader.getNumericDocValues(countsFieldName);
        if (counts == null) {
            // PlainBinary (single-valued field): raw bytes are the sort key.
            return values;
        }
        // Whole segment single-valued (no document holds more than one value): the raw payload is already the
        // sort key in both encodings, so the MinMaxBinaryDocValues wrapper - and its per-doc counts advance and
        // decode branch - can be skipped entirely. The skipper is null while a segment is still buffered at flush
        // time, where we correctly fall back to the wrapper (which reads counts with nextDoc()). Lucene's
        // IndexingChain also calls getSortKeyDocValues() with a synthetic DocValuesLeafReader purely to validate
        // the index sort field's doc values type at index time; that reader throws UnsupportedOperationException
        // from getDocValuesSkipper(), so treat that the same as "no skipper available".
        DocValuesSkipper countsSkipper;
        try {
            countsSkipper = reader.getDocValuesSkipper(countsFieldName);
        } catch (UnsupportedOperationException e) {
            countsSkipper = null;
        }
        if (countsSkipper != null && countsSkipper.maxValue() <= 1) {
            return values;
        }
        return new MinMaxBinaryDocValues(values, counts, maxMode, arrayOrder);
    }

    /**
     * Decodes the minimum ({@code maxMode=false}) or maximum ({@code maxMode=true}) sort key from a document's raw
     * binary doc values payload with the given value {@code count}, dispatching to whichever decoder matches this
     * field's encoding ({@code arrayOrder}). Shared by {@link MinMaxBinaryDocValues#binaryValue()} and
     * {@code LongValuesComparatorSource}'s host.name singleton check.
     */
    public static BytesRef decodeExtreme(BytesRef raw, long count, boolean maxMode, boolean arrayOrder) {
        if (count <= 1) {
            // count=1 (or a lone slot): raw bytes are the sort key in either encoding, no decoding needed.
            return raw;
        }
        if (arrayOrder) {
            return MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.decodeExtreme(raw, (int) count, maxMode);
        }
        return MultiValuedBinaryDocValuesField.SeparateCount.decodeExtreme(raw, maxMode);
    }

    /**
     * Wraps binary doc values, returning either the minimum or maximum value as the sort key — decoded according to
     * whichever encoding ({@code SeparateCount} or {@code ArrayOrderInlineNull}) this field actually uses.
     */
    private static final class MinMaxBinaryDocValues extends FilterBinaryDocValues {
        private final NumericDocValues counts;
        private final boolean maxMode;
        private final boolean arrayOrder;

        MinMaxBinaryDocValues(BinaryDocValues values, NumericDocValues counts, boolean maxMode, boolean arrayOrder) {
            super(values);
            this.counts = counts;
            this.maxMode = maxMode;
            this.arrayOrder = arrayOrder;
        }

        @Override
        public int nextDoc() throws IOException {
            int doc = in.nextDoc();
            if (doc != NO_MORE_DOCS) {
                // Use nextDoc (sequential) rather than advanceExact: during segment flush
                // Lucene iterates docs with nextDoc() only and the buffered NumericDocValues
                // writer does not support advanceExact(). The binary and count fields are
                // always indexed together so they have the same doc IDs and stay in sync.
                counts.nextDoc();
            }
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc = in.advance(target);
            if (doc != NO_MORE_DOCS) {
                counts.advance(doc);
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
            return decodeExtreme(in.binaryValue(), counts.longValue(), maxMode, arrayOrder);
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
            boolean arrayOrder = in.readInt() == 1;
            return new MultiValuedBinaryDocValuesSortField(field, reverse, missingValue, maxMode, arrayOrder);
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
            out.writeInt(msf.arrayOrder ? 1 : 0);
        }
    }
}
