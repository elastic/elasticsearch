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
import org.apache.lucene.index.CorruptIndexException;
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
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;

import java.io.IOException;

/**
 * A {@link BinarySortField} for keyword/IP fields stored as high-cardinality binary doc values, in whichever
 * {@link BinaryDocValuesFormat} the mapping chose — see {@link #binaryFormat()}.
 *
 * <p>For single-valued documents the blob is the raw term bytes and no decoding is needed, except under
 * {@link BinaryDocValuesFormat#COLUMNAR_PAYLOAD}, which frames every document. For multi-valued documents this
 * class extracts either the minimum or maximum value as the sort key, consistent with how
 * {@code SortedSetSortField} behaves with a {@code MIN} or {@code MAX} selector.
 */
public final class MultiValuedBinaryDocValuesSortField extends BinarySortField {

    public static final String PROVIDER_NAME = "MultiValuedBinaryDocValuesSortField";

    private final boolean maxMode;
    private final BinaryDocValuesFormat binaryFormat;

    /** Returns {@code true} when this field uses the maximum (last) value for multi-valued documents. */
    boolean isMaxMode() {
        return maxMode;
    }

    /** How this field's binary doc values are laid out, and so which decoder extracts a sort key from them. */
    public BinaryDocValuesFormat binaryFormat() {
        return binaryFormat;
    }

    public MultiValuedBinaryDocValuesSortField(String field, boolean reverse, Object missingValue, boolean maxMode) {
        this(field, reverse, missingValue, maxMode, BinaryDocValuesFormat.SEPARATE_COUNT);
    }

    public MultiValuedBinaryDocValuesSortField(
        String field,
        boolean reverse,
        Object missingValue,
        boolean maxMode,
        BinaryDocValuesFormat binaryFormat
    ) {
        super(field, reverse, missingValue, PROVIDER_NAME);
        this.maxMode = maxMode;
        this.binaryFormat = binaryFormat;
    }

    @Override
    protected BinaryDocValues getSortKeyDocValues(LeafReader reader) throws IOException {
        BinaryDocValues values = DocValues.getBinary(reader, getField());
        return switch (binaryFormat) {
            // The payload carries its own count, so there is nothing to advance alongside it.
            case COLUMNAR_PAYLOAD -> new ColumnarPayloadMinMaxBinaryDocValues(values, maxMode);
            case ARRAY_ORDER_INLINE_NULL, SEPARATE_COUNT -> {
                String countsFieldName = getField() + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;
                NumericDocValues counts = reader.getNumericDocValues(countsFieldName);
                if (counts == null) {
                    // PlainBinary (single-valued field): raw bytes are the sort key.
                    yield values;
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
                    yield values;
                }
                yield new MinMaxBinaryDocValues(values, counts, maxMode, binaryFormat);
            }
        };
    }

    /**
     * Decodes the minimum ({@code maxMode=false}) or maximum ({@code maxMode=true}) sort key from a document's raw
     * binary doc values blob, dispatching to whichever decoder matches the field's {@code format}. Shared by
     * {@link MinMaxBinaryDocValues#binaryValue()} and {@code LongValuesComparatorSource}'s host.name singleton check.
     *
     * <p>{@code count} is the companion {@code .counts} value, and is ignored for a columnar payload, which carries
     * its own count and writes no companion — so a caller that has no count in hand can pass anything for it.
     *
     * <p>Returns {@code null} for a columnar payload holding no non-null slot — an empty array, or one holding nothing
     * but nulls. The other formats never write a blob for such a document, so they have nothing to be asked about and
     * always return a key; the payload describes them, and handing its bytes back would sort the document on its own
     * framing.
     */
    @Nullable
    public static BytesRef decodeExtreme(BytesRef raw, long count, boolean maxMode, BinaryDocValuesFormat format) throws IOException {
        return switch (format) {
            case COLUMNAR_PAYLOAD -> new StringBinaryPayload.Decoder().extreme(raw, maxMode);
            // count=1 (or a lone slot): raw bytes are the sort key in either encoding, no decoding needed.
            case ARRAY_ORDER_INLINE_NULL -> count <= 1
                ? raw
                : MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.decodeExtreme(raw, (int) count, maxMode);
            case SEPARATE_COUNT -> count <= 1 ? raw : MultiValuedBinaryDocValuesField.SeparateCount.decodeExtreme(raw, maxMode);
        };
    }

    /**
     * Wraps a columnar payload field, returning either the minimum or maximum non-null value as the sort key.
     *
     * <p>A payload is written for every present document, including one whose slots are all null and one holding no slot at all. Neither
     * has a value to sort on, so both are skipped here and read as missing — which is what the other encodings get for free by writing no
     * blob for them. Skipping in the iterator rather than at {@link #binaryValue()} is what both index-sort drivers understand: they take
     * a document the cursor stepped over as having no value.
     */
    private static final class ColumnarPayloadMinMaxBinaryDocValues extends FilterBinaryDocValues {
        private final boolean maxMode;
        private final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        private BytesRef sortKey;

        ColumnarPayloadMinMaxBinaryDocValues(BinaryDocValues values, boolean maxMode) {
            super(values);
            this.maxMode = maxMode;
        }

        @Override
        public int nextDoc() throws IOException {
            return skipToValued(in.nextDoc());
        }

        @Override
        public int advance(int target) throws IOException {
            return skipToValued(in.advance(target));
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (in.advanceExact(target) && decodeSortKey()) {
                return true;
            }
            sortKey = null;
            return false;
        }

        /**
         * The sort key of the document this is positioned on. Only meaningful once a positioning call has reported a value;
         * {@code null} otherwise, rather than the previous document's key.
         */
        @Override
        public BytesRef binaryValue() {
            return sortKey;
        }

        /** Steps forward from {@code doc} to the first document that has a sort key, decoding it on the way. */
        private int skipToValued(int doc) throws IOException {
            for (; doc != NO_MORE_DOCS; doc = in.nextDoc()) {
                if (decodeSortKey()) {
                    return doc;
                }
            }
            sortKey = null;
            return NO_MORE_DOCS;
        }

        /** Decodes the sort key of the document {@code in} is positioned on, reporting whether it has one at all. */
        private boolean decodeSortKey() throws IOException {
            sortKey = decoder.extreme(in.binaryValue(), maxMode);
            return sortKey != null;
        }
    }

    /**
     * Wraps binary doc values, returning either the minimum or maximum value as the sort key — decoded according to
     * whichever encoding ({@code SeparateCount} or {@code ArrayOrderInlineNull}) this field actually uses.
     */
    private static final class MinMaxBinaryDocValues extends FilterBinaryDocValues {
        private final NumericDocValues counts;
        private final boolean maxMode;
        private final BinaryDocValuesFormat binaryFormat;

        MinMaxBinaryDocValues(BinaryDocValues values, NumericDocValues counts, boolean maxMode, BinaryDocValuesFormat binaryFormat) {
            super(values);
            this.counts = counts;
            this.maxMode = maxMode;
            this.binaryFormat = binaryFormat;
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
            return decodeExtreme(in.binaryValue(), counts.longValue(), maxMode, binaryFormat);
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
            // Ordinals, and the two this replaced a boolean with hold its former 0/1: segments written before the
            // format became three-valued read back as SEPARATE_COUNT and ARRAY_ORDER_INLINE_NULL unchanged. A value we
            // do not know is a corrupt segment, or one from a newer node that appended a format; either way we cannot
            // decode the field.
            int formatOrdinal = in.readInt();
            BinaryDocValuesFormat[] formats = BinaryDocValuesFormat.values();
            if (formatOrdinal < 0 || formatOrdinal >= formats.length) {
                throw new CorruptIndexException("unknown binary doc values format ordinal [" + formatOrdinal + "]", in);
            }
            return new MultiValuedBinaryDocValuesSortField(field, reverse, missingValue, maxMode, formats[formatOrdinal]);
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
            out.writeInt(msf.binaryFormat.ordinal());
        }
    }
}
