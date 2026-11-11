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
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.comparators.LongComparator;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * A numeric {@link SortField} on a field whose doc values the ColumNAR codec stores: binary doc values holding a
 * {@link NumericBinaryPayload} per document. Lucene's {@code SortedNumericSortField} reads {@code SORTED_NUMERIC} doc values,
 * so it can't sort an index by such a field; this one sorts each document by the minimum or maximum of its values, as
 * {@code SortedNumericSortField} does with a {@code MIN} or {@code MAX} selector.
 *
 * <p>Like {@code SortedNumericSortField}, one class serves every numeric {@link SortField.Type}, which it records in segment
 * info alongside the rest of the sort. So far only {@code long} fields are stored with ColumNAR, so only {@link Type#LONG}
 * is accepted.
 */
// TODO: accept the other numeric types once ColumNAR stores them.
public final class ColumnarNumericSortField extends SortField {

    public static final String PROVIDER_NAME = "ColumnarNumericSortField";

    private final boolean maxMode;

    public ColumnarNumericSortField(String field, Type type, boolean reverse, boolean maxMode, @Nullable Object missingValue) {
        super(field, requireSupported(type), reverse);
        this.maxMode = maxMode;
        if (missingValue != null) {
            setMissingValue(missingValue);
        }
    }

    private static Type requireSupported(Type type) {
        return switch (type) {
            case LONG -> type;
            default -> throw new IllegalArgumentException("ColumNAR doesn't store [" + type + "] numeric fields");
        };
    }

    /** Whether documents sort by their maximum value rather than their minimum. */
    public boolean maxMode() {
        return maxMode;
    }

    /**
     * Whether a document in this segment may hold more than one value. The segment is sorted by each document's
     * minimum or maximum only, so where this is true the index order is not the order of every value the field holds,
     * and nothing may skip or stop early on it. A segment that cannot tell answers true.
     */
    public boolean isMaybeMultiValued(LeafReader reader) throws IOException {
        BinaryDocValues values = reader.getBinaryDocValues(getField());
        if (values == null) {
            return false;
        }
        // Payloads that are not a column, such as those of another codec, say nothing of the segment as a whole.
        return values instanceof ColumnarNumericBinaryDocValues columnar ? columnar.isSingleValued() == false : true;
    }

    @Override
    public IndexSorter getIndexSorter() {
        return switch (getType()) {
            case LONG -> new IndexSorter.LongSorter(PROVIDER_NAME, (Long) missingValue, reverse, this::sortKeys);
            default -> throw new AssertionError("unsupported numeric type [" + getType() + "]");
        };
    }

    @Override
    public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
        return switch (getType()) {
            case LONG -> new LongComparator(numHits, getField(), (Long) missingValue, reverse, Pruning.NONE) {
                @Override
                public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                    return new LongLeafComparator(context) {
                        @Override
                        protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                            return sortKeys(context.reader());
                        }
                    };
                }
            };
            default -> throw new AssertionError("unsupported numeric type [" + getType() + "]");
        };
    }

    /**
     * Each document's sort key. A document without a value writes no payload, so the binary doc values visit exactly the
     * documents that have one.
     */
    private NumericDocValues sortKeys(LeafReader reader) throws IOException {
        BinaryDocValues values = DocValues.getBinary(reader, getField());
        return values instanceof ColumnarNumericBinaryDocValues columnar
            ? new ColumnSortKeys(columnar, maxMode)
            : new PayloadSortKeys(values, maxMode);
    }

    private abstract static class SortKeys extends NumericDocValues {
        private final BinaryDocValues in;

        SortKeys(BinaryDocValues in) {
            this.in = in;
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
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public long cost() {
            return in.cost();
        }
    }

    /** Reads the extreme straight out of the column, without building the payload. */
    private static final class ColumnSortKeys extends SortKeys {
        private final ColumnarNumericBinaryDocValues columnar;
        private final boolean maxMode;

        ColumnSortKeys(ColumnarNumericBinaryDocValues columnar, boolean maxMode) {
            super(columnar);
            this.columnar = columnar;
            this.maxMode = maxMode;
        }

        @Override
        public long longValue() throws IOException {
            long extreme = columnar.valueAt(0);
            for (int i = 1, count = columnar.docValueCount(); i < count; i++) {
                long value = columnar.valueAt(i);
                extreme = maxMode ? Math.max(extreme, value) : Math.min(extreme, value);
            }
            return extreme;
        }
    }

    /** Decodes payloads that haven't been through the codec, such as the buffered ones a segment is sorted by at flush. */
    private static final class PayloadSortKeys extends SortKeys {
        private final BinaryDocValues values;
        private final boolean maxMode;
        private final long[][] decoded = { new long[8] };

        PayloadSortKeys(BinaryDocValues values, boolean maxMode) {
            super(values);
            this.values = values;
            this.maxMode = maxMode;
        }

        @Override
        public long longValue() throws IOException {
            int count = NumericBinaryPayload.decode(values.binaryValue(), decoded);
            assert count > 0 : "a document without a value writes no payload";
            long extreme = decoded[0][0];
            for (int i = 1; i < count; i++) {
                extreme = maxMode ? Math.max(extreme, decoded[0][i]) : Math.min(extreme, decoded[0][i]);
            }
            return extreme;
        }
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && maxMode == ((ColumnarNumericSortField) obj).maxMode;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Boolean.hashCode(maxMode);
    }

    /**
     * SPI provider so this sort field can be serialized to and deserialized from segment info. The layout, like
     * {@code SortedNumericSortField}'s, is the field, the numeric type by name, the order, the mode, and an optional missing
     * value written as its type.
     */
    public static final class Provider extends SortFieldProvider {

        public static final String NAME = PROVIDER_NAME;

        /** Public no-arg constructor required by the SPI mechanism. */
        public Provider() {
            super(NAME);
        }

        @Override
        public SortField readSortField(DataInput in) throws IOException {
            String field = in.readString();
            Type type = Type.valueOf(in.readString());
            boolean reverse = in.readInt() == 1;
            boolean maxMode = in.readInt() == 1;
            Object missingValue = null;
            if (in.readInt() == 1) {
                missingValue = switch (type) {
                    case LONG -> in.readLong();
                    default -> throw new IllegalArgumentException("ColumNAR doesn't store [" + type + "] numeric fields");
                };
            }
            return new ColumnarNumericSortField(field, type, reverse, maxMode, missingValue);
        }

        @Override
        public void writeSortField(SortField sf, DataOutput out) throws IOException {
            assert sf instanceof ColumnarNumericSortField;
            ColumnarNumericSortField sortField = (ColumnarNumericSortField) sf;
            out.writeString(sortField.getField());
            out.writeString(sortField.getType().name());
            out.writeInt(sortField.getReverse() ? 1 : 0);
            out.writeInt(sortField.maxMode ? 1 : 0);
            if (sortField.getMissingValue() == null) {
                out.writeInt(0);
            } else {
                out.writeInt(1);
                switch (sortField.getType()) {
                    case LONG -> out.writeLong((Long) sortField.getMissingValue());
                    default -> throw new AssertionError("unsupported numeric type [" + sortField.getType() + "]");
                }
            }
        }
    }
}
