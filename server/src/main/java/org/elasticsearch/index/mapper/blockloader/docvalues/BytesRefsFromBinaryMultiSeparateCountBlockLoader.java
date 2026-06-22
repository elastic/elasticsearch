/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.FieldArrayContext;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.BinaryAndCounts;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedDocValues;

import java.io.IOException;

/**
 * Block loader for multi-value binary fields which store count in a separate parallel numeric doc value column.
 */
public class BytesRefsFromBinaryMultiSeparateCountBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    /**
     * Where a document's array order is recorded.
     */
    public enum ArrayOrderSource {
        NONE,  // no ordering
        FROM_OFFSETS,  // reconstructs order from a sidebar .offsets field
        INLINE  // order is already preserved in the binary blob, so reads the blob directly
    }

    private final String fieldName;
    private final ArrayOrderSource arrayOrderSource;

    public BytesRefsFromBinaryMultiSeparateCountBlockLoader(String fieldName) {
        this(fieldName, ArrayOrderSource.NONE);
    }

    public BytesRefsFromBinaryMultiSeparateCountBlockLoader(String fieldName, boolean readInArrayOrder) {
        this(fieldName, readInArrayOrder ? ArrayOrderSource.FROM_OFFSETS : ArrayOrderSource.NONE);
    }

    public BytesRefsFromBinaryMultiSeparateCountBlockLoader(String fieldName, ArrayOrderSource arrayOrderSource) {
        this.fieldName = fieldName;
        this.arrayOrderSource = arrayOrderSource;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    // For ConditionalBlockLoader:
    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        if (context.reader().getFieldInfos().fieldInfo(fieldName) == null) {
            return ConstantNull.ROW_READER;
        }

        AbstractBytesRefsFromBinaryReader columnAtATimeReader = (AbstractBytesRefsFromBinaryReader) reader(breaker, context);
        return new RowStrideReader() {
            @Override
            public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                columnAtATimeReader.read(docId, storedFields, builder);
            }

            @Override
            public boolean canReuse(int startingDocID) {
                return columnAtATimeReader.canReuse(startingDocID);
            }

            @Override
            public void close() {
                columnAtATimeReader.close();
            }
        };
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        if (arrayOrderSource == ArrayOrderSource.INLINE) {
            // The ArrayOrderInlineNull format never collapses to "all counts == 1 means single value" (a lone null is count==1 with no
            // binary blob), so we must always load the counts column and advance on it.
            BinaryAndCounts bc = BinaryAndCounts.get(breaker, context, fieldName, false);
            if (bc == null) {
                // Binary field absent in this segment (only all-null / empty arrays): every value reads as null.
                return ConstantNull.COLUMN_READER;
            }
            return new ArrayOrderInlineNull(bc.binary(), bc.counts());
        }
        BinaryAndCounts bc = BinaryAndCounts.get(breaker, context, fieldName, true);
        if (bc == null) {
            return ConstantNull.COLUMN_READER;
        }
        if (bc.counts() == null) {
            return new BytesRefsFromBinaryBlockLoader.BytesRefsFromBinary(bc.binary());
        }
        if (arrayOrderSource == ArrayOrderSource.FROM_OFFSETS) {
            TrackingSortedDocValues offsets;
            try {
                offsets = TrackingSortedDocValues.get(breaker, context, FieldArrayContext.offsetsFieldName(fieldName));
            } catch (Exception e) {
                // We already reserved breaker space for the binary and counts doc values above. If acquiring the offsets companion fails
                // (ex. circuit breaker) we must release that reservation here, otherwise it leaks.
                Releasables.close(bc.binary(), bc.counts());
                throw e;
            }
            if (offsets != null) {
                return new ArrayOrder(bc.binary(), bc.counts(), offsets);
            }
        }
        return new BytesRefsFromBinarySeparateCount(bc.binary(), bc.counts());
    }

    static class BytesRefsFromBinarySeparateCount extends AbstractBytesRefsFromBinaryReader {

        protected final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();
        protected final TrackingNumericDocValues counts;

        BytesRefsFromBinarySeparateCount(TrackingBinaryDocValues docValues, TrackingNumericDocValues counts) {
            super(docValues);
            this.counts = counts;
        }

        @Override
        public void read(int doc, BytesRefBuilder builder) throws IOException {
            if (false == docValues.docValues().advanceExact(doc)) {
                builder.appendNull();
                return;
            }

            boolean advanced = counts.docValues().advanceExact(doc);
            assert advanced;

            reader.read(docValues.docValues().binaryValue(), counts.docValues().longValue(), builder);
        }

        @Override
        public String toString() {
            return "BytesRefsFromBinarySeparateCount";
        }

        @Override
        public void close() {
            Releasables.close(super::close, counts);
        }
    }

    static class ArrayOrder extends AbstractBytesRefsFromBinaryReader {

        private final BytesRefsFromBinarySeparateCount separateCountFallback;
        private final TrackingNumericDocValues counts;
        private final TrackingSortedDocValues offsets;
        private final ByteArrayStreamInput scratch = new ByteArrayStreamInput();
        private final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

        ArrayOrder(TrackingBinaryDocValues docValues, TrackingNumericDocValues counts, TrackingSortedDocValues offsets) {
            super(docValues);
            this.offsets = offsets;
            this.counts = counts;
            this.separateCountFallback = new BytesRefsFromBinarySeparateCount(docValues, counts);
        }

        @Override
        public void read(int docId, BlockLoader.BytesRefBuilder builder) throws IOException {
            int[] offsetToOrd = OffsetsAwareBlockLoaderHelper.readOffsets(offsets.docValues(), scratch, docId);

            // if no offsets were recorded, delegate to the non-ordered per-doc emit inherited from the parent
            if (offsetToOrd == null) {
                separateCountFallback.read(docId, builder);
                return;
            }

            // no values arrived (all slots null) — emit a single null position
            if (docValues.docValues().advanceExact(docId) == false) {
                assert OffsetsAwareBlockLoaderHelper.allNulls(offsetToOrd);
                builder.appendNull();
                return;
            }

            boolean advanced = counts.docValues().advanceExact(docId);
            assert advanced;

            // materialize the per-doc values once so we can index into them by ord
            BytesRef[] materialized = reader.materialize(docValues.docValues().binaryValue(), counts.docValues().longValue());

            OffsetsAwareBlockLoaderHelper.emit(offsetToOrd, builder, ord -> builder.appendBytesRef(materialized[ord]));
        }

        @Override
        public String toString() {
            return "BytesRefsFromBinarySeparateCount.ArrayOrder";
        }

        @Override
        public void close() {
            Releasables.close(super::close, counts, offsets);
        }
    }

    /**
     * Reader for {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ArrayOrderInlineNull ArrayOrderInlineNull}.
     * Drops nulls and emits the non-null values in document order (a single non-null value as a bare value, two or more inside a
     * position entry). Advances on the {@code .counts} field, since an all-null or empty array writes a count but no binary blob.
     */
    static class ArrayOrderInlineNull extends AbstractBytesRefsFromBinaryReader {

        private final TrackingNumericDocValues counts;
        private final ByteArrayStreamInput in = new ByteArrayStreamInput();
        private final BytesRef scratch = new BytesRef();
        private int[] offsets = new int[8];
        private int[] lengths = new int[8];

        ArrayOrderInlineNull(TrackingBinaryDocValues docValues, TrackingNumericDocValues counts) {
            super(docValues);
            this.counts = counts;
        }

        @Override
        public int docId() {
            return counts.docValues().docID();
        }

        @Override
        public void read(int doc, BlockLoader.BytesRefBuilder builder) throws IOException {
            if (counts.docValues().advanceExact(doc) == false) {
                builder.appendNull(); // field absent for this document
                return;
            }
            int slotCount = Math.toIntExact(counts.docValues().longValue());
            if (docValues.docValues().advanceExact(doc) == false) {
                // all-null array or empty array: no non-null values
                builder.appendNull();
                return;
            }
            BytesRef bytes = docValues.docValues().binaryValue();
            if (slotCount == 1) {
                builder.appendBytesRef(bytes); // single non-null value stored raw
                return;
            }
            scratch.bytes = bytes.bytes;
            in.reset(bytes.bytes, bytes.offset, bytes.length);
            int nonNull = 0;
            for (int i = 0; i < slotCount; i++) {
                int encodedLength = in.readVInt();
                if (encodedLength == 0) {
                    continue; // null slot dropped
                }
                int length = encodedLength - 1;
                int offset = in.getPosition();
                in.setPosition(offset + length);
                ensureCapacity(nonNull + 1);
                offsets[nonNull] = offset;
                lengths[nonNull] = length;
                nonNull++;
            }
            if (nonNull == 0) {
                // binary present implies at least one non-null value, but stay defensive
                builder.appendNull();
            } else if (nonNull == 1) {
                scratch.offset = offsets[0];
                scratch.length = lengths[0];
                builder.appendBytesRef(scratch);
            } else {
                builder.beginPositionEntry();
                for (int i = 0; i < nonNull; i++) {
                    scratch.offset = offsets[i];
                    scratch.length = lengths[i];
                    builder.appendBytesRef(scratch);
                }
                builder.endPositionEntry();
            }
        }

        private void ensureCapacity(int minSize) {
            if (offsets.length < minSize) {
                offsets = ArrayUtil.grow(offsets, minSize);
                lengths = ArrayUtil.grow(lengths, minSize);
            }
        }

        @Override
        public String toString() {
            return "BytesRefsFromArrayOrderInlineNullBinarySeparateCount";
        }

        @Override
        public void close() {
            Releasables.close(super::close, counts);
        }
    }
}
