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
        INLINE  // order is preserved in the binary blob with per-doc deduplication: [D][distinct values][per-slot ordinals]
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
        BinaryAndCounts bc = BinaryAndCounts.get(breaker, context, fieldName, true);
        if (bc == null) {
            return ConstantNull.COLUMN_READER;
        }
        if (bc.counts() == null) {
            // The .counts skipper proved maxValue <= 1, so no document carries the multi-slot ([count][...]/[len+1][val]) encoding: every
            // present blob is a single raw value and an absent blob is a lone null / empty array, which the plain reader emits as null.
            return new BytesRefsFromBinaryBlockLoader.BytesRefsFromBinary(bc.binary());
        }
        if (arrayOrderSource == ArrayOrderSource.INLINE) {
            // Multi-slot documents exist (maxValue >= 2): decode the in-order inline-null format, advancing on the counts column since an
            // all-null or empty array writes a count but no binary blob.
            return new ArrayOrderDeduplicated(bc.binary(), bc.counts());
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
     * Reader for {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ArrayOrderDeduplicated ArrayOrderDeduplicated}.
     * Decodes the per-doc deduplicating blob, drops null ordinals (0), and emits the non-null values in document order. Advances on
     * the {@code .counts} field, since an all-null or empty array writes a count but no binary blob.
     * <p>
     * When {@code slotCount == distinctCount} (no duplicates, no nulls) the blob is {@code [D][len1][val1]...[lenD][valD]} and no
     * ordinal stream follows. When {@code slotCount > distinctCount} the ordinal stream {@code [ord1]...[ordSlotCount]} is appended
     * and {@code ord == 0} marks a null slot.
     */
    static class ArrayOrderDeduplicated extends AbstractBytesRefsFromBinaryReader {

        private final TrackingNumericDocValues counts;
        private final ByteArrayStreamInput in = new ByteArrayStreamInput();
        private final BytesRef scratch = new BytesRef();
        private int[] distinctOffsets = new int[4];
        private int[] distinctLengths = new int[4];
        private int[] ordinalOffsets = new int[8];
        private int[] ordinalLengths = new int[8];

        ArrayOrderDeduplicated(TrackingBinaryDocValues docValues, TrackingNumericDocValues counts) {
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

            // Read distinctCount distinct values, capturing their offsets and lengths within the blob.
            int distinctCount = in.readVInt();
            ensureDistinctCapacity(distinctCount);
            for (int d = 0; d < distinctCount; d++) {
                int length = in.readVInt();
                int offset = in.getPosition();
                in.setPosition(offset + length);
                distinctOffsets[d] = offset;
                distinctLengths[d] = length;
            }

            // When slotCount == distinctCount there are no duplicates and no nulls: the distinct values in first-seen order ARE the slots.
            // When slotCount > distinctCount the ordinal stream follows and ordinal 0 marks a null slot.
            ensureOrdinalCapacity(slotCount); // slotCount >= distinctCount, so this covers both branches
            int nonNull;
            if (slotCount == distinctCount) {
                System.arraycopy(distinctOffsets, 0, ordinalOffsets, 0, distinctCount);
                System.arraycopy(distinctLengths, 0, ordinalLengths, 0, distinctCount);
                nonNull = distinctCount;
            } else {
                nonNull = 0;
                for (int i = 0; i < slotCount; i++) {
                    int ord = in.readVInt();
                    if (ord == 0) {
                        continue; // null slot dropped
                    }
                    ordinalOffsets[nonNull] = distinctOffsets[ord - 1];
                    ordinalLengths[nonNull] = distinctLengths[ord - 1];
                    nonNull++;
                }
            }

            if (nonNull == 0) {
                builder.appendNull();
            } else if (nonNull == 1) {
                scratch.offset = ordinalOffsets[0];
                scratch.length = ordinalLengths[0];
                builder.appendBytesRef(scratch);
            } else {
                builder.beginPositionEntry();
                for (int i = 0; i < nonNull; i++) {
                    scratch.offset = ordinalOffsets[i];
                    scratch.length = ordinalLengths[i];
                    builder.appendBytesRef(scratch);
                }
                builder.endPositionEntry();
            }
        }

        private void ensureDistinctCapacity(int minSize) {
            if (distinctOffsets.length < minSize) {
                distinctOffsets = ArrayUtil.grow(distinctOffsets, minSize);
                distinctLengths = ArrayUtil.grow(distinctLengths, minSize);
            }
        }

        private void ensureOrdinalCapacity(int minSize) {
            if (ordinalOffsets.length < minSize) {
                ordinalOffsets = ArrayUtil.grow(ordinalOffsets, minSize);
                ordinalLengths = ArrayUtil.grow(ordinalLengths, minSize);
            }
        }

        @Override
        public String toString() {
            return "BytesRefsFromArrayOrderDeduplicatedBinarySeparateCount";
        }

        @Override
        public void close() {
            Releasables.close(super::close, counts);
        }
    }
}
