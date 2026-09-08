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
import org.elasticsearch.columnar.string.StringBlockSink;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.BinaryAndCounts;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;

import java.io.IOException;
import java.util.function.BiFunction;

/**
 * Block loader for multi-value binary fields which store count in a separate parallel numeric doc value column.
 */
public class BytesRefsFromBinaryMultiSeparateCountBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final String fieldName;
    private final BinaryDocValuesFormat binaryFormat;

    public BytesRefsFromBinaryMultiSeparateCountBlockLoader(String fieldName) {
        this(fieldName, BinaryDocValuesFormat.SEPARATE_COUNT);
    }

    public BytesRefsFromBinaryMultiSeparateCountBlockLoader(String fieldName, BinaryDocValuesFormat binaryFormat) {
        this.fieldName = fieldName;
        this.binaryFormat = binaryFormat;
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
        return switch (binaryFormat) {
            case COLUMNAR_PAYLOAD -> {
                // The count travels in the blob, so there is no companion column to load or advance on.
                TrackingBinaryDocValues binary = TrackingBinaryDocValues.get(breaker, context, fieldName);
                yield binary == null ? ConstantNull.COLUMN_READER : new ColumnarPayload(binary);
            }
            // Multi-slot documents exist (maxValue >= 2): decode the in-order inline-null format, advancing on the counts column since an
            // all-null or empty array writes a count but no binary blob.
            case ARRAY_ORDER_INLINE_NULL -> withCounts(breaker, context, ArrayOrderInlineNull::new);
            case SEPARATE_COUNT -> withCounts(breaker, context, BytesRefsFromBinarySeparateCount::new);
        };
    }

    /**
     * Resolves the binary column and its {@code .counts} companion, which both companion-carrying framings need, and
     * hands them to {@code reader}.
     */
    private ColumnAtATimeReader withCounts(
        CircuitBreaker breaker,
        LeafReaderContext context,
        BiFunction<TrackingBinaryDocValues, TrackingNumericDocValues, ColumnAtATimeReader> reader
    ) throws IOException {
        BinaryAndCounts bc = BinaryAndCounts.get(breaker, context, fieldName, true);
        if (bc == null) {
            return ConstantNull.COLUMN_READER;
        }
        if (bc.counts() == null) {
            // The .counts skipper proved maxValue <= 1, so no document carries the multi-slot ([count][...]/[len+1][val]) encoding: every
            // present blob is a single raw value and an absent blob is a lone null / empty array, which the plain reader emits as null.
            return new BytesRefsFromBinaryBlockLoader.BytesRefsFromBinary(bc.binary());
        }
        return reader.apply(bc.binary(), bc.counts());
    }

    /**
     * Reader for the columnar codec's payload, where the slot count is carried in the blob. Drops nulls and emits the non-null values in
     * document order; a document whose slots are all null, or which holds none at all, emits a null.
     */
    static class ColumnarPayload extends AbstractBytesRefsFromBinaryReader {

        private final MultiValueColumnarPayloadBinaryDocValuesReader reader = new MultiValueColumnarPayloadBinaryDocValuesReader();

        ColumnarPayload(TrackingBinaryDocValues docValues) {
            super(docValues);
        }

        /**
         * A page read from the column rather than a payload decoded for every document.
         *
         * <p>Where the column keeps a dictionary this is the shape it already stores: the page comes back as ordinals
         * into its own distinct values, each resolved once however many documents name it, and is handed over without
         * a lookup per value or a remapping. Where it does not, the page still comes back a page at a time, and a run
         * of equal values is copied once.
         *
         * <p>The column declines a page covering a document it has no value for, since a page has no way to say which
         * one; the payload path below then reads them, as it does for a segment that arrives as an overlay rather than
         * as a column.
         *
         * <p>A document may repeat within a page, which a lookup or a top-n asks for. The iterator a page resolves its
         * ranks through is only required not to be moved backwards, so asking it twice for the same document is a
         * position it already holds.
         */
        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            if (docValues.docValues() instanceof StringColumnSource columnar) {
                final int count = docs.count() - offset;
                if (count > 0) {
                    final int[] wanted = new int[count];
                    for (int i = 0; i < count; i++) {
                        wanted[i] = docs.get(offset + i);
                    }
                    final PageSink sink = new PageSink(factory);
                    if (columnar.reader().readBlock(wanted, 0, count, sink)) {
                        return sink.block;
                    }
                }
            }
            return super.read(factory, docs, offset, nullsFiltered);
        }

        @Override
        public void read(int doc, BlockLoader.BytesRefBuilder builder) throws IOException {
            if (docValues.docValues().advanceExact(doc) == false) {
                builder.appendNull(); // field absent for this document
                return;
            }
            reader.read(docValues.docValues().binaryValue(), builder);
        }

        @Override
        public String toString() {
            return "BytesRefsFromColumnarPayload";
        }
    }

    /** Turns a page of a string column into a block, in whichever of the two shapes the page arrived. */
    private static final class PageSink implements StringBlockSink {

        private final BlockLoader.BlockFactory factory;
        private BlockLoader.Block block;

        private PageSink(BlockLoader.BlockFactory factory) {
            this.factory = factory;
        }

        @Override
        public void appendOrdinals(int[] ordinals, int valueCount, int[] valueCounts, int docCount, BytesRef[] dictionary, int size) {
            block = factory.buildOrdinalBytesRefDirect(ordinals, valueCount, valueCounts, docCount, dictionary, size);
        }

        @Override
        public void appendValues(BytesRef[] values, int valueCount, int[] valueCounts, int docCount) {
            // Sized in positions, which is what the block holds - a multi-valued page has more values than those.
            try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docCount)) {
                int at = 0;
                for (int doc = 0; doc < docCount; doc++) {
                    final int held = valueCounts == null ? 1 : valueCounts[doc];
                    if (held == 0) {
                        builder.appendNull();
                    } else if (held == 1) {
                        builder.appendBytesRef(values[at++]);
                    } else {
                        builder.beginPositionEntry();
                        for (int i = 0; i < held; i++) {
                            builder.appendBytesRef(values[at++]);
                        }
                        builder.endPositionEntry();
                    }
                }
                assert at == valueCount : "read " + at + " values, was given " + valueCount;
                block = builder.build();
            }
        }
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
