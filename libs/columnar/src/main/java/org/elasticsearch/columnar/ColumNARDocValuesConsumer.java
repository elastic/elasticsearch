/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.numeric.NumericColumnWriter;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.columnar.numeric.SkipIndexCodec;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.StringColumnMetadata;
import org.elasticsearch.columnar.string.StringColumnValues;
import org.elasticsearch.columnar.string.StringColumnWriter;
import org.elasticsearch.columnar.string.ValueStream;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes tagged columns onto the binary substrate; numeric types decode their {@code NumericBinaryPayload}
 * into the long column. Field metadata is flushed on {@link #close()}.
 *
 * <p><b>Merge contract.</b> {@link #mergeBinaryField} re-encodes all source segments through the
 * current writer's pipeline. There is no version-preserving merge and no mixed-version output
 * segment: a force-merge is a silent format upgrade.
 */
final class ColumNARDocValuesConsumer extends DocValuesConsumer {

    private final int maxDoc;
    private final Directory directory;
    private final IOContext context;
    private final IndexOutput data;
    private final IndexOutput meta;
    private final List<FieldEntry> fields = new ArrayList<>();
    private final NumericPipelineSelector pipelineSelector;
    private final int blockSize;

    /** Bytes a chunk of a string column's byte stream holds before it is closed and compressed. */
    private static final int TARGET_CHUNK_BYTES = 64 * 1024;
    private boolean closed = false;

    private record FieldEntry(int fieldNumber, byte fieldTypeId, ColumnMetadata metadata) {}

    ColumNARDocValuesConsumer(SegmentWriteState state, NumericPipelineSelector pipelineSelector, int blockSize) throws IOException {
        this.pipelineSelector = pipelineSelector;
        this.blockSize = blockSize;
        this.maxDoc = state.segmentInfo.maxDoc();
        this.directory = state.directory;
        this.context = state.context;
        boolean success = false;
        try {
            String dataName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.DATA_EXTENSION
            );
            data = state.directory.createOutput(dataName, state.context);
            ColumnarCodecUtil.writeHeader(
                data,
                ColumNARDocValuesFormat.DATA_CODEC,
                FormatVersion.CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );

            String metaName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.META_EXTENSION
            );
            meta = state.directory.createOutput(metaName, state.context);
            ColumnarCodecUtil.writeHeader(
                meta,
                ColumNARDocValuesFormat.META_CODEC,
                FormatVersion.CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        ColumnarFieldType type = ColumnarFieldType.fromField(field);
        if (type.isNumeric()) {
            writeNumericColumn(field, type, () -> ColumnarNumericBinaryDocValues.decodePayloads(valuesProducer.getBinary(field)));
        } else {
            assert type == ColumnarFieldType.STRING : "Unsupported ColumNAR type [" + type + "]";
            writeStringColumn(field, type, () -> ColumnarStringBinaryDocValues.decodePayloads(valuesProducer.getBinary(field)));
        }
    }

    /**
     * Merge: re-runs the encoder pipeline over the source segments, reading their values in bulk off
     * disk via {@link ColumnarNumericBinaryDocValues#directValues}. A fresh merge cursor
     * ({@link DocIDMerger} in merged doc order) is built per pass — count, iterator, values (the skip
     * index is built inline while the values are encoded, so it needs no pass of its own).
     */
    @Override
    public void mergeBinaryField(FieldInfo field, MergeState mergeState) throws IOException {
        ColumnarFieldType type = ColumnarFieldType.fromField(field);
        if (type.isNumeric()) {
            writeNumericColumn(field, type, () -> numericMergeCursor(field, mergeState));
        } else {
            assert type == ColumnarFieldType.STRING : "Unsupported ColumNAR type [" + type + "]";
            writeStringColumn(field, type, () -> stringMergeCursor(field, mergeState));
        }
    }

    private static NumericColumnValues numericMergeCursor(FieldInfo field, MergeState mergeState) throws IOException {
        List<ColumnMergeSub<NumericColumnValues>> subs = new ArrayList<>();
        long cost = 0;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            DocValuesProducer producer = mergeState.docValuesProducers[i];
            if (producer == null) {
                continue;
            }
            FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(field.name);
            if (readerField == null || readerField.getDocValuesType() != DocValuesType.BINARY) {
                continue;
            }
            BinaryDocValues binary = producer.getBinary(readerField);
            if (binary == null) {
                continue;
            }
            // Read decoded longs directly for our own columns; fall back to the payload for anything else.
            NumericColumnValues values = binary instanceof ColumnarNumericBinaryDocValues columnar
                ? columnar.directValues()
                : ColumnarNumericBinaryDocValues.decodePayloads(binary);
            cost += values.cost();
            subs.add(new ColumnMergeSub<>(mergeState.docMaps[i], values));
        }

        DocIDMerger<ColumnMergeSub<NumericColumnValues>> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        long finalCost = cost;
        return new NumericColumnValues() {
            private ColumnMergeSub<NumericColumnValues> current;
            private int docID = -1;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = merger.next();
                docID = current == null ? DocIdSetIterator.NO_MORE_DOCS : current.mappedDocID;
                return docID;
            }

            @Override
            public int valueCount() {
                return current.values.valueCount();
            }

            @Override
            public long nextValue() throws IOException {
                return current.values.nextValue();
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }
        };
    }

    /**
     * One source segment's cursor, in merged doc order. The type parameter keeps the column's own value
     * accessors reachable through {@link #values}, which {@link DocIDMerger.Sub} itself does not expose.
     */
    private static final class ColumnMergeSub<T extends DocIdSetIterator> extends DocIDMerger.Sub {
        private final T values;

        ColumnMergeSub(MergeState.DocMap docMap, T values) {
            super(docMap);
            this.values = values;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
    }

    /**
     * The string counterpart of {@link #numericMergeCursor}: reads each source segment's values in bulk off disk via
     * {@link ColumnarStringBinaryDocValues#directValues}, in merged doc order. A fresh cursor is built per pass
     * — count plus cardinality probe, iterator, then values.
     */
    private static StringColumnValues stringMergeCursor(FieldInfo field, MergeState mergeState) throws IOException {
        List<ColumnMergeSub<StringColumnValues>> subs = new ArrayList<>();
        long cost = 0;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            DocValuesProducer producer = mergeState.docValuesProducers[i];
            if (producer == null) {
                continue;
            }
            FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(field.name);
            if (readerField == null || readerField.getDocValuesType() != DocValuesType.BINARY) {
                continue;
            }
            BinaryDocValues binary = producer.getBinary(readerField);
            if (binary == null) {
                continue;
            }
            // Read decoded values directly for our own columns; fall back to the payload for anything else.
            StringColumnValues values = binary instanceof ColumnarStringBinaryDocValues columnar
                ? columnar.directValues()
                : ColumnarStringBinaryDocValues.decodePayloads(binary);
            cost += values.cost();
            subs.add(new ColumnMergeSub<>(mergeState.docMaps[i], values));
        }

        DocIDMerger<ColumnMergeSub<StringColumnValues>> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        long finalCost = cost;
        return new StringColumnValues() {
            private ColumnMergeSub<StringColumnValues> current;
            private int docID = -1;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = merger.next();
                docID = current == null ? DocIdSetIterator.NO_MORE_DOCS : current.mappedDocID;
                return docID;
            }

            @Override
            public int valueCount() {
                return current.values.valueCount();
            }

            @Override
            public BytesRef nextValue() throws IOException {
                return current.values.nextValue();
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }
        };
    }

    private void writeNumericColumn(FieldInfo field, ColumnarFieldType type, IOSupplier<NumericColumnValues> cursors) throws IOException {
        // Count in one pass, then stream the values block by block from fresh cursors — never buffer
        // the whole field on-heap, so a large merge stays memory-bounded.
        int numDocsWithField = 0;
        long numValues = 0;
        NumericColumnValues counter = cursors.get();
        for (int doc = counter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = counter.nextDoc()) {
            numDocsWithField++;
            numValues += counter.valueCount();
        }

        // A BINARY field can't carry a skipper, so the column builds its own skip index inline
        // during the value-encode pass — no extra cursor over the data.
        final NumericPipeline pipeline = pipelineSelector.select(field.name, type).build(blockSize);
        assert pipeline.blockSize() == blockSize
            : "template ignored blockSize argument: built " + pipeline.blockSize() + ", expected " + blockSize;
        NumericColumnMetadata metadata = NumericColumnWriter.write(
            maxDoc,
            numDocsWithField,
            numValues,
            cursors,
            pipeline,
            BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
            SkipIndexCodec.forId(SkipIndexCodec.MULTI_LEVEL_ID),
            directory,
            context,
            data
        );
        fields.add(new FieldEntry(field.number, type.id(), metadata));
    }

    /**
     * Counts the column in one pass, then streams the values block by block from fresh cursors — never
     * buffering the whole field on-heap.
     */
    private void writeStringColumn(FieldInfo field, ColumnarFieldType type, IOSupplier<StringColumnValues> cursors) throws IOException {
        int numDocsWithField = 0;
        long numValues = 0;
        StringColumnValues counter = cursors.get();
        for (int doc = counter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = counter.nextDoc()) {
            numDocsWithField++;
            int count = counter.valueCount();
            if (count != 1) {
                throw new UnsupportedOperationException(
                    "ColumNAR string columns are single-valued; document [" + doc + "] of field [" + field.name + "] has " + count
                );
            }
            numValues += count;
        }

        StringColumnMetadata metadata = StringColumnWriter.write(
            maxDoc,
            numDocsWithField,
            numValues,
            cursors,
            ValueStream.VALUES_PER_BLOCK,
            ChunkCodec.ZSTD,
            TARGET_CHUNK_BYTES,
            directory,
            context,
            data
        );
        fields.add(new FieldEntry(field.number, type.id(), metadata));
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("numeric");
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("sorted-numeric");
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("sorted");
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("sorted-set");
    }

    private static UnsupportedOperationException typedNotSupported(String shape) {
        return new UnsupportedOperationException(
            "ColumNAR is a binary doc-values format and does not handle "
                + shape
                + " doc values; store the field as a binary doc-values field carrying the '"
                + ColumNARDocValuesFormat.TYPE_ATTRIBUTE
                + "' attribute"
        );
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        boolean success = false;
        try {
            for (FieldEntry entry : fields) {
                meta.writeInt(entry.fieldNumber());
                meta.writeByte(entry.fieldTypeId());
                entry.metadata().writeTo(meta);
            }
            meta.writeInt(-1);
            CodecUtil.writeFooter(meta);
            CodecUtil.writeFooter(data);
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, meta);
            }
        }
    }
}
