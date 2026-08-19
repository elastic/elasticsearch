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
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.numeric.NumericColumnWriter;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.columnar.numeric.SkipIndexCodec;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
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
    private final IndexOutput skipIndex;
    private final List<FieldEntry> fields = new ArrayList<>();
    private final NumericPipelineSelector pipelineSelector;
    private final int blockSize;
    private boolean closed = false;

    private record FieldEntry(int fieldNumber, byte fieldTypeId, NumericColumnMetadata metadata) {}

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

            String skipName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.SKIP_EXTENSION
            );
            skipIndex = state.directory.createOutput(skipName, state.context);
            ColumnarCodecUtil.writeHeader(
                skipIndex,
                ColumNARDocValuesFormat.SKIP_CODEC,
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
            throw new UnsupportedOperationException("ColumNAR field type [" + type + "] is not implemented yet");
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
        if (type.isNumeric() == false) {
            throw new UnsupportedOperationException("ColumNAR field type [" + type + "] is not implemented yet");
        }
        writeNumericColumn(field, type, () -> mergeCursor(field, mergeState));
    }

    private static NumericColumnValues mergeCursor(FieldInfo field, MergeState mergeState) throws IOException {
        List<MergeSub> subs = new ArrayList<>();
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
            subs.add(new MergeSub(mergeState.docMaps[i], values));
        }

        DocIDMerger<MergeSub> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        long finalCost = cost;
        return new NumericColumnValues() {
            private MergeSub current;
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

    private static final class MergeSub extends DocIDMerger.Sub {
        private final NumericColumnValues values;

        MergeSub(MergeState.DocMap docMap, NumericColumnValues values) {
            super(docMap);
            this.values = values;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
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
            data,
            skipIndex
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
            CodecUtil.writeFooter(skipIndex);
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, skipIndex, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, skipIndex, meta);
            }
        }
    }
}
