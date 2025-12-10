/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.BFloat16;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.index.codec.vectors.es93.ES93BFloat16FlatVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;

public final class ES93BFloat16FlatVectorsWriter extends FlatVectorsWriter {

    private static final long SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ES93BFloat16FlatVectorsWriter.class);

    private final SegmentWriteState segmentWriteState;
    private final IndexOutput meta, vectorData;

    private final List<FieldWriter<?>> fields = new ArrayList<>();
    private boolean finished;

    public ES93BFloat16FlatVectorsWriter(SegmentWriteState state, FlatVectorsScorer scorer) throws IOException {
        super(scorer);
        segmentWriteState = state;
        String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES93BFloat16FlatVectorsFormat.META_EXTENSION
        );

        String vectorDataFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES93BFloat16FlatVectorsFormat.VECTOR_DATA_EXTENSION
        );

        boolean success = false;
        try {
            meta = state.directory.createOutput(metaFileName, state.context);
            vectorData = state.directory.createOutput(vectorDataFileName, state.context);

            CodecUtil.writeIndexHeader(
                meta,
                ES93BFloat16FlatVectorsFormat.META_CODEC_NAME,
                ES93BFloat16FlatVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.writeIndexHeader(
                vectorData,
                ES93BFloat16FlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
                ES93BFloat16FlatVectorsFormat.VERSION_CURRENT,
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
    public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        FieldWriter<?> newField = FieldWriter.create(fieldInfo);
        fields.add(newField);
        return newField;
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        for (FieldWriter<?> field : fields) {
            if (sortMap == null) {
                writeField(field, maxDoc);
            } else {
                writeSortingField(field, maxDoc, sortMap);
            }
            field.finish();
        }
    }

    @Override
    public void finish() throws IOException {
        if (finished) {
            throw new IllegalStateException("already finished");
        }
        finished = true;
        if (meta != null) {
            // write end of fields marker
            meta.writeInt(-1);
            CodecUtil.writeFooter(meta);
        }
        if (vectorData != null) {
            CodecUtil.writeFooter(vectorData);
        }
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_RAM_BYTES_USED;
        for (FieldWriter<?> field : fields) {
            total += field.ramBytesUsed();
        }
        return total;
    }

    private void writeField(FieldWriter<?> fieldData, int maxDoc) throws IOException {
        // write vector values
        long vectorDataOffset = vectorData.alignFilePointer(BFloat16.BYTES);
        switch (fieldData.fieldInfo.getVectorEncoding()) {
            case FLOAT32 -> writeBFloat16Vectors(fieldData);
            case BYTE -> throw new IllegalStateException(
                "Incorrect encoding for field " + fieldData.fieldInfo.name + ": " + VectorEncoding.BYTE
            );
        }
        long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

        writeMeta(fieldData.fieldInfo, maxDoc, vectorDataOffset, vectorDataLength, fieldData.docsWithField);
    }

    private void writeBFloat16Vectors(FieldWriter<?> fieldData) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(fieldData.dim * BFloat16.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (Object v : fieldData.vectors) {
            BFloat16.floatToBFloat16((float[]) v, buffer.asShortBuffer());
            vectorData.writeBytes(buffer.array(), buffer.array().length);
        }
    }

    private void writeSortingField(FieldWriter<?> fieldData, int maxDoc, Sorter.DocMap sortMap) throws IOException {
        final int[] ordMap = new int[fieldData.docsWithField.cardinality()]; // new ord to old ord

        DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
        mapOldOrdToNewOrd(fieldData.docsWithField, sortMap, null, ordMap, newDocsWithField);

        // write vector values
        long vectorDataOffset = switch (fieldData.fieldInfo.getVectorEncoding()) {
            case FLOAT32 -> writeSortedBFloat16Vectors(fieldData, ordMap);
            case BYTE -> throw new IllegalStateException(
                "Incorrect encoding for field " + fieldData.fieldInfo.name + ": " + VectorEncoding.BYTE
            );
        };
        long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

        writeMeta(fieldData.fieldInfo, maxDoc, vectorDataOffset, vectorDataLength, newDocsWithField);
    }

    private long writeSortedBFloat16Vectors(FieldWriter<?> fieldData, int[] ordMap) throws IOException {
        long vectorDataOffset = vectorData.alignFilePointer(BFloat16.BYTES);
        final ByteBuffer buffer = ByteBuffer.allocate(fieldData.dim * BFloat16.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int ordinal : ordMap) {
            float[] vector = (float[]) fieldData.vectors.get(ordinal);
            BFloat16.floatToBFloat16(vector, buffer.asShortBuffer());
            vectorData.writeBytes(buffer.array(), buffer.array().length);
        }
        return vectorDataOffset;
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        // Since we know we will not be searching for additional indexing, we can just write the
        // the vectors directly to the new segment.
        long vectorDataOffset = vectorData.alignFilePointer(BFloat16.BYTES);
        // No need to use temporary file as we don't have to re-open for reading
        DocsWithFieldSet docsWithField = switch (fieldInfo.getVectorEncoding()) {
            case FLOAT32 -> writeVectorData(vectorData, MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
            case BYTE -> throw new IllegalStateException("Incorrect encoding for field " + fieldInfo.name + ": " + VectorEncoding.BYTE);
        };
        long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
        writeMeta(fieldInfo, segmentWriteState.segmentInfo.maxDoc(), vectorDataOffset, vectorDataLength, docsWithField);
    }

    @Override
    public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        long vectorDataOffset = vectorData.alignFilePointer(BFloat16.BYTES);
        IndexOutput tempVectorData = segmentWriteState.directory.createTempOutput(vectorData.getName(), "temp", segmentWriteState.context);
        IndexInput vectorDataInput = null;
        boolean success = false;
        try {
            // write the vector data to a temporary file
            DocsWithFieldSet docsWithField = switch (fieldInfo.getVectorEncoding()) {
                case FLOAT32 -> writeVectorData(tempVectorData, MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
                case BYTE -> throw new UnsupportedOperationException("ES92BFloat16FlatVectorsWriter only supports float vectors");
            };
            CodecUtil.writeFooter(tempVectorData);
            IOUtils.close(tempVectorData);

            // This temp file will be accessed in a random-access fashion to construct the HNSW graph.
            // Note: don't use the context from the state, which is a flush/merge context, not expecting
            // to perform random reads.
            vectorDataInput = segmentWriteState.directory.openInput(
                tempVectorData.getName(),
                IOContext.DEFAULT.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM)
            );
            // copy the temporary file vectors to the actual data file
            vectorData.copyBytes(vectorDataInput, vectorDataInput.length() - CodecUtil.footerLength());
            CodecUtil.retrieveChecksum(vectorDataInput);
            long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
            writeMeta(fieldInfo, segmentWriteState.segmentInfo.maxDoc(), vectorDataOffset, vectorDataLength, docsWithField);
            success = true;
            final IndexInput finalVectorDataInput = vectorDataInput;
            final RandomVectorScorerSupplier randomVectorScorerSupplier = vectorsScorer.getRandomVectorScorerSupplier(
                fieldInfo.getVectorSimilarityFunction(),
                new OffHeapBFloat16VectorValues.DenseOffHeapVectorValues(
                    fieldInfo.getVectorDimension(),
                    docsWithField.cardinality(),
                    finalVectorDataInput,
                    fieldInfo.getVectorDimension() * BFloat16.BYTES,
                    vectorsScorer,
                    fieldInfo.getVectorSimilarityFunction()
                )
            );
            return new FlatCloseableRandomVectorScorerSupplier(() -> {
                IOUtils.close(finalVectorDataInput);
                segmentWriteState.directory.deleteFile(tempVectorData.getName());
            }, docsWithField.cardinality(), randomVectorScorerSupplier);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(vectorDataInput, tempVectorData);
                try {
                    segmentWriteState.directory.deleteFile(tempVectorData.getName());
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    private void writeMeta(FieldInfo field, int maxDoc, long vectorDataOffset, long vectorDataLength, DocsWithFieldSet docsWithField)
        throws IOException {
        meta.writeInt(field.number);
        meta.writeInt(field.getVectorEncoding().ordinal());
        meta.writeInt(field.getVectorSimilarityFunction().ordinal());
        meta.writeVLong(vectorDataOffset);
        meta.writeVLong(vectorDataLength);
        meta.writeVInt(field.getVectorDimension());

        // write docIDs
        int count = docsWithField.cardinality();
        meta.writeInt(count);
        OrdToDocDISIReaderConfiguration.writeStoredMeta(DIRECT_MONOTONIC_BLOCK_SHIFT, meta, vectorData, count, maxDoc, docsWithField);
    }

    /**
     * Writes the vector values to the output and returns a set of documents that contains vectors.
     */
    private static DocsWithFieldSet writeVectorData(IndexOutput output, FloatVectorValues floatVectorValues) throws IOException {
        DocsWithFieldSet docsWithField = new DocsWithFieldSet();
        ByteBuffer buffer = ByteBuffer.allocate(floatVectorValues.dimension() * BFloat16.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        KnnVectorValues.DocIndexIterator iter = floatVectorValues.iterator();
        for (int docV = iter.nextDoc(); docV != NO_MORE_DOCS; docV = iter.nextDoc()) {
            // write vector
            float[] value = floatVectorValues.vectorValue(iter.index());
            BFloat16.floatToBFloat16(value, buffer.asShortBuffer());
            output.writeBytes(buffer.array(), buffer.limit());
            docsWithField.add(docV);
        }
        return docsWithField;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(meta, vectorData);
    }

    private abstract static class FieldWriter<T> extends FlatFieldVectorsWriter<T> {
        private static final long SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FieldWriter.class);
        private final FieldInfo fieldInfo;
        private final int dim;
        private final DocsWithFieldSet docsWithField;
        private final List<T> vectors;
        private boolean finished;

        private int lastDocID = -1;

        static FieldWriter<?> create(FieldInfo fieldInfo) {
            int dim = fieldInfo.getVectorDimension();
            return switch (fieldInfo.getVectorEncoding()) {
                case FLOAT32 -> new ES93BFloat16FlatVectorsWriter.FieldWriter<float[]>(fieldInfo) {
                    @Override
                    public float[] copyValue(float[] value) {
                        return ArrayUtil.copyOfSubArray(value, 0, dim);
                    }
                };
                case BYTE -> throw new IllegalStateException("Incorrect encoding for field " + fieldInfo.name + ": " + VectorEncoding.BYTE);
            };
        }

        FieldWriter(FieldInfo fieldInfo) {
            super();
            this.fieldInfo = fieldInfo;
            this.dim = fieldInfo.getVectorDimension();
            this.docsWithField = new DocsWithFieldSet();
            vectors = new ArrayList<>();
        }

        @Override
        public void addValue(int docID, T vectorValue) throws IOException {
            if (finished) {
                throw new IllegalStateException("already finished, cannot add more values");
            }
            if (docID == lastDocID) {
                throw new IllegalArgumentException(
                    "VectorValuesField \""
                        + fieldInfo.name
                        + "\" appears more than once in this document (only one value is allowed per field)"
                );
            }
            assert docID > lastDocID;
            T copy = copyValue(vectorValue);
            docsWithField.add(docID);
            vectors.add(copy);
            lastDocID = docID;
        }

        @Override
        public long ramBytesUsed() {
            long size = SHALLOW_RAM_BYTES_USED;
            if (vectors.size() == 0) return size;

            int byteSize = fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32
                ? BFloat16.BYTES
                : fieldInfo.getVectorEncoding().byteSize;

            return size + docsWithField.ramBytesUsed() + (long) vectors.size() * (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER) + (long) vectors.size() * fieldInfo.getVectorDimension() * byteSize;
        }

        @Override
        public List<T> getVectors() {
            return vectors;
        }

        @Override
        public DocsWithFieldSet getDocsWithFieldSet() {
            return docsWithField;
        }

        @Override
        public void finish() throws IOException {
            if (finished) {
                return;
            }
            this.finished = true;
        }

        @Override
        public boolean isFinished() {
            return finished;
        }
    }

    static final class FlatCloseableRandomVectorScorerSupplier implements CloseableRandomVectorScorerSupplier {

        private final RandomVectorScorerSupplier supplier;
        private final Closeable onClose;
        private final int numVectors;

        FlatCloseableRandomVectorScorerSupplier(Closeable onClose, int numVectors, RandomVectorScorerSupplier supplier) {
            this.onClose = onClose;
            this.supplier = supplier;
            this.numVectors = numVectors;
        }

        @Override
        public UpdateableRandomVectorScorer scorer() throws IOException {
            return supplier.scorer();
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return supplier.copy();
        }

        @Override
        public void close() throws IOException {
            onClose.close();
        }

        @Override
        public int totalVectorCount() {
            return numVectors;
        }
    }
}
