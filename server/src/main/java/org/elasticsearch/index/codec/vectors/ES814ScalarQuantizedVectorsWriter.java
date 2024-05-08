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
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.FlatVectorsWriter;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.codecs.lucene99.OffHeapQuantizedByteVectorValues;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizedRandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.vec.VectorScorerFactory;
import org.elasticsearch.vec.VectorScorerSupplierAdapter;
import org.elasticsearch.vec.VectorSimilarityType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_COMPONENT;
import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat.calculateDefaultConfidenceInterval;
import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter.mergeAndRecalculateQuantiles;
import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter.writeQuantizedVectorData;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

/**
 * Writes quantized vector values and metadata to index segments.
 * Amended copy of Lucene99ScalarQuantizedVectorsWriter
 */
public final class ES814ScalarQuantizedVectorsWriter extends FlatVectorsWriter {

    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private static final long SHALLOW_RAM_BYTES_USED = shallowSizeOfInstance(ES814ScalarQuantizedVectorsWriter.class);

    // Used for determining if a new quantization state requires a re-quantization
    // for a given segment.
    // This ensures that in expectation 4/5 of the vector would be unchanged by requantization.
    // Furthermore, only those values where the value is within 1/5 of the centre of a quantization
    // bin will be changed. In these cases the error introduced by snapping one way or another
    // is small compared to the error introduced by quantization in the first place. Furthermore,
    // empirical testing showed that the relative error by not requantizing is small (compared to
    // the quantization error) and the condition is sensitive enough to detect all adversarial cases,
    // such as merging clustered data.
    private static final float REQUANTIZATION_LIMIT = 0.2f;
    private final SegmentWriteState segmentWriteState;

    private final List<FieldWriter> fields = new ArrayList<>();
    private final IndexOutput meta, quantizedVectorData;
    private final Float confidenceInterval;
    private final FlatVectorsWriter rawVectorDelegate;
    private boolean finished;

    ES814ScalarQuantizedVectorsWriter(SegmentWriteState state, Float confidenceInterval, FlatVectorsWriter rawVectorDelegate)
        throws IOException {
        this.confidenceInterval = confidenceInterval;
        segmentWriteState = state;
        String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES814ScalarQuantizedVectorsFormat.META_EXTENSION
        );

        String quantizedVectorDataFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES814ScalarQuantizedVectorsFormat.VECTOR_DATA_EXTENSION
        );
        this.rawVectorDelegate = rawVectorDelegate;
        boolean success = false;
        try {
            meta = state.directory.createOutput(metaFileName, state.context);
            quantizedVectorData = state.directory.createOutput(quantizedVectorDataFileName, state.context);

            CodecUtil.writeIndexHeader(
                meta,
                ES814ScalarQuantizedVectorsFormat.META_CODEC_NAME,
                ES814ScalarQuantizedVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.writeIndexHeader(
                quantizedVectorData,
                ES814ScalarQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
                ES814ScalarQuantizedVectorsFormat.VERSION_CURRENT,
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
    public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo, KnnFieldVectorsWriter<?> indexWriter) throws IOException {
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            float confidenceInterval = this.confidenceInterval == null
                ? calculateDefaultConfidenceInterval(fieldInfo.getVectorDimension())
                : this.confidenceInterval;
            FieldWriter quantizedWriter = new FieldWriter(confidenceInterval, fieldInfo, segmentWriteState.infoStream, indexWriter);
            fields.add(quantizedWriter);
            indexWriter = quantizedWriter;
        }
        return rawVectorDelegate.addField(fieldInfo, indexWriter);
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
        // Since we know we will not be searching for additional indexing, we can just write the
        // the vectors directly to the new segment.
        // No need to use temporary file as we don't have to re-open for reading
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            ScalarQuantizer mergedQuantizationState = mergeQuantiles(fieldInfo, mergeState);
            MergedQuantizedVectorValues byteVectorValues = MergedQuantizedVectorValues.mergeQuantizedByteVectorValues(
                fieldInfo,
                mergeState,
                mergedQuantizationState
            );
            long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
            DocsWithFieldSet docsWithField = writeQuantizedVectorData(quantizedVectorData, byteVectorValues);
            long vectorDataLength = quantizedVectorData.getFilePointer() - vectorDataOffset;
            float confidenceInterval = this.confidenceInterval == null
                ? calculateDefaultConfidenceInterval(fieldInfo.getVectorDimension())
                : this.confidenceInterval;
            writeMeta(
                fieldInfo,
                segmentWriteState.segmentInfo.maxDoc(),
                vectorDataOffset,
                vectorDataLength,
                confidenceInterval,
                mergedQuantizationState.getLowerQuantile(),
                mergedQuantizationState.getUpperQuantile(),
                docsWithField
            );
        }
    }

    @Override
    public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            // Simply merge the underlying delegate, which just copies the raw vector data to a new
            // segment file
            rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
            ScalarQuantizer mergedQuantizationState = mergeQuantiles(fieldInfo, mergeState);
            return mergeOneFieldToIndex(segmentWriteState, fieldInfo, mergeState, mergedQuantizationState);
        }
        // We only merge the delegate, since the field type isn't float32, quantization wasn't
        // supported, so bypass it.
        return rawVectorDelegate.mergeOneFieldToIndex(fieldInfo, mergeState);
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        rawVectorDelegate.flush(maxDoc, sortMap);
        for (FieldWriter field : fields) {
            field.finish();
            if (sortMap == null) {
                writeField(field, maxDoc);
            } else {
                writeSortingField(field, maxDoc, sortMap);
            }
        }
    }

    @Override
    public void finish() throws IOException {
        if (finished) {
            throw new IllegalStateException("already finished");
        }
        finished = true;
        rawVectorDelegate.finish();
        if (meta != null) {
            // write end of fields marker
            meta.writeInt(-1);
            CodecUtil.writeFooter(meta);
        }
        if (quantizedVectorData != null) {
            CodecUtil.writeFooter(quantizedVectorData);
        }
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_RAM_BYTES_USED;
        for (FieldWriter field : fields) {
            total += field.ramBytesUsed();
        }
        return total;
    }

    private void writeField(FieldWriter fieldData, int maxDoc) throws IOException {
        // write vector values
        long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
        writeQuantizedVectors(fieldData);
        long vectorDataLength = quantizedVectorData.getFilePointer() - vectorDataOffset;

        writeMeta(
            fieldData.fieldInfo,
            maxDoc,
            vectorDataOffset,
            vectorDataLength,
            confidenceInterval,
            fieldData.minQuantile,
            fieldData.maxQuantile,
            fieldData.docsWithField
        );
    }

    private void writeMeta(
        FieldInfo field,
        int maxDoc,
        long vectorDataOffset,
        long vectorDataLength,
        Float confidenceInterval,
        Float lowerQuantile,
        Float upperQuantile,
        DocsWithFieldSet docsWithField
    ) throws IOException {
        meta.writeInt(field.number);
        meta.writeInt(field.getVectorEncoding().ordinal());
        meta.writeInt(field.getVectorSimilarityFunction().ordinal());
        meta.writeVLong(vectorDataOffset);
        meta.writeVLong(vectorDataLength);
        meta.writeVInt(field.getVectorDimension());
        int count = docsWithField.cardinality();
        meta.writeInt(count);
        if (count > 0) {
            assert Float.isFinite(lowerQuantile) && Float.isFinite(upperQuantile);
            meta.writeInt(
                Float.floatToIntBits(
                    confidenceInterval != null ? confidenceInterval : calculateDefaultConfidenceInterval(field.getVectorDimension())
                )
            );
            meta.writeInt(Float.floatToIntBits(lowerQuantile));
            meta.writeInt(Float.floatToIntBits(upperQuantile));
        }
        // write docIDs
        OrdToDocDISIReaderConfiguration.writeStoredMeta(
            DIRECT_MONOTONIC_BLOCK_SHIFT,
            meta,
            quantizedVectorData,
            count,
            maxDoc,
            docsWithField
        );
    }

    private void writeQuantizedVectors(FieldWriter fieldData) throws IOException {
        ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
        byte[] vector = new byte[fieldData.fieldInfo.getVectorDimension()];
        final ByteBuffer offsetBuffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        float[] copy = fieldData.normalize ? new float[fieldData.fieldInfo.getVectorDimension()] : null;
        for (float[] v : fieldData.floatVectors) {
            if (fieldData.normalize) {
                System.arraycopy(v, 0, copy, 0, copy.length);
                VectorUtil.l2normalize(copy);
                v = copy;
            }

            float offsetCorrection = scalarQuantizer.quantize(v, vector, fieldData.fieldInfo.getVectorSimilarityFunction());
            quantizedVectorData.writeBytes(vector, vector.length);
            offsetBuffer.putFloat(offsetCorrection);
            quantizedVectorData.writeBytes(offsetBuffer.array(), offsetBuffer.array().length);
            offsetBuffer.rewind();
        }
    }

    private void writeSortingField(FieldWriter fieldData, int maxDoc, Sorter.DocMap sortMap) throws IOException {
        final int[] docIdOffsets = new int[sortMap.size()];
        int offset = 1; // 0 means no vector for this (field, document)
        DocIdSetIterator iterator = fieldData.docsWithField.iterator();
        for (int docID = iterator.nextDoc(); docID != NO_MORE_DOCS; docID = iterator.nextDoc()) {
            int newDocID = sortMap.oldToNew(docID);
            docIdOffsets[newDocID] = offset++;
        }
        DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
        final int[] ordMap = new int[offset - 1]; // new ord to old ord
        int ord = 0;
        int doc = 0;
        for (int docIdOffset : docIdOffsets) {
            if (docIdOffset != 0) {
                ordMap[ord] = docIdOffset - 1;
                newDocsWithField.add(doc);
                ord++;
            }
            doc++;
        }

        // write vector values
        long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
        writeSortedQuantizedVectors(fieldData, ordMap);
        long quantizedVectorLength = quantizedVectorData.getFilePointer() - vectorDataOffset;
        writeMeta(
            fieldData.fieldInfo,
            maxDoc,
            vectorDataOffset,
            quantizedVectorLength,
            confidenceInterval,
            fieldData.minQuantile,
            fieldData.maxQuantile,
            newDocsWithField
        );
    }

    private void writeSortedQuantizedVectors(FieldWriter fieldData, int[] ordMap) throws IOException {
        ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
        byte[] vector = new byte[fieldData.fieldInfo.getVectorDimension()];
        final ByteBuffer offsetBuffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        float[] copy = fieldData.normalize ? new float[fieldData.fieldInfo.getVectorDimension()] : null;
        for (int ordinal : ordMap) {
            float[] v = fieldData.floatVectors.get(ordinal);
            if (fieldData.normalize) {
                System.arraycopy(v, 0, copy, 0, copy.length);
                VectorUtil.l2normalize(copy);
                v = copy;
            }
            float offsetCorrection = scalarQuantizer.quantize(v, vector, fieldData.fieldInfo.getVectorSimilarityFunction());
            quantizedVectorData.writeBytes(vector, vector.length);
            offsetBuffer.putFloat(offsetCorrection);
            quantizedVectorData.writeBytes(offsetBuffer.array(), offsetBuffer.array().length);
            offsetBuffer.rewind();
        }
    }

    private ScalarQuantizer mergeQuantiles(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        assert fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32;
        float confidenceInterval = this.confidenceInterval == null
            ? calculateDefaultConfidenceInterval(fieldInfo.getVectorDimension())
            : this.confidenceInterval;
        return mergeAndRecalculateQuantiles(mergeState, fieldInfo, confidenceInterval);
    }

    private ScalarQuantizedCloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
        SegmentWriteState segmentWriteState,
        FieldInfo fieldInfo,
        MergeState mergeState,
        ScalarQuantizer mergedQuantizationState
    ) throws IOException {
        long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
        IndexOutput tempQuantizedVectorData = segmentWriteState.directory.createTempOutput(
            quantizedVectorData.getName(),
            "temp",
            segmentWriteState.context
        );
        IndexInput quantizationDataInput = null;
        boolean success = false;
        try {
            MergedQuantizedVectorValues byteVectorValues = MergedQuantizedVectorValues.mergeQuantizedByteVectorValues(
                fieldInfo,
                mergeState,
                mergedQuantizationState
            );
            DocsWithFieldSet docsWithField = writeQuantizedVectorData(tempQuantizedVectorData, byteVectorValues);
            CodecUtil.writeFooter(tempQuantizedVectorData);
            IOUtils.close(tempQuantizedVectorData);
            quantizationDataInput = segmentWriteState.directory.openInput(tempQuantizedVectorData.getName(), segmentWriteState.context);
            quantizedVectorData.copyBytes(quantizationDataInput, quantizationDataInput.length() - CodecUtil.footerLength());
            long vectorDataLength = quantizedVectorData.getFilePointer() - vectorDataOffset;
            CodecUtil.retrieveChecksum(quantizationDataInput);
            float confidenceInterval = this.confidenceInterval == null
                ? calculateDefaultConfidenceInterval(fieldInfo.getVectorDimension())
                : this.confidenceInterval;
            writeMeta(
                fieldInfo,
                segmentWriteState.segmentInfo.maxDoc(),
                vectorDataOffset,
                vectorDataLength,
                confidenceInterval,
                mergedQuantizationState.getLowerQuantile(),
                mergedQuantizationState.getUpperQuantile(),
                docsWithField
            );
            success = true;
            final IndexInput finalQuantizationDataInput = quantizationDataInput;

            // retrieve a scorer
            RandomVectorScorerSupplier scorerSupplier = null;
            Optional<VectorScorerFactory> factory = VectorScorerFactory.instance();
            if (factory.isPresent()) {
                var scorer = factory.get()
                    .getInt7ScalarQuantizedVectorScorer(
                        byteVectorValues.dimension(),
                        docsWithField.cardinality(),
                        mergedQuantizationState.getConstantMultiplier(),
                        VectorSimilarityType.of(fieldInfo.getVectorSimilarityFunction()),
                        quantizationDataInput
                    )
                    .map(VectorScorerSupplierAdapter::new);
                if (scorer.isPresent()) {
                    scorerSupplier = scorer.get();
                }
            }
            if (scorerSupplier == null) {
                scorerSupplier = new ScalarQuantizedRandomVectorScorerSupplier(
                    fieldInfo.getVectorSimilarityFunction(),
                    mergedQuantizationState,
                    new OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues(
                        fieldInfo.getVectorDimension(),
                        docsWithField.cardinality(),
                        quantizationDataInput
                    )
                );
            }

            return new ScalarQuantizedCloseableRandomVectorScorerSupplier(() -> {
                IOUtils.close(finalQuantizationDataInput);
                segmentWriteState.directory.deleteFile(tempQuantizedVectorData.getName());
            }, docsWithField.cardinality(), scorerSupplier);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(tempQuantizedVectorData, quantizationDataInput);
                deleteFilesIgnoringExceptions(segmentWriteState.directory, tempQuantizedVectorData.getName());
            }
        }
    }

    @SuppressForbidden(reason = "closing using Lucene's variant")
    private static void deleteFilesIgnoringExceptions(Directory dir, String... files) {
        org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(dir, files);
    }

    private static QuantizedVectorsReader getQuantizedKnnVectorsReader(KnnVectorsReader vectorsReader, String fieldName) {
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader) {
            vectorsReader = ((PerFieldKnnVectorsFormat.FieldsReader) vectorsReader).getFieldReader(fieldName);
        }
        if (vectorsReader instanceof QuantizedVectorsReader) {
            return (QuantizedVectorsReader) vectorsReader;
        }
        return null;
    }

    /**
     * Returns true if the quantiles of the new quantization state are too far from the quantiles of
     * the existing quantization state. This would imply that floating point values would slightly
     * shift quantization buckets.
     *
     * @param existingQuantiles The existing quantiles for a segment
     * @param newQuantiles The new quantiles for a segment, could be merged, or fully re-calculated
     * @return true if the floating point values should be requantized
     */
    static boolean shouldRequantize(ScalarQuantizer existingQuantiles, ScalarQuantizer newQuantiles) {
        float tol = REQUANTIZATION_LIMIT * (newQuantiles.getUpperQuantile() - newQuantiles.getLowerQuantile()) / 128f;
        if (Math.abs(existingQuantiles.getUpperQuantile() - newQuantiles.getUpperQuantile()) > tol) {
            return true;
        }
        return Math.abs(existingQuantiles.getLowerQuantile() - newQuantiles.getLowerQuantile()) > tol;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(meta, quantizedVectorData, rawVectorDelegate);
    }

    static class FieldWriter extends FlatFieldVectorsWriter<float[]> {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance(FieldWriter.class);
        private final List<float[]> floatVectors;
        private final FieldInfo fieldInfo;
        private final float confidenceInterval;
        private final InfoStream infoStream;
        private final boolean normalize;
        private float minQuantile = Float.POSITIVE_INFINITY;
        private float maxQuantile = Float.NEGATIVE_INFINITY;
        private boolean finished;
        private final DocsWithFieldSet docsWithField;

        @SuppressWarnings("unchecked")
        FieldWriter(float confidenceInterval, FieldInfo fieldInfo, InfoStream infoStream, KnnFieldVectorsWriter<?> indexWriter) {
            super((KnnFieldVectorsWriter<float[]>) indexWriter);
            this.confidenceInterval = confidenceInterval;
            this.fieldInfo = fieldInfo;
            this.normalize = fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE;
            this.floatVectors = new ArrayList<>();
            this.infoStream = infoStream;
            this.docsWithField = new DocsWithFieldSet();
        }

        void finish() throws IOException {
            if (finished) {
                return;
            }
            if (floatVectors.size() == 0) {
                finished = true;
                return;
            }
            ScalarQuantizer quantizer = ScalarQuantizer.fromVectors(
                new FloatVectorWrapper(floatVectors, fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE),
                confidenceInterval,
                floatVectors.size()
            );
            minQuantile = quantizer.getLowerQuantile();
            maxQuantile = quantizer.getUpperQuantile();
            if (infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
                infoStream.message(
                    QUANTIZED_VECTOR_COMPONENT,
                    "quantized field="
                        + " confidenceInterval="
                        + confidenceInterval
                        + " minQuantile="
                        + minQuantile
                        + " maxQuantile="
                        + maxQuantile
                );
            }
            finished = true;
        }

        ScalarQuantizer createQuantizer() {
            assert finished;
            return new ScalarQuantizer(minQuantile, maxQuantile, confidenceInterval);
        }

        @Override
        public long ramBytesUsed() {
            long size = SHALLOW_SIZE;
            if (indexingDelegate != null) {
                size += indexingDelegate.ramBytesUsed();
            }
            if (floatVectors.size() == 0) return size;
            return size + (long) floatVectors.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        }

        @Override
        public void addValue(int docID, float[] vectorValue) throws IOException {
            docsWithField.add(docID);
            floatVectors.add(vectorValue);
            if (indexingDelegate != null) {
                indexingDelegate.addValue(docID, vectorValue);
            }
        }

        @Override
        public float[] copyValue(float[] vectorValue) {
            throw new UnsupportedOperationException();
        }
    }

    static class FloatVectorWrapper extends FloatVectorValues {
        private final List<float[]> vectorList;
        private final float[] copy;
        private final boolean normalize;
        protected int curDoc = -1;

        FloatVectorWrapper(List<float[]> vectorList, boolean normalize) {
            this.vectorList = vectorList;
            this.copy = new float[vectorList.get(0).length];
            this.normalize = normalize;
        }

        @Override
        public int dimension() {
            return vectorList.get(0).length;
        }

        @Override
        public int size() {
            return vectorList.size();
        }

        @Override
        public float[] vectorValue() throws IOException {
            if (curDoc == -1 || curDoc >= vectorList.size()) {
                throw new IOException("Current doc not set or too many iterations");
            }
            if (normalize) {
                System.arraycopy(vectorList.get(curDoc), 0, copy, 0, copy.length);
                VectorUtil.l2normalize(copy);
                return copy;
            }
            return vectorList.get(curDoc);
        }

        @Override
        public int docID() {
            if (curDoc >= vectorList.size()) {
                return NO_MORE_DOCS;
            }
            return curDoc;
        }

        @Override
        public int nextDoc() throws IOException {
            curDoc++;
            return docID();
        }

        @Override
        public int advance(int target) throws IOException {
            curDoc = target;
            return docID();
        }
    }

    private static class QuantizedByteVectorValueSub extends DocIDMerger.Sub {
        private final QuantizedByteVectorValues values;

        QuantizedByteVectorValueSub(MergeState.DocMap docMap, QuantizedByteVectorValues values) {
            super(docMap);
            this.values = values;
            assert values.docID() == -1;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
    }

    /** Returns a merged view over all the segment's {@link QuantizedByteVectorValues}. */
    static class MergedQuantizedVectorValues extends QuantizedByteVectorValues {
        public static MergedQuantizedVectorValues mergeQuantizedByteVectorValues(
            FieldInfo fieldInfo,
            MergeState mergeState,
            ScalarQuantizer scalarQuantizer
        ) throws IOException {
            assert fieldInfo != null && fieldInfo.hasVectorValues();

            List<QuantizedByteVectorValueSub> subs = new ArrayList<>();
            for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
                if (mergeState.knnVectorsReaders[i] != null
                    && mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name) != null) {
                    QuantizedVectorsReader reader = getQuantizedKnnVectorsReader(mergeState.knnVectorsReaders[i], fieldInfo.name);
                    assert scalarQuantizer != null;
                    final QuantizedByteVectorValueSub sub;
                    // Either our quantization parameters are way different than the merged ones
                    // Or we have never been quantized.
                    if (reader == null
                        || reader.getQuantizationState(fieldInfo.name) == null
                        || shouldRequantize(reader.getQuantizationState(fieldInfo.name), scalarQuantizer)) {
                        sub = new QuantizedByteVectorValueSub(
                            mergeState.docMaps[i],
                            new QuantizedFloatVectorValues(
                                mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name),
                                fieldInfo.getVectorSimilarityFunction(),
                                scalarQuantizer
                            )
                        );
                    } else {
                        sub = new QuantizedByteVectorValueSub(
                            mergeState.docMaps[i],
                            new OffsetCorrectedQuantizedByteVectorValues(
                                reader.getQuantizedVectorValues(fieldInfo.name),
                                fieldInfo.getVectorSimilarityFunction(),
                                scalarQuantizer,
                                reader.getQuantizationState(fieldInfo.name)
                            )
                        );
                    }
                    subs.add(sub);
                }
            }
            return new MergedQuantizedVectorValues(subs, mergeState);
        }

        private final List<QuantizedByteVectorValueSub> subs;
        private final DocIDMerger<QuantizedByteVectorValueSub> docIdMerger;
        private final int size;

        private int docId;
        private QuantizedByteVectorValueSub current;

        private MergedQuantizedVectorValues(List<QuantizedByteVectorValueSub> subs, MergeState mergeState) throws IOException {
            this.subs = subs;
            docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
            int totalSize = 0;
            for (QuantizedByteVectorValueSub sub : subs) {
                totalSize += sub.values.size();
            }
            size = totalSize;
            docId = -1;
        }

        @Override
        public byte[] vectorValue() throws IOException {
            return current.values.vectorValue();
        }

        @Override
        public int docID() {
            return docId;
        }

        @Override
        public int nextDoc() throws IOException {
            current = docIdMerger.next();
            if (current == null) {
                docId = NO_MORE_DOCS;
            } else {
                docId = current.mappedDocID;
            }
            return docId;
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int dimension() {
            return subs.get(0).values.dimension();
        }

        @Override
        public float getScoreCorrectionConstant() throws IOException {
            return current.values.getScoreCorrectionConstant();
        }
    }

    private static class QuantizedFloatVectorValues extends QuantizedByteVectorValues {
        private final FloatVectorValues values;
        private final ScalarQuantizer quantizer;
        private final byte[] quantizedVector;
        private final float[] normalizedVector;
        private float offsetValue = 0f;

        private final VectorSimilarityFunction vectorSimilarityFunction;

        QuantizedFloatVectorValues(FloatVectorValues values, VectorSimilarityFunction vectorSimilarityFunction, ScalarQuantizer quantizer) {
            this.values = values;
            this.quantizer = quantizer;
            this.quantizedVector = new byte[values.dimension()];
            this.vectorSimilarityFunction = vectorSimilarityFunction;
            if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
                this.normalizedVector = new float[values.dimension()];
            } else {
                this.normalizedVector = null;
            }
        }

        @Override
        public float getScoreCorrectionConstant() {
            return offsetValue;
        }

        @Override
        public int dimension() {
            return values.dimension();
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public byte[] vectorValue() throws IOException {
            return quantizedVector;
        }

        @Override
        public int docID() {
            return values.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            int doc = values.nextDoc();
            if (doc != NO_MORE_DOCS) {
                quantize();
            }
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc = values.advance(target);
            if (doc != NO_MORE_DOCS) {
                quantize();
            }
            return doc;
        }

        private void quantize() throws IOException {
            if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
                System.arraycopy(values.vectorValue(), 0, normalizedVector, 0, normalizedVector.length);
                VectorUtil.l2normalize(normalizedVector);
                offsetValue = quantizer.quantize(normalizedVector, quantizedVector, vectorSimilarityFunction);
            } else {
                offsetValue = quantizer.quantize(values.vectorValue(), quantizedVector, vectorSimilarityFunction);
            }
        }
    }

    static final class ScalarQuantizedCloseableRandomVectorScorerSupplier implements CloseableRandomVectorScorerSupplier {

        private final RandomVectorScorerSupplier supplier;
        private final Closeable onClose;
        private final int numVectors;

        ScalarQuantizedCloseableRandomVectorScorerSupplier(Closeable onClose, int numVectors, RandomVectorScorerSupplier supplier) {
            this.onClose = onClose;
            this.supplier = supplier;
            this.numVectors = numVectors;
        }

        @Override
        public RandomVectorScorer scorer(int ord) throws IOException {
            return supplier.scorer(ord);
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

    private static final class OffsetCorrectedQuantizedByteVectorValues extends QuantizedByteVectorValues {

        private final QuantizedByteVectorValues in;
        private final VectorSimilarityFunction vectorSimilarityFunction;
        private final ScalarQuantizer scalarQuantizer, oldScalarQuantizer;

        private OffsetCorrectedQuantizedByteVectorValues(
            QuantizedByteVectorValues in,
            VectorSimilarityFunction vectorSimilarityFunction,
            ScalarQuantizer scalarQuantizer,
            ScalarQuantizer oldScalarQuantizer
        ) {
            this.in = in;
            this.vectorSimilarityFunction = vectorSimilarityFunction;
            this.scalarQuantizer = scalarQuantizer;
            this.oldScalarQuantizer = oldScalarQuantizer;
        }

        @Override
        public float getScoreCorrectionConstant() throws IOException {
            return scalarQuantizer.recalculateCorrectiveOffset(in.vectorValue(), oldScalarQuantizer, vectorSimilarityFunction);
        }

        @Override
        public int dimension() {
            return in.dimension();
        }

        @Override
        public int size() {
            return in.size();
        }

        @Override
        public byte[] vectorValue() throws IOException {
            return in.vectorValue();
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
    }
}
