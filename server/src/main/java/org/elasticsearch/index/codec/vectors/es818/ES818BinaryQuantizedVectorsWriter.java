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
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.FloatArrayList;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.codec.vectors.BQSpaceUtils;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;
import static org.elasticsearch.index.codec.vectors.es818.ES818BinaryQuantizedVectorsFormat.BINARIZED_VECTOR_COMPONENT;
import static org.elasticsearch.index.codec.vectors.es818.ES818BinaryQuantizedVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
 */
@SuppressForbidden(reason = "Lucene classes")
public class ES818BinaryQuantizedVectorsWriter extends FlatVectorsWriter {
    private static final long SHALLOW_RAM_BYTES_USED = shallowSizeOfInstance(ES818BinaryQuantizedVectorsWriter.class);

    private final SegmentWriteState segmentWriteState;
    private final List<FieldWriter> fields = new ArrayList<>();
    private final IndexOutput meta, binarizedVectorData;
    private final FlatVectorsWriter rawVectorDelegate;
    private final ES818BinaryFlatVectorsScorer vectorsScorer;
    private boolean finished;

    /**
     * Sole constructor
     *
     * @param vectorsScorer the scorer to use for scoring vectors
     */
    protected ES818BinaryQuantizedVectorsWriter(
        ES818BinaryFlatVectorsScorer vectorsScorer,
        FlatVectorsWriter rawVectorDelegate,
        SegmentWriteState state
    ) throws IOException {
        super(vectorsScorer);
        this.vectorsScorer = vectorsScorer;
        this.segmentWriteState = state;
        String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES818BinaryQuantizedVectorsFormat.META_EXTENSION
        );

        String binarizedVectorDataFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES818BinaryQuantizedVectorsFormat.VECTOR_DATA_EXTENSION
        );
        this.rawVectorDelegate = rawVectorDelegate;
        boolean success = false;
        try {
            meta = state.directory.createOutput(metaFileName, state.context);
            binarizedVectorData = state.directory.createOutput(binarizedVectorDataFileName, state.context);

            CodecUtil.writeIndexHeader(
                meta,
                ES818BinaryQuantizedVectorsFormat.META_CODEC_NAME,
                ES818BinaryQuantizedVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.writeIndexHeader(
                binarizedVectorData,
                ES818BinaryQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
                ES818BinaryQuantizedVectorsFormat.VERSION_CURRENT,
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
        FlatFieldVectorsWriter<?> rawVectorDelegate = this.rawVectorDelegate.addField(fieldInfo);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            @SuppressWarnings("unchecked")
            FieldWriter fieldWriter = new FieldWriter(fieldInfo, (FlatFieldVectorsWriter<float[]>) rawVectorDelegate);
            fields.add(fieldWriter);
            return fieldWriter;
        }
        return rawVectorDelegate;
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        rawVectorDelegate.flush(maxDoc, sortMap);
        for (FieldWriter field : fields) {
            // after raw vectors are written, normalize vectors for clustering and quantization
            if (VectorSimilarityFunction.COSINE == field.fieldInfo.getVectorSimilarityFunction()) {
                field.normalizeVectors();
            }
            final float[] clusterCenter;
            int vectorCount = field.flatFieldVectorsWriter.getVectors().size();
            clusterCenter = new float[field.dimensionSums.length];
            if (vectorCount > 0) {
                for (int i = 0; i < field.dimensionSums.length; i++) {
                    clusterCenter[i] = field.dimensionSums[i] / vectorCount;
                }
                if (VectorSimilarityFunction.COSINE == field.fieldInfo.getVectorSimilarityFunction()) {
                    VectorUtil.l2normalize(clusterCenter);
                }
            }
            if (segmentWriteState.infoStream.isEnabled(BINARIZED_VECTOR_COMPONENT)) {
                segmentWriteState.infoStream.message(BINARIZED_VECTOR_COMPONENT, "Vectors' count:" + vectorCount);
            }
            OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(field.fieldInfo.getVectorSimilarityFunction());
            if (sortMap == null) {
                writeField(field, clusterCenter, maxDoc, quantizer);
            } else {
                writeSortingField(field, clusterCenter, maxDoc, sortMap, quantizer);
            }
            field.finish();
        }
    }

    private void writeField(FieldWriter fieldData, float[] clusterCenter, int maxDoc, OptimizedScalarQuantizer quantizer)
        throws IOException {
        // write vector values
        long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
        writeBinarizedVectors(fieldData, clusterCenter, quantizer);
        long vectorDataLength = binarizedVectorData.getFilePointer() - vectorDataOffset;
        float centroidDp = fieldData.getVectors().size() > 0 ? VectorUtil.dotProduct(clusterCenter, clusterCenter) : 0;

        writeMeta(
            fieldData.fieldInfo,
            maxDoc,
            vectorDataOffset,
            vectorDataLength,
            clusterCenter,
            centroidDp,
            fieldData.getDocsWithFieldSet()
        );
    }

    private void writeBinarizedVectors(FieldWriter fieldData, float[] clusterCenter, OptimizedScalarQuantizer scalarQuantizer)
        throws IOException {
        int discreteDims = BQVectorUtils.discretize(fieldData.fieldInfo.getVectorDimension(), 64);
        byte[] quantizationScratch = new byte[discreteDims];
        byte[] vector = new byte[discreteDims / 8];
        for (int i = 0; i < fieldData.getVectors().size(); i++) {
            float[] v = fieldData.getVectors().get(i);
            OptimizedScalarQuantizer.QuantizationResult corrections = scalarQuantizer.scalarQuantize(
                v,
                quantizationScratch,
                (byte) 1,
                clusterCenter
            );
            BQVectorUtils.packAsBinary(quantizationScratch, vector);
            binarizedVectorData.writeBytes(vector, vector.length);
            binarizedVectorData.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
            binarizedVectorData.writeInt(Float.floatToIntBits(corrections.upperInterval()));
            binarizedVectorData.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
            assert corrections.quantizedComponentSum() >= 0 && corrections.quantizedComponentSum() <= 0xffff;
            binarizedVectorData.writeShort((short) corrections.quantizedComponentSum());
        }
    }

    private void writeSortingField(
        FieldWriter fieldData,
        float[] clusterCenter,
        int maxDoc,
        Sorter.DocMap sortMap,
        OptimizedScalarQuantizer scalarQuantizer
    ) throws IOException {
        final int[] ordMap = new int[fieldData.getDocsWithFieldSet().cardinality()]; // new ord to old ord

        DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
        mapOldOrdToNewOrd(fieldData.getDocsWithFieldSet(), sortMap, null, ordMap, newDocsWithField);

        // write vector values
        long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
        writeSortedBinarizedVectors(fieldData, clusterCenter, ordMap, scalarQuantizer);
        long quantizedVectorLength = binarizedVectorData.getFilePointer() - vectorDataOffset;

        float centroidDp = VectorUtil.dotProduct(clusterCenter, clusterCenter);
        writeMeta(fieldData.fieldInfo, maxDoc, vectorDataOffset, quantizedVectorLength, clusterCenter, centroidDp, newDocsWithField);
    }

    private void writeSortedBinarizedVectors(
        FieldWriter fieldData,
        float[] clusterCenter,
        int[] ordMap,
        OptimizedScalarQuantizer scalarQuantizer
    ) throws IOException {
        int discreteDims = BQVectorUtils.discretize(fieldData.fieldInfo.getVectorDimension(), 64);
        byte[] quantizationScratch = new byte[discreteDims];
        byte[] vector = new byte[discreteDims / 8];
        for (int ordinal : ordMap) {
            float[] v = fieldData.getVectors().get(ordinal);
            OptimizedScalarQuantizer.QuantizationResult corrections = scalarQuantizer.scalarQuantize(
                v,
                quantizationScratch,
                (byte) 1,
                clusterCenter
            );
            BQVectorUtils.packAsBinary(quantizationScratch, vector);
            binarizedVectorData.writeBytes(vector, vector.length);
            binarizedVectorData.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
            binarizedVectorData.writeInt(Float.floatToIntBits(corrections.upperInterval()));
            binarizedVectorData.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
            assert corrections.quantizedComponentSum() >= 0 && corrections.quantizedComponentSum() <= 0xffff;
            binarizedVectorData.writeShort((short) corrections.quantizedComponentSum());
        }
    }

    private void writeMeta(
        FieldInfo field,
        int maxDoc,
        long vectorDataOffset,
        long vectorDataLength,
        float[] clusterCenter,
        float centroidDp,
        DocsWithFieldSet docsWithField
    ) throws IOException {
        meta.writeInt(field.number);
        meta.writeInt(field.getVectorEncoding().ordinal());
        meta.writeInt(field.getVectorSimilarityFunction().ordinal());
        meta.writeVInt(field.getVectorDimension());
        meta.writeVLong(vectorDataOffset);
        meta.writeVLong(vectorDataLength);
        int count = docsWithField.cardinality();
        meta.writeVInt(count);
        if (count > 0) {
            final ByteBuffer buffer = ByteBuffer.allocate(field.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buffer.asFloatBuffer().put(clusterCenter);
            meta.writeBytes(buffer.array(), buffer.array().length);
            meta.writeInt(Float.floatToIntBits(centroidDp));
        }
        OrdToDocDISIReaderConfiguration.writeStoredMeta(
            DIRECT_MONOTONIC_BLOCK_SHIFT,
            meta,
            binarizedVectorData,
            count,
            maxDoc,
            docsWithField
        );
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
        if (binarizedVectorData != null) {
            CodecUtil.writeFooter(binarizedVectorData);
        }
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            final float[] centroid;
            final float[] mergedCentroid = new float[fieldInfo.getVectorDimension()];
            int vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);
            // Don't need access to the random vectors, we can just use the merged
            rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
            centroid = mergedCentroid;
            if (segmentWriteState.infoStream.isEnabled(BINARIZED_VECTOR_COMPONENT)) {
                segmentWriteState.infoStream.message(BINARIZED_VECTOR_COMPONENT, "Vectors' count:" + vectorCount);
            }
            FloatVectorValues floatVectorValues = KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
            if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
                floatVectorValues = new NormalizedFloatVectorValues(floatVectorValues);
            }
            BinarizedFloatVectorValues binarizedVectorValues = new BinarizedFloatVectorValues(
                floatVectorValues,
                new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction()),
                centroid
            );
            long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
            DocsWithFieldSet docsWithField = writeBinarizedVectorData(binarizedVectorData, binarizedVectorValues);
            long vectorDataLength = binarizedVectorData.getFilePointer() - vectorDataOffset;
            float centroidDp = docsWithField.cardinality() > 0 ? VectorUtil.dotProduct(centroid, centroid) : 0;
            writeMeta(
                fieldInfo,
                segmentWriteState.segmentInfo.maxDoc(),
                vectorDataOffset,
                vectorDataLength,
                centroid,
                centroidDp,
                docsWithField
            );
        } else {
            rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
        }
    }

    static DocsWithFieldSet writeBinarizedVectorAndQueryData(
        IndexOutput binarizedVectorData,
        IndexOutput binarizedQueryData,
        FloatVectorValues floatVectorValues,
        float[] centroid,
        OptimizedScalarQuantizer binaryQuantizer
    ) throws IOException {
        int discretizedDimension = BQVectorUtils.discretize(floatVectorValues.dimension(), 64);
        DocsWithFieldSet docsWithField = new DocsWithFieldSet();
        byte[][] quantizationScratch = new byte[2][floatVectorValues.dimension()];
        byte[] toIndex = new byte[discretizedDimension / 8];
        byte[] toQuery = new byte[(discretizedDimension / 8) * BQSpaceUtils.B_QUERY];
        KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
        for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
            // write index vector
            OptimizedScalarQuantizer.QuantizationResult[] r = binaryQuantizer.multiScalarQuantize(
                floatVectorValues.vectorValue(iterator.index()),
                quantizationScratch,
                new byte[] { 1, 4 },
                centroid
            );
            // pack and store document bit vector
            BQVectorUtils.packAsBinary(quantizationScratch[0], toIndex);
            binarizedVectorData.writeBytes(toIndex, toIndex.length);
            binarizedVectorData.writeInt(Float.floatToIntBits(r[0].lowerInterval()));
            binarizedVectorData.writeInt(Float.floatToIntBits(r[0].upperInterval()));
            binarizedVectorData.writeInt(Float.floatToIntBits(r[0].additionalCorrection()));
            assert r[0].quantizedComponentSum() >= 0 && r[0].quantizedComponentSum() <= 0xffff;
            binarizedVectorData.writeShort((short) r[0].quantizedComponentSum());
            docsWithField.add(docV);

            // pack and store the 4bit query vector
            BQSpaceUtils.transposeHalfByte(quantizationScratch[1], toQuery);
            binarizedQueryData.writeBytes(toQuery, toQuery.length);
            binarizedQueryData.writeInt(Float.floatToIntBits(r[1].lowerInterval()));
            binarizedQueryData.writeInt(Float.floatToIntBits(r[1].upperInterval()));
            binarizedQueryData.writeInt(Float.floatToIntBits(r[1].additionalCorrection()));
            assert r[1].quantizedComponentSum() >= 0 && r[1].quantizedComponentSum() <= 0xffff;
            binarizedQueryData.writeShort((short) r[1].quantizedComponentSum());
        }
        return docsWithField;
    }

    static DocsWithFieldSet writeBinarizedVectorData(IndexOutput output, BinarizedByteVectorValues binarizedByteVectorValues)
        throws IOException {
        DocsWithFieldSet docsWithField = new DocsWithFieldSet();
        KnnVectorValues.DocIndexIterator iterator = binarizedByteVectorValues.iterator();
        for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
            // write vector
            byte[] binaryValue = binarizedByteVectorValues.vectorValue(iterator.index());
            output.writeBytes(binaryValue, binaryValue.length);
            OptimizedScalarQuantizer.QuantizationResult corrections = binarizedByteVectorValues.getCorrectiveTerms(iterator.index());
            output.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
            output.writeInt(Float.floatToIntBits(corrections.upperInterval()));
            output.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
            assert corrections.quantizedComponentSum() >= 0 && corrections.quantizedComponentSum() <= 0xffff;
            output.writeShort((short) corrections.quantizedComponentSum());
            docsWithField.add(docV);
        }
        return docsWithField;
    }

    @Override
    public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            final float[] centroid;
            final float cDotC;
            final float[] mergedCentroid = new float[fieldInfo.getVectorDimension()];
            int vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);

            // Don't need access to the random vectors, we can just use the merged
            rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
            centroid = mergedCentroid;
            cDotC = vectorCount > 0 ? VectorUtil.dotProduct(centroid, centroid) : 0;
            if (segmentWriteState.infoStream.isEnabled(BINARIZED_VECTOR_COMPONENT)) {
                segmentWriteState.infoStream.message(BINARIZED_VECTOR_COMPONENT, "Vectors' count:" + vectorCount);
            }
            return mergeOneFieldToIndex(segmentWriteState, fieldInfo, mergeState, centroid, cDotC);
        }
        return rawVectorDelegate.mergeOneFieldToIndex(fieldInfo, mergeState);
    }

    private CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
        SegmentWriteState segmentWriteState,
        FieldInfo fieldInfo,
        MergeState mergeState,
        float[] centroid,
        float cDotC
    ) throws IOException {
        long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
        final IndexOutput tempQuantizedVectorData = segmentWriteState.directory.createTempOutput(
            binarizedVectorData.getName(),
            "temp",
            segmentWriteState.context
        );
        final IndexOutput tempScoreQuantizedVectorData = segmentWriteState.directory.createTempOutput(
            binarizedVectorData.getName(),
            "score_temp",
            segmentWriteState.context
        );
        IndexInput binarizedDataInput = null;
        IndexInput binarizedScoreDataInput = null;
        boolean success = false;
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        try {
            FloatVectorValues floatVectorValues = KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
            if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
                floatVectorValues = new NormalizedFloatVectorValues(floatVectorValues);
            }
            DocsWithFieldSet docsWithField = writeBinarizedVectorAndQueryData(
                tempQuantizedVectorData,
                tempScoreQuantizedVectorData,
                floatVectorValues,
                centroid,
                quantizer
            );
            CodecUtil.writeFooter(tempQuantizedVectorData);
            IOUtils.close(tempQuantizedVectorData);
            binarizedDataInput = segmentWriteState.directory.openInput(tempQuantizedVectorData.getName(), segmentWriteState.context);
            binarizedVectorData.copyBytes(binarizedDataInput, binarizedDataInput.length() - CodecUtil.footerLength());
            long vectorDataLength = binarizedVectorData.getFilePointer() - vectorDataOffset;
            CodecUtil.retrieveChecksum(binarizedDataInput);
            CodecUtil.writeFooter(tempScoreQuantizedVectorData);
            IOUtils.close(tempScoreQuantizedVectorData);
            binarizedScoreDataInput = segmentWriteState.directory.openInput(
                tempScoreQuantizedVectorData.getName(),
                segmentWriteState.context
            );
            writeMeta(
                fieldInfo,
                segmentWriteState.segmentInfo.maxDoc(),
                vectorDataOffset,
                vectorDataLength,
                centroid,
                cDotC,
                docsWithField
            );
            success = true;
            final IndexInput finalBinarizedDataInput = binarizedDataInput;
            final IndexInput finalBinarizedScoreDataInput = binarizedScoreDataInput;
            OffHeapBinarizedVectorValues vectorValues = new OffHeapBinarizedVectorValues.DenseOffHeapVectorValues(
                fieldInfo.getVectorDimension(),
                docsWithField.cardinality(),
                centroid,
                cDotC,
                quantizer,
                fieldInfo.getVectorSimilarityFunction(),
                vectorsScorer,
                finalBinarizedDataInput
            );
            RandomVectorScorerSupplier scorerSupplier = vectorsScorer.getRandomVectorScorerSupplier(
                fieldInfo.getVectorSimilarityFunction(),
                new OffHeapBinarizedQueryVectorValues(
                    finalBinarizedScoreDataInput,
                    fieldInfo.getVectorDimension(),
                    docsWithField.cardinality()
                ),
                vectorValues
            );
            return new BinarizedCloseableRandomVectorScorerSupplier(scorerSupplier, vectorValues, () -> {
                IOUtils.close(finalBinarizedDataInput, finalBinarizedScoreDataInput);
                IOUtils.deleteFilesIgnoringExceptions(
                    segmentWriteState.directory,
                    tempQuantizedVectorData.getName(),
                    tempScoreQuantizedVectorData.getName()
                );
            });
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(
                    tempQuantizedVectorData,
                    tempScoreQuantizedVectorData,
                    binarizedDataInput,
                    binarizedScoreDataInput
                );
                IOUtils.deleteFilesIgnoringExceptions(
                    segmentWriteState.directory,
                    tempQuantizedVectorData.getName(),
                    tempScoreQuantizedVectorData.getName()
                );
            }
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(meta, binarizedVectorData, rawVectorDelegate);
    }

    static float[] getCentroid(KnnVectorsReader vectorsReader, String fieldName) {
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
            vectorsReader = candidateReader.getFieldReader(fieldName);
        }
        if (vectorsReader instanceof ES818BinaryQuantizedVectorsReader reader) {
            return reader.getCentroid(fieldName);
        }
        return null;
    }

    static int mergeAndRecalculateCentroids(MergeState mergeState, FieldInfo fieldInfo, float[] mergedCentroid) throws IOException {
        boolean recalculate = false;
        int totalVectorCount = 0;
        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
            KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
            if (knnVectorsReader == null || knnVectorsReader.getFloatVectorValues(fieldInfo.name) == null) {
                continue;
            }
            float[] centroid = getCentroid(knnVectorsReader, fieldInfo.name);
            int vectorCount = knnVectorsReader.getFloatVectorValues(fieldInfo.name).size();
            if (vectorCount == 0) {
                continue;
            }
            totalVectorCount += vectorCount;
            // If there aren't centroids, or previously clustered with more than one cluster
            // or if there are deleted docs, we must recalculate the centroid
            if (centroid == null || mergeState.liveDocs[i] != null) {
                recalculate = true;
                break;
            }
            for (int j = 0; j < centroid.length; j++) {
                mergedCentroid[j] += centroid[j] * vectorCount;
            }
        }
        if (recalculate) {
            return calculateCentroid(mergeState, fieldInfo, mergedCentroid);
        } else {
            for (int j = 0; j < mergedCentroid.length; j++) {
                mergedCentroid[j] = mergedCentroid[j] / totalVectorCount;
            }
            if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
                VectorUtil.l2normalize(mergedCentroid);
            }
            return totalVectorCount;
        }
    }

    static int calculateCentroid(MergeState mergeState, FieldInfo fieldInfo, float[] centroid) throws IOException {
        assert fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32);
        // clear out the centroid
        Arrays.fill(centroid, 0);
        int count = 0;
        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
            KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
            if (knnVectorsReader == null) continue;
            FloatVectorValues vectorValues = mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name);
            if (vectorValues == null) {
                continue;
            }
            KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
            for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                ++count;
                float[] vector = vectorValues.vectorValue(iterator.index());
                // TODO Panama sum
                for (int j = 0; j < vector.length; j++) {
                    centroid[j] += vector[j];
                }
            }
        }
        if (count == 0) {
            return count;
        }
        // TODO Panama div
        for (int i = 0; i < centroid.length; i++) {
            centroid[i] /= count;
        }
        if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
            VectorUtil.l2normalize(centroid);
        }
        return count;
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_RAM_BYTES_USED;
        for (FieldWriter field : fields) {
            // the field tracks the delegate field usage
            total += field.ramBytesUsed();
        }
        return total;
    }

    static class FieldWriter extends FlatFieldVectorsWriter<float[]> {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance(FieldWriter.class);
        private final FieldInfo fieldInfo;
        private boolean finished;
        private final FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter;
        private final float[] dimensionSums;
        private final FloatArrayList magnitudes = new FloatArrayList();

        FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter) {
            this.fieldInfo = fieldInfo;
            this.flatFieldVectorsWriter = flatFieldVectorsWriter;
            this.dimensionSums = new float[fieldInfo.getVectorDimension()];
        }

        @Override
        public List<float[]> getVectors() {
            return flatFieldVectorsWriter.getVectors();
        }

        public void normalizeVectors() {
            for (int i = 0; i < flatFieldVectorsWriter.getVectors().size(); i++) {
                float[] vector = flatFieldVectorsWriter.getVectors().get(i);
                float magnitude = magnitudes.get(i);
                for (int j = 0; j < vector.length; j++) {
                    vector[j] /= magnitude;
                }
            }
        }

        @Override
        public DocsWithFieldSet getDocsWithFieldSet() {
            return flatFieldVectorsWriter.getDocsWithFieldSet();
        }

        @Override
        public void finish() throws IOException {
            if (finished) {
                return;
            }
            assert flatFieldVectorsWriter.isFinished();
            finished = true;
        }

        @Override
        public boolean isFinished() {
            return finished && flatFieldVectorsWriter.isFinished();
        }

        @Override
        public void addValue(int docID, float[] vectorValue) throws IOException {
            flatFieldVectorsWriter.addValue(docID, vectorValue);
            if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
                float dp = VectorUtil.dotProduct(vectorValue, vectorValue);
                float divisor = (float) Math.sqrt(dp);
                magnitudes.add(divisor);
                for (int i = 0; i < vectorValue.length; i++) {
                    dimensionSums[i] += (vectorValue[i] / divisor);
                }
            } else {
                for (int i = 0; i < vectorValue.length; i++) {
                    dimensionSums[i] += vectorValue[i];
                }
            }
        }

        @Override
        public float[] copyValue(float[] vectorValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ramBytesUsed() {
            long size = SHALLOW_SIZE;
            size += flatFieldVectorsWriter.ramBytesUsed();
            size += magnitudes.ramBytesUsed();
            return size;
        }
    }

    // When accessing vectorValue method, targerOrd here means a row ordinal.
    static class OffHeapBinarizedQueryVectorValues {
        private final IndexInput slice;
        private final int dimension;
        private final int size;
        protected final byte[] binaryValue;
        protected final ByteBuffer byteBuffer;
        private final int byteSize;
        protected final float[] correctiveValues;
        private int lastOrd = -1;
        private int quantizedComponentSum;

        OffHeapBinarizedQueryVectorValues(IndexInput data, int dimension, int size) {
            this.slice = data;
            this.dimension = dimension;
            this.size = size;
            // 4x the quantized binary dimensions
            int binaryDimensions = (BQVectorUtils.discretize(dimension, 64) / 8) * BQSpaceUtils.B_QUERY;
            this.byteBuffer = ByteBuffer.allocate(binaryDimensions);
            this.binaryValue = byteBuffer.array();
            // + 1 for the quantized sum
            this.correctiveValues = new float[3];
            this.byteSize = binaryDimensions + Float.BYTES * 3 + Short.BYTES;
        }

        public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int targetOrd) throws IOException {
            if (lastOrd == targetOrd) {
                return new OptimizedScalarQuantizer.QuantizationResult(
                    correctiveValues[0],
                    correctiveValues[1],
                    correctiveValues[2],
                    quantizedComponentSum
                );
            }
            vectorValue(targetOrd);
            return new OptimizedScalarQuantizer.QuantizationResult(
                correctiveValues[0],
                correctiveValues[1],
                correctiveValues[2],
                quantizedComponentSum
            );
        }

        public int size() {
            return size;
        }

        public int dimension() {
            return dimension;
        }

        public OffHeapBinarizedQueryVectorValues copy() throws IOException {
            return new OffHeapBinarizedQueryVectorValues(slice.clone(), dimension, size);
        }

        public IndexInput getSlice() {
            return slice;
        }

        public byte[] vectorValue(int targetOrd) throws IOException {
            if (lastOrd == targetOrd) {
                return binaryValue;
            }
            slice.seek((long) targetOrd * byteSize);
            slice.readBytes(binaryValue, 0, binaryValue.length);
            slice.readFloats(correctiveValues, 0, 3);
            quantizedComponentSum = Short.toUnsignedInt(slice.readShort());
            lastOrd = targetOrd;
            return binaryValue;
        }
    }

    static class BinarizedFloatVectorValues extends BinarizedByteVectorValues {
        private OptimizedScalarQuantizer.QuantizationResult corrections;
        private final byte[] binarized;
        private final byte[] initQuantized;
        private final float[] centroid;
        private final FloatVectorValues values;
        private final OptimizedScalarQuantizer quantizer;

        private int lastOrd = -1;

        BinarizedFloatVectorValues(FloatVectorValues delegate, OptimizedScalarQuantizer quantizer, float[] centroid) {
            this.values = delegate;
            this.quantizer = quantizer;
            this.binarized = new byte[BQVectorUtils.discretize(delegate.dimension(), 64) / 8];
            this.initQuantized = new byte[delegate.dimension()];
            this.centroid = centroid;
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int ord) {
            if (ord != lastOrd) {
                throw new IllegalStateException(
                    "attempt to retrieve corrective terms for different ord " + ord + " than the quantization was done for: " + lastOrd
                );
            }
            return corrections;
        }

        @Override
        public byte[] vectorValue(int ord) throws IOException {
            if (ord != lastOrd) {
                binarize(ord);
                lastOrd = ord;
            }
            return binarized;
        }

        @Override
        public int dimension() {
            return values.dimension();
        }

        @Override
        public OptimizedScalarQuantizer getQuantizer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float[] getCentroid() throws IOException {
            return centroid;
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BinarizedByteVectorValues copy() throws IOException {
            return new BinarizedFloatVectorValues(values.copy(), quantizer, centroid);
        }

        private void binarize(int ord) throws IOException {
            corrections = quantizer.scalarQuantize(values.vectorValue(ord), initQuantized, (byte) 1, centroid);
            BQVectorUtils.packAsBinary(initQuantized, binarized);
        }

        @Override
        public DocIndexIterator iterator() {
            return values.iterator();
        }

        @Override
        public int ordToDoc(int ord) {
            return values.ordToDoc(ord);
        }
    }

    static class BinarizedCloseableRandomVectorScorerSupplier implements CloseableRandomVectorScorerSupplier {
        private final RandomVectorScorerSupplier supplier;
        private final KnnVectorValues vectorValues;
        private final Closeable onClose;

        BinarizedCloseableRandomVectorScorerSupplier(RandomVectorScorerSupplier supplier, KnnVectorValues vectorValues, Closeable onClose) {
            this.supplier = supplier;
            this.onClose = onClose;
            this.vectorValues = vectorValues;
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
            return vectorValues.size();
        }
    }

    static final class NormalizedFloatVectorValues extends FloatVectorValues {
        private final FloatVectorValues values;
        private final float[] normalizedVector;

        NormalizedFloatVectorValues(FloatVectorValues values) {
            this.values = values;
            this.normalizedVector = new float[values.dimension()];
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
        public int ordToDoc(int ord) {
            return values.ordToDoc(ord);
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            System.arraycopy(values.vectorValue(ord), 0, normalizedVector, 0, normalizedVector.length);
            VectorUtil.l2normalize(normalizedVector);
            return normalizedVector;
        }

        @Override
        public DocIndexIterator iterator() {
            return values.iterator();
        }

        @Override
        public NormalizedFloatVectorValues copy() throws IOException {
            return new NormalizedFloatVectorValues(values.copy());
        }
    }
}
