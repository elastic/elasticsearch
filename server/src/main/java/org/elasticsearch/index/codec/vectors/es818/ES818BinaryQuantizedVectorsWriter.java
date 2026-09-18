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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.es816.BinaryQuantizer;
import org.elasticsearch.simdvec.ESVectorUtil;

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
    // Whether an HNSW graph build follows a merge, and the graph threshold it applies. The graph build's
    // merge scorer consumes the query-side records this writer can produce alongside the index-side ones.
    private final boolean mergeQueryDataForGraphBuild;
    private final int mergeQueryDataGraphThreshold;
    // Query-side files this writer created for the current merge. Once finish() has run they belong to
    // the reader the graph build opens; if the merge aborts before that, close() deletes them.
    private final List<String> mergeQueriesWritten = new ArrayList<>();
    private boolean mergeQueriesHandedOff;
    private boolean finished;

    /**
     * Sole constructor
     *
     * @param vectorsScorer the scorer to use for scoring vectors
     */
    public ES818BinaryQuantizedVectorsWriter(
        ES818BinaryFlatVectorsScorer vectorsScorer,
        FlatVectorsWriter rawVectorDelegate,
        SegmentWriteState state
    ) throws IOException {
        this(vectorsScorer, rawVectorDelegate, state, false, 0);
    }

    /**
     * @param mergeQueryDataForGraphBuild whether an HNSW graph build follows a merge. If so, a merge that is
     *     expected to build a graph also writes the query-side records its merge scorer needs, in the same
     *     pass as the index-side records, for
     *     {@link ES818BinaryQuantizedVectorsReader#getRandomVectorScorerSupplierForMerge} to pick up.
     * @param mergeQueryDataGraphThreshold that build's graph threshold (see {@code hnswGraphThreshold} in
     *     {@code Lucene99HnswVectorsWriter}), which decides whether a merge of a given size builds a graph
     */
    @SuppressWarnings("this-escape")
    public ES818BinaryQuantizedVectorsWriter(
        ES818BinaryFlatVectorsScorer vectorsScorer,
        FlatVectorsWriter rawVectorDelegate,
        SegmentWriteState state,
        boolean mergeQueryDataForGraphBuild,
        int mergeQueryDataGraphThreshold
    ) throws IOException {
        super(vectorsScorer);
        this.vectorsScorer = vectorsScorer;
        this.segmentWriteState = state;
        this.mergeQueryDataForGraphBuild = mergeQueryDataForGraphBuild;
        this.mergeQueryDataGraphThreshold = mergeQueryDataGraphThreshold;
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
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
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
        float centroidDp = fieldData.getVectors().size() > 0 ? ESVectorUtil.dotProduct(clusterCenter, clusterCenter) : 0;

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
        int[] quantizationScratch = new int[discreteDims];
        byte[] vector = new byte[discreteDims / 8];
        float[] scratch = new float[fieldData.fieldInfo.getVectorDimension()];
        for (int i = 0; i < fieldData.getVectors().size(); i++) {
            float[] v = fieldData.getVectors().get(i);
            OptimizedScalarQuantizer.QuantizationResult corrections = scalarQuantizer.scalarQuantize(
                v,
                scratch,
                quantizationScratch,
                (byte) 1,
                clusterCenter
            );
            ESVectorUtil.pack1BitValues(quantizationScratch, vector);
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

        float centroidDp = ESVectorUtil.dotProduct(clusterCenter, clusterCenter);
        writeMeta(fieldData.fieldInfo, maxDoc, vectorDataOffset, quantizedVectorLength, clusterCenter, centroidDp, newDocsWithField);
    }

    private void writeSortedBinarizedVectors(
        FieldWriter fieldData,
        float[] clusterCenter,
        int[] ordMap,
        OptimizedScalarQuantizer scalarQuantizer
    ) throws IOException {
        int discreteDims = BQVectorUtils.discretize(fieldData.fieldInfo.getVectorDimension(), 64);
        int[] quantizationScratch = new int[discreteDims];
        byte[] vector = new byte[discreteDims / 8];
        float[] scratch = new float[fieldData.fieldInfo.getVectorDimension()];
        for (int ordinal : ordMap) {
            float[] v = fieldData.getVectors().get(ordinal);
            OptimizedScalarQuantizer.QuantizationResult corrections = scalarQuantizer.scalarQuantize(
                v,
                scratch,
                quantizationScratch,
                (byte) 1,
                clusterCenter
            );
            ESVectorUtil.pack1BitValues(quantizationScratch, vector);
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
        // From here on the query-side files belong to the reader the graph build opens (Lucene calls
        // finish() and close() back to back before opening it). A merge that aborts earlier never gets
        // here, and close() deletes them.
        mergeQueriesHandedOff = true;
    }

    @Override
    public void mergeOneFlatVectorField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            final float[] centroid;
            final float[] mergedCentroid = new float[fieldInfo.getVectorDimension()];
            int vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);
            // Don't need access to the random vectors, we can just use the merged
            centroid = mergedCentroid;
            if (segmentWriteState.infoStream.isEnabled(BINARIZED_VECTOR_COMPONENT)) {
                segmentWriteState.infoStream.message(BINARIZED_VECTOR_COMPONENT, "Vectors' count:" + vectorCount);
            }
            FloatVectorValues floatVectorValues = KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
            if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
                floatVectorValues = new NormalizedFloatVectorValues(floatVectorValues);
            }
            OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
            long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
            DocsWithFieldSet docsWithField;
            if (graphBuildFollows(vectorCount)) {
                // One pass over the merged vectors writes both the index-side records and the query-side
                // records the graph build's merge scorer needs, so the merged raw vectors are never read back.
                docsWithField = writeBinarizedVectorAndMergeQueryData(fieldInfo, floatVectorValues, quantizer, centroid);
            } else {
                docsWithField = writeBinarizedVectorData(
                    binarizedVectorData,
                    new BinarizedFloatVectorValues(floatVectorValues, quantizer, centroid)
                );
            }
            long vectorDataLength = binarizedVectorData.getFilePointer() - vectorDataOffset;
            float centroidDp = docsWithField.cardinality() > 0 ? ESVectorUtil.dotProduct(centroid, centroid) : 0;
            writeMeta(
                fieldInfo,
                segmentWriteState.segmentInfo.maxDoc(),
                vectorDataOffset,
                vectorDataLength,
                centroid,
                centroidDp,
                docsWithField
            );
        }
    }

    static DocsWithFieldSet writeBinarizedVectorData(IndexOutput output, BinarizedByteVectorValues binarizedByteVectorValues)
        throws IOException {
        DocsWithFieldSet docsWithField = new DocsWithFieldSet();
        KnnVectorValues.DocIndexIterator iterator = binarizedByteVectorValues.iterator();
        for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
            // write vector
            byte[] binaryValue = binarizedByteVectorValues.vectorValue(iterator.index());
            output.writeBytes(binaryValue, binaryValue.length);
            writeCorrectiveTerms(output, binarizedByteVectorValues.getCorrectiveTerms(iterator.index()));
            docsWithField.add(docV);
        }
        return docsWithField;
    }

    /**
     * Writes the merged vectors' 1-bit index-side records to the quantized vector data file and, in the same
     * pass over the same float vectors, their 4-bit query-side records to a file with a fixed name for this
     * segment, format suffix and field. {@link ES818BinaryQuantizedVectorsReader#getRandomVectorScorerSupplierForMerge}
     * opens that file to score the graph build instead of reading the merged raw vectors back. Both record
     * layouts are shared with the flush-time writer and with merges that build no graph.
     */
    private DocsWithFieldSet writeBinarizedVectorAndMergeQueryData(
        FieldInfo fieldInfo,
        FloatVectorValues floatVectorValues,
        OptimizedScalarQuantizer quantizer,
        float[] centroid
    ) throws IOException {
        int dimension = floatVectorValues.dimension();
        int discretizedDims = BQVectorUtils.discretize(dimension, 64);
        float[] scratch = new float[dimension];
        int[] indexQuantized = new int[dimension];
        byte[] indexPacked = new byte[discretizedDims / 8];
        int[] queryQuantized = new int[dimension];
        byte[] queryPacked = new byte[(discretizedDims / 8) * BinaryQuantizer.B_QUERY];
        int[][] quantized = new int[][] { indexQuantized, queryQuantized };
        byte[] bits = new byte[] { INDEX_BITS, BinaryQuantizer.B_QUERY };
        DocsWithFieldSet docsWithField = new DocsWithFieldSet();
        // A fixed name, so the reader of this segment can open it without listing the directory. A stale
        // file of that name is not expected (IndexWriter deletes a failed merge's files); if one exists,
        // createOutput fails here and the merge fails loudly rather than consuming it.
        String queriesName = mergeQueriesTempName(segmentWriteState.segmentInfo.name, segmentWriteState.segmentSuffix, fieldInfo.number);
        IndexOutput queries = segmentWriteState.directory.createOutput(queriesName, segmentWriteState.context);
        mergeQueriesWritten.add(queriesName);
        try (queries) {
            // The header ties the file to this segment: the reader accepts only a file whose segment id and
            // suffix match, and treats anything else of that name as corrupt.
            CodecUtil.writeIndexHeader(
                queries,
                MERGE_QUERIES_CODEC_NAME,
                ES818BinaryQuantizedVectorsFormat.VERSION_CURRENT,
                segmentWriteState.segmentInfo.getId(),
                segmentWriteState.segmentSuffix
            );
            KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
            for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
                // one centering pass over the vector serves both quantizations
                OptimizedScalarQuantizer.QuantizationResult[] results = quantizer.multiScalarQuantize(
                    floatVectorValues.vectorValue(iterator.index()),
                    scratch,
                    quantized,
                    bits,
                    centroid
                );
                writeIndexRecord(binarizedVectorData, indexQuantized, indexPacked, results[0]);
                writeQueryRecord(queries, queryQuantized, queryPacked, results[1]);
                docsWithField.add(docV);
            }
            CodecUtil.writeFooter(queries);
        } catch (Throwable t) {
            IOUtils.deleteFilesIgnoringExceptions(segmentWriteState.directory, queriesName);
            throw t;
        }
        return docsWithField;
    }

    /** Whether a merge of {@code vectorCount} vectors goes on to build an HNSW graph, as {@code Lucene99HnswVectorsWriter} decides it. */
    private boolean graphBuildFollows(int vectorCount) {
        if (mergeQueryDataForGraphBuild == false || vectorCount <= 0) {
            return false;
        }
        if (mergeQueryDataGraphThreshold <= 0) {
            return true;
        }
        int expectedVisitedNodes = HnswGraphSearcher.expectedVisitedNodes(mergeQueryDataGraphThreshold, vectorCount);
        return vectorCount > expectedVisitedNodes && expectedVisitedNodes > 0;
    }

    /** The 1-bit index-side record: packed bits, then the corrective terms. */
    static void writeIndexRecord(
        IndexOutput output,
        int[] quantized,
        byte[] packed,
        OptimizedScalarQuantizer.QuantizationResult corrections
    ) throws IOException {
        ESVectorUtil.pack1BitValues(quantized, packed);
        output.writeBytes(packed, packed.length);
        writeCorrectiveTerms(output, corrections);
    }

    /** The 4-bit query-side record: strided nibbles, then the corrective terms. */
    static void writeQueryRecord(
        IndexOutput output,
        int[] quantized,
        byte[] packed,
        OptimizedScalarQuantizer.QuantizationResult corrections
    ) throws IOException {
        ESVectorUtil.stride4BitValues(quantized, packed);
        output.writeBytes(packed, packed.length);
        writeCorrectiveTerms(output, corrections);
    }

    static void writeCorrectiveTerms(IndexOutput output, OptimizedScalarQuantizer.QuantizationResult corrections) throws IOException {
        output.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
        output.writeInt(Float.floatToIntBits(corrections.upperInterval()));
        output.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
        assert corrections.quantizedComponentSum() >= 0 && corrections.quantizedComponentSum() <= 0xffff;
        output.writeShort((short) corrections.quantizedComponentSum());
    }

    static final byte INDEX_BITS = 1;

    /** Segment-suffix component of the query-side temp file a merge writes: {@code <segment>_<suffix>_bbqmq<field>.tmp}. */
    static final String MERGE_QUERIES_TEMP_SUFFIX = "bbqmq";

    /** Codec name in the query-side temp file's index header, which also carries the segment id and suffix. */
    static final String MERGE_QUERIES_CODEC_NAME = "ES818BinaryQuantizedVectorsFormatMergeQueries";

    /** The fixed name of the query-side temp file for a field of a segment being merged. */
    static String mergeQueriesTempName(String segmentName, String segmentSuffix, int fieldNumber) {
        String suffix = MERGE_QUERIES_TEMP_SUFFIX + fieldNumber;
        return IndexFileNames.segmentFileName(segmentName, segmentSuffix.isEmpty() ? suffix : segmentSuffix + "_" + suffix, "tmp");
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.close(meta, binarizedVectorData, rawVectorDelegate);
        } finally {
            // A merge that aborts before finish() (another field's merge failed, or the merge was
            // cancelled) never opens the reader that would own these files, and IndexWriter only deletes
            // a failed merge's files once the merge has registered them, which has not happened yet.
            // Delete them here.
            if (mergeQueriesHandedOff == false && mergeQueriesWritten.isEmpty() == false) {
                IOUtils.deleteFilesIgnoringExceptions(segmentWriteState.directory, mergeQueriesWritten);
            }
        }
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
                float dp = ESVectorUtil.dotProduct(vectorValue, vectorValue);
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

    static class BinarizedFloatVectorValues extends BinarizedByteVectorValues {
        private OptimizedScalarQuantizer.QuantizationResult corrections;
        private final byte[] binarized;
        private final int[] initQuantized;
        private final float[] centroid, scratch;
        private final FloatVectorValues values;
        private final OptimizedScalarQuantizer quantizer;

        private int lastOrd = -1;

        BinarizedFloatVectorValues(FloatVectorValues delegate, OptimizedScalarQuantizer quantizer, float[] centroid) {
            this.values = delegate;
            this.quantizer = quantizer;
            this.binarized = new byte[BQVectorUtils.discretize(delegate.dimension(), 64) / 8];
            this.initQuantized = new int[delegate.dimension()];
            this.scratch = new float[delegate.dimension()];
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
            corrections = quantizer.scalarQuantize(values.vectorValue(ord), scratch, initQuantized, (byte) 1, centroid);
            ESVectorUtil.pack1BitValues(initQuantized, binarized);
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
        public int getVectorByteLength() {
            return values.getVectorByteLength();
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
