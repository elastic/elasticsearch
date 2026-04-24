/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.ByteVectorValues;
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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Base class for IVF vectors writer.
 */
public abstract class IVFVectorsWriter extends KnnVectorsWriter {

    private final List<FieldWriter> fieldWriters = new ArrayList<>();
    private final IndexOutput ivfCentroids, ivfClusters;
    private final IndexOutput ivfMeta;
    private final String rawVectorFormatName;
    private final Boolean useDirectIOReads;
    private final FlatVectorsWriter rawVectorDelegate;
    private final int flatVectorThreshold;
    private final boolean shouldWriteDirectIoReads;

    @SuppressWarnings("this-escape")
    protected IVFVectorsWriter(
        SegmentWriteState state,
        String rawVectorFormatName,
        Boolean useDirectIOReads,
        FlatVectorsWriter rawVectorDelegate,
        int writeVersion,
        String codecName,
        String metaExtension,
        String centroidExtension,
        String clusterExtension,
        boolean shouldWriteDirectIoReads,
        int flatVectorThreshold
    ) throws IOException {
        this.rawVectorFormatName = rawVectorFormatName;
        this.useDirectIOReads = useDirectIOReads;
        this.rawVectorDelegate = rawVectorDelegate;
        this.flatVectorThreshold = flatVectorThreshold;
        this.shouldWriteDirectIoReads = shouldWriteDirectIoReads;
        final String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
        final String ivfCentroidsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, centroidExtension);
        final String ivfClustersFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, clusterExtension);
        try {
            ivfMeta = state.directory.createOutput(metaFileName, state.context);
            CodecUtil.writeIndexHeader(ivfMeta, codecName, writeVersion, state.segmentInfo.getId(), state.segmentSuffix);
            ivfCentroids = state.directory.createOutput(ivfCentroidsFileName, state.context);
            CodecUtil.writeIndexHeader(ivfCentroids, codecName, writeVersion, state.segmentInfo.getId(), state.segmentSuffix);
            ivfClusters = state.directory.createOutput(ivfClustersFileName, state.context);
            CodecUtil.writeIndexHeader(ivfClusters, codecName, writeVersion, state.segmentInfo.getId(), state.segmentSuffix);
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    @Override
    public final KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        final FlatFieldVectorsWriter<?> rawVectorDelegate = this.rawVectorDelegate.addField(fieldInfo);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32) || fieldInfo.getVectorEncoding().equals(VectorEncoding.BYTE)) {
            fieldWriters.add(new FieldWriter(fieldInfo, rawVectorDelegate));
        } else {
            // unknown encoding; write meta presence only
            fieldWriters.add(new FieldWriter(fieldInfo, null));
        }
        return rawVectorDelegate;
    }

    /**
     * Returns the effective similarity function. For cosine similarity,
     * vectors are expected to be L2-normalized before entering the pipeline (float vectors are
     * normalized by the mapper, byte vectors are normalized during byte-to-float conversion),
     * so the effective similarity for clustering and quantization is dot-product.
     */
    public static VectorSimilarityFunction effectiveSimilarity(FieldInfo fieldInfo) {
        if (fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE) {
            return VectorSimilarityFunction.DOT_PRODUCT;
        }
        return fieldInfo.getVectorSimilarityFunction();
    }

    /**
     * Scales each centroid so its magnitude matches the average magnitude of the vectors assigned to it.
     * KMeans centroids are arithmetic means of their assigned vectors, which causes "shrinkage toward the mean":
     * the centroid magnitude is systematically smaller than the average magnitude of assigned vectors due to
     * directional variance. This biases residual computation and reduces quantization quality for similarity
     * functions where magnitude matters (dot product, maximum inner product).
     *
     * @param centroids the centroids to scale in-place
     * @param assignments per-vector centroid assignments (index = vector ordinal, value = centroid ordinal)
     * @param vectors random-access vector values for computing magnitudes
     */
    protected static void scaleCentroidsToAverageMagnitude(float[][] centroids, int[] assignments, KMeansFloatVectorValues vectors)
        throws IOException {
        int numCentroids = centroids.length;
        double[] magnitudeSum = new double[numCentroids];
        int[] assignmentCount = new int[numCentroids];
        for (int i = 0; i < assignments.length; i++) {
            int centroidOrd = assignments[i];
            float[] vector = vectors.vectorValue(i);
            double norm = Math.sqrt(ESVectorUtil.dotProduct(vector, vector));
            magnitudeSum[centroidOrd] += norm;
            assignmentCount[centroidOrd]++;
        }
        for (int c = 0; c < numCentroids; c++) {
            if (assignmentCount[c] == 0) {
                continue;
            }
            double avgMagnitude = magnitudeSum[c] / assignmentCount[c];
            float[] centroid = centroids[c];
            double centroidNorm = Math.sqrt(ESVectorUtil.dotProduct(centroid, centroid));
            if (centroidNorm > 0) {
                float scale = (float) (avgMagnitude / centroidNorm);
                for (int d = 0; d < centroid.length; d++) {
                    centroid[d] *= scale;
                }
            }
        }
    }

    public abstract CentroidAssignments calculateCentroids(FieldInfo fieldInfo, KMeansFloatVectorValues floatVectorValues)
        throws IOException;

    public abstract CentroidAssignments calculateCentroids(
        FieldInfo fieldInfo,
        KMeansFloatVectorValues floatVectorValues,
        MergeState mergeState
    ) throws IOException;

    public record CentroidOffsetAndLength(LongValues offsets, LongValues lengths) {}

    public abstract void writeCentroids(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        int[] centroidAssignments,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput
    ) throws IOException;

    public abstract void writeCentroids(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        int[] centroidAssignments,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput,
        MergeState mergeState
    ) throws IOException;

    public abstract CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        int[] assignments,
        int[] overspillAssignments
    ) throws IOException;

    public abstract CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        MergeState mergeState,
        int[] assignments,
        int[] overspillAssignments
    ) throws IOException;

    public abstract CentroidSupplier createCentroidSupplier(
        IndexInput centroidsInput,
        CentroidSlices centroidSlices,
        int numCentroids,
        FieldInfo fieldInfo,
        float[] globalCentroid
    ) throws IOException;

    public abstract CentroidSupplier createCentroidSupplier(FieldInfo info, float[][] centroids, float[] globalCentroid) throws IOException;

    protected abstract Preconditioner inheritPreconditioner(FieldInfo fieldInfo, MergeState mergeState) throws IOException;

    protected abstract Preconditioner createPreconditioner(int dimension);

    protected abstract void writePreconditioner(Preconditioner precondtioner, IndexOutput out) throws IOException;

    protected abstract FloatVectorValues preconditionVectors(Preconditioner precondtioner, FloatVectorValues vectors);

    protected abstract Consumer<List<float[]>> preconditionVectors(Preconditioner preconditioner);

    @Override
    public final void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        rawVectorDelegate.flush(maxDoc, sortMap);
        for (FieldWriter fieldWriter : fieldWriters) {
            // build preconditioner if necessary, only need one given that this writer is tied to a format that has a fixed dim & block dim
            // write preconditioner subsequently in the centroids file
            Preconditioner preconditioner = createPreconditioner(fieldWriter.fieldInfo().getVectorDimension());
            if (fieldWriter.delegate == null) {
                // field has unknown encoding; just write meta information
                writeMeta(fieldWriter.fieldInfo, 0, 0, 0, 0, 0, null, 0, 0, 0, 0);
                continue;
            }
            // build a float vector values with random access
            final KMeansFloatVectorValues floatVectorValues;
            if (fieldWriter.fieldInfo.getVectorEncoding().equals(VectorEncoding.BYTE)) {
                @SuppressWarnings("unchecked")
                final FlatFieldVectorsWriter<byte[]> byteWriter = (FlatFieldVectorsWriter<byte[]>) fieldWriter.delegate;
                boolean normalizeCosine = fieldWriter.fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE;
                floatVectorValues = getKMeansFloatVectorValuesFromBytes(
                    fieldWriter.fieldInfo,
                    byteWriter,
                    maxDoc,
                    preconditionVectors(preconditioner),
                    sortMap,
                    normalizeCosine
                );
            } else {
                @SuppressWarnings("unchecked")
                final FlatFieldVectorsWriter<float[]> floatWriter = (FlatFieldVectorsWriter<float[]>) fieldWriter.delegate;
                floatVectorValues = getKMeansFloatVectorValues(
                    fieldWriter.fieldInfo,
                    floatWriter,
                    maxDoc,
                    preconditionVectors(preconditioner),
                    sortMap
                );
            }

            // build centroids
            final CentroidAssignments centroidAssignments = floatVectorValues.size() > 0
                && flatVectorThreshold > 0
                && floatVectorValues.size() <= flatVectorThreshold
                    ? buildFlatCentroidAssignments(fieldWriter.fieldInfo, floatVectorValues)
                    : calculateCentroids(fieldWriter.fieldInfo, floatVectorValues);
            final CentroidSupplier centroidSupplier = createCentroidSupplier(
                fieldWriter.fieldInfo,
                centroidAssignments.centroids(),
                centroidAssignments.globalCentroid()
            );
            // write posting lists
            final long postingListOffset = ivfClusters.alignFilePointer(Float.BYTES);
            final CentroidOffsetAndLength centroidOffsetAndLength = buildAndWritePostingsLists(
                fieldWriter.fieldInfo,
                centroidSupplier,
                floatVectorValues,
                ivfClusters,
                postingListOffset,
                centroidAssignments.assignments(),
                centroidAssignments.overspillAssignments()
            );
            final long postingListLength = ivfClusters.getFilePointer() - postingListOffset;
            // write centroids
            final float[] globalCentroid = centroidAssignments.globalCentroid();
            final long centroidOffset = ivfCentroids.alignFilePointer(Float.BYTES);
            writeCentroids(
                fieldWriter.fieldInfo,
                centroidSupplier,
                centroidAssignments.assignments(),
                globalCentroid,
                centroidOffsetAndLength,
                ivfCentroids
            );
            final long centroidLength = ivfCentroids.getFilePointer() - centroidOffset;
            long preconditionerOffset = ivfCentroids.getFilePointer();
            writePreconditioner(preconditioner, ivfCentroids);
            long preconditionerLength = ivfCentroids.getFilePointer() - preconditionerOffset;
            // write meta file
            writeMeta(
                fieldWriter.fieldInfo,
                centroidSupplier.size(),
                centroidOffset,
                centroidLength,
                postingListOffset,
                postingListLength,
                globalCentroid,
                preconditionerOffset,
                preconditionerLength,
                0,
                0
            );
        }
    }

    private static KMeansFloatVectorValues getKMeansFloatVectorValues(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<float[]> fieldVectorsWriter,
        int maxDoc,
        Consumer<List<float[]>> vectorTransform,
        Sorter.DocMap sortMap
    ) throws IOException {
        List<float[]> vectors = fieldVectorsWriter.getVectors();
        vectorTransform.accept(vectors);
        if (vectors.size() == maxDoc && sortMap == null) {
            return KMeansFloatVectorValues.build(vectors, null, fieldInfo.getVectorDimension());
        } else if (sortMap == null) {
            final DocIdSetIterator iterator = fieldVectorsWriter.getDocsWithFieldSet().iterator();
            final int[] docIds = new int[vectors.size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = iterator.nextDoc();
            }
            assert iterator.nextDoc() == NO_MORE_DOCS;
            return KMeansFloatVectorValues.build(vectors, docIds, fieldInfo.getVectorDimension());
        } else {
            DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
            final int[] ordMap = new int[fieldVectorsWriter.getDocsWithFieldSet().cardinality()]; // new ord to old ord
            KnnVectorsWriter.mapOldOrdToNewOrd(fieldVectorsWriter.getDocsWithFieldSet(), sortMap, null, ordMap, newDocsWithField);
            final DocIdSetIterator iterator = newDocsWithField.iterator();
            final int[] docIds = new int[vectors.size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = iterator.nextDoc();
            }
            assert iterator.nextDoc() == NO_MORE_DOCS;
            List<float[]> orderedVectors = new AbstractList<>() {

                @Override
                public int size() {
                    return vectors.size();
                }

                @Override
                public float[] get(int index) {
                    return vectors.get(ordMap[index]);
                }
            };
            return KMeansFloatVectorValues.build(orderedVectors, docIds, fieldInfo.getVectorDimension());
        }
    }

    /**
     * Converts byte vectors from a {@link FlatFieldVectorsWriter} to float vectors and builds
     * {@link KMeansFloatVectorValues} for the IVF flush pipeline. Each byte value [-128, 127]
     * becomes the corresponding float [-128.0, 127.0].
     * <p>
     * When {@code normalize} is true (cosine similarity), each converted float vector is
     * L2-normalized to unit length so that the IVF pipeline can treat them identically to
     * dot-product vectors.
     */
    private static KMeansFloatVectorValues getKMeansFloatVectorValuesFromBytes(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<byte[]> fieldVectorsWriter,
        int maxDoc,
        Consumer<List<float[]>> vectorTransform,
        Sorter.DocMap sortMap,
        boolean normalize
    ) throws IOException {
        List<byte[]> byteVectors = fieldVectorsWriter.getVectors();
        // Convert byte vectors to float vectors
        List<float[]> floatVectors = new ArrayList<>(byteVectors.size());
        for (byte[] bv : byteVectors) {
            float[] fv = new float[bv.length];
            for (int i = 0; i < bv.length; i++) {
                fv[i] = bv[i];
            }
            if (normalize) {
                VectorUtil.l2normalize(fv);
            }
            floatVectors.add(fv);
        }
        vectorTransform.accept(floatVectors);
        if (floatVectors.size() == maxDoc && sortMap == null) {
            return KMeansFloatVectorValues.build(floatVectors, null, fieldInfo.getVectorDimension());
        } else if (sortMap == null) {
            final DocIdSetIterator iterator = fieldVectorsWriter.getDocsWithFieldSet().iterator();
            final int[] docIds = new int[floatVectors.size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = iterator.nextDoc();
            }
            assert iterator.nextDoc() == NO_MORE_DOCS;
            return KMeansFloatVectorValues.build(floatVectors, docIds, fieldInfo.getVectorDimension());
        } else {
            DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
            final int[] ordMap = new int[fieldVectorsWriter.getDocsWithFieldSet().cardinality()]; // new ord to old ord
            KnnVectorsWriter.mapOldOrdToNewOrd(fieldVectorsWriter.getDocsWithFieldSet(), sortMap, null, ordMap, newDocsWithField);
            final DocIdSetIterator iterator = newDocsWithField.iterator();
            final int[] docIds = new int[floatVectors.size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = iterator.nextDoc();
            }
            assert iterator.nextDoc() == NO_MORE_DOCS;
            List<float[]> orderedVectors = new AbstractList<>() {
                @Override
                public int size() {
                    return floatVectors.size();
                }

                @Override
                public float[] get(int index) {
                    return floatVectors.get(ordMap[index]);
                }
            };
            return KMeansFloatVectorValues.build(orderedVectors, docIds, fieldInfo.getVectorDimension());
        }
    }

    /**
     * Builds a flat centroid assignment for a small set of vectors.
     * <p>
     * When the number of vectors is below the IVF flush threshold, we do not
     * build multiple clusters. Instead, we compute a single centroid as the
     * arithmetic mean of all vectors and assign every vector to that single
     * centroid, producing a flat vector storage layout.
     *
     * @param fieldInfo          field metadata providing the vector dimension
     * @param floatVectorValues  the vectors to summarize into a single centroid
     * @return a {@link CentroidAssignments} instance with one centroid and
     *         all vectors assigned to it
     */
    protected final CentroidAssignments buildFlatCentroidAssignments(FieldInfo fieldInfo, FloatVectorValues floatVectorValues)
        throws IOException {
        int dimension = fieldInfo.getVectorDimension();
        int count = floatVectorValues.size();
        float[] centroid = new float[dimension];
        for (int i = 0; i < count; i++) {
            float[] vector = floatVectorValues.vectorValue(i);
            for (int d = 0; d < dimension; d++) {
                centroid[d] += vector[d];
            }
        }
        for (int d = 0; d < dimension; d++) {
            centroid[d] /= count;
        }
        // Scale centroid magnitude to match the average magnitude of assigned vectors.
        VectorSimilarityFunction sim = fieldInfo.getVectorSimilarityFunction();
        if (sim == VectorSimilarityFunction.COSINE
            || sim == VectorSimilarityFunction.DOT_PRODUCT
            || sim == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT) {
            double magnitudeSum = 0;
            for (int i = 0; i < count; i++) {
                float[] vector = floatVectorValues.vectorValue(i);
                magnitudeSum += Math.sqrt(ESVectorUtil.dotProduct(vector, vector));
            }
            double avgMagnitude = magnitudeSum / count;
            double centroidNorm = Math.sqrt(ESVectorUtil.dotProduct(centroid, centroid));
            if (centroidNorm > 0) {
                float scale = (float) (avgMagnitude / centroidNorm);
                for (int d = 0; d < dimension; d++) {
                    centroid[d] *= scale;
                }
            }
        }
        // For flat centroid assignments there is a single global centroid and no SOAR (secondary) centroid assignments,
        // so we pass an empty array for soarAssignments.
        int[] assignments = new int[count];
        return new CentroidAssignments(dimension, new float[][] { centroid }, assignments, new int[0]);
    }

    @Override
    public final void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32) || fieldInfo.getVectorEncoding().equals(VectorEncoding.BYTE)) {
            mergeOneFieldIVF(fieldInfo, mergeState);
        } else {
            // we simply write information that the field is present but we don't do anything with it.
            writeMeta(fieldInfo, 0, 0, 0, 0, 0, null, 0, 0, 0, 0);
        }
        // we merge the vectors at the end so we only have two copies of the vectors on disk at the same time.
        rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
    }

    private void writeMeta(
        FieldInfo field,
        int numCentroids,
        long centroidOffset,
        long centroidLength,
        long postingListOffset,
        long postingListLength,
        float[] globalCentroid,
        long preconditionerOffset,
        long preconditionerLength,
        int numberOfSlices,
        int maxSliceSize
    ) throws IOException {
        ivfMeta.writeInt(field.number);
        ivfMeta.writeString(rawVectorFormatName);
        if (shouldWriteDirectIoReads) {
            assert useDirectIOReads != null : "shouldWriteDirectIoReads is true but useDirectIOReads is null";
            ivfMeta.writeByte(useDirectIOReads ? (byte) 1 : 0);
        }
        ivfMeta.writeInt(field.getVectorEncoding().ordinal());
        ivfMeta.writeInt(distFuncToOrd(field.getVectorSimilarityFunction()));
        ivfMeta.writeInt(numCentroids);
        ivfMeta.writeLong(centroidOffset);
        ivfMeta.writeLong(centroidLength);
        if (centroidLength > 0) {
            ivfMeta.writeLong(postingListOffset);
            ivfMeta.writeLong(postingListLength);
            final ByteBuffer buffer = ByteBuffer.allocate(globalCentroid.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buffer.asFloatBuffer().put(globalCentroid);
            ivfMeta.writeBytes(buffer.array(), buffer.array().length);
            ivfMeta.writeInt(Float.floatToIntBits(ESVectorUtil.dotProduct(globalCentroid, globalCentroid)));
        }
        doWriteMeta(ivfMeta, field, numCentroids, preconditionerOffset, preconditionerLength, numberOfSlices, maxSliceSize);
    }

    protected abstract void doWriteMeta(
        IndexOutput metaOutput,
        FieldInfo field,
        int numCentroids,
        long preconditionerOffset,
        long preconditionerLength,
        int numberOfSlices,
        int maxSliceSize
    ) throws IOException;

    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    private void mergeOneFieldIVF(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        final int numVectors;
        String tempRawVectorsFileName = null;
        String docsFileName = null;
        // build a float vector values with random access. In order to do that we dump the vectors to
        // a temporary file and if the segment is not dense, the docs to another file/
        Preconditioner preconditioner;
        try (
            IndexOutput vectorsOut = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "ivfvec_", IOContext.DEFAULT)
        ) {
            tempRawVectorsFileName = vectorsOut.getName();
            FloatVectorValues mergedFloatVectorValues;
            if (fieldInfo.getVectorEncoding().equals(VectorEncoding.BYTE)) {
                ByteVectorValues mergedByteValues = MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
                boolean normalizeCosine = fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE;
                mergedFloatVectorValues = new ByteToFloatVectorValues(mergedByteValues, normalizeCosine);
            } else {
                mergedFloatVectorValues = MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
            }

            // TODO: we only want to write this once but we'll wind up doing it for every field with the same dim and blockdim
            preconditioner = inheritPreconditioner(fieldInfo, mergeState);
            mergedFloatVectorValues = preconditionVectors(preconditioner, mergedFloatVectorValues);

            // if the segment is dense, we don't need to do anything with docIds.
            boolean dense = mergedFloatVectorValues.size() == mergeState.segmentInfo.maxDoc();
            try (
                IndexOutput docsOut = dense
                    ? null
                    : mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "ivfdoc_", IOContext.DEFAULT)
            ) {
                if (docsOut != null) {
                    docsFileName = docsOut.getName();
                }
                // TODO do this better, we shouldn't have to write to a temp file, we should be able to
                // to just from the merged vector values, the tricky part is the random access.
                numVectors = writeFloatVectorValues(fieldInfo, docsOut, vectorsOut, mergedFloatVectorValues);
                CodecUtil.writeFooter(vectorsOut);
                if (docsOut != null) {
                    CodecUtil.writeFooter(docsOut);
                }
            }
        } catch (Throwable t) {
            if (tempRawVectorsFileName != null) {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, tempRawVectorsFileName);
            }
            if (docsFileName != null) {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, docsFileName);
            }
            throw t;
        }
        if (numVectors == 0) {
            long centroidOffset = ivfCentroids.getFilePointer();
            writeMeta(fieldInfo, 0, centroidOffset, 0, 0, 0, null, 0, 0, 0, 0);
            return;
        }
        // now open the temp file and build the index structures. It is expected these files to be read in sequential order.
        // Even when the file might be sample, the reads will be always in increase order, therefore we set the ReadAdvice to SEQUENTIAL
        // so the OS can optimize read ahead in low memory situations.
        try (
            IndexInput vectors = mergeState.segmentInfo.dir.openInput(
                tempRawVectorsFileName,
                IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL)
            );
            IndexInput docs = docsFileName == null
                ? null
                : mergeState.segmentInfo.dir.openInput(docsFileName, IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL))
        ) {
            final KMeansFloatVectorValues floatVectorValues = getKMeansFloatVectorValues(fieldInfo, docs, vectors, numVectors);

            final long centroidOffset;
            final long centroidLength;
            final long postingListOffset;
            final long postingListLength;
            final int numCentroids;
            final int[] assignments;
            final int[] overspillAssignments;
            final float[] calculatedGlobalCentroid;
            final CentroidSlices centroidSlices;
            String centroidTempName = null;
            IndexOutput centroidTemp = null;
            try {
                centroidTemp = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "civf_", IOContext.DEFAULT);
                centroidTempName = centroidTemp.getName();
                CentroidAssignments centroidAssignments = calculateCentroids(fieldInfo, floatVectorValues, mergeState);
                // write the centroids to a temporary file so we are not holding them on heap
                final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                for (float[] centroid : centroidAssignments.centroids()) {
                    buffer.asFloatBuffer().put(centroid);
                    centroidTemp.writeBytes(buffer.array(), buffer.array().length);
                }
                numCentroids = centroidAssignments.numCentroids();
                assignments = centroidAssignments.assignments();
                calculatedGlobalCentroid = centroidAssignments.globalCentroid();
                overspillAssignments = centroidAssignments.overspillAssignments();
                centroidSlices = centroidAssignments.centroidSlices();
            } catch (Throwable t) {
                if (centroidTempName != null) {
                    IOUtils.closeWhileHandlingException(centroidTemp);
                    org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, centroidTempName);
                }
                throw t;
            }
            try {
                if (numCentroids == 0) {
                    centroidOffset = ivfCentroids.getFilePointer();
                    writeMeta(fieldInfo, 0, centroidOffset, 0, 0, 0, null, 0, 0, 0, 0);
                    CodecUtil.writeFooter(centroidTemp);
                    IOUtils.close(centroidTemp);
                    return;
                }
                CodecUtil.writeFooter(centroidTemp);
                IOUtils.close(centroidTemp);

                try (IndexInput centroidsInput = mergeState.segmentInfo.dir.openInput(centroidTempName, IOContext.DEFAULT)) {
                    CentroidSupplier centroidSupplier = createCentroidSupplier(
                        centroidsInput,
                        centroidSlices,
                        numCentroids,
                        fieldInfo,
                        calculatedGlobalCentroid
                    );
                    // write posting lists
                    postingListOffset = ivfClusters.alignFilePointer(Float.BYTES);
                    final CentroidOffsetAndLength centroidOffsetAndLength = buildAndWritePostingsLists(
                        fieldInfo,
                        centroidSupplier,
                        floatVectorValues,
                        ivfClusters,
                        postingListOffset,
                        mergeState,
                        assignments,
                        overspillAssignments
                    );
                    postingListLength = ivfClusters.getFilePointer() - postingListOffset;
                    // write centroids
                    centroidOffset = ivfCentroids.alignFilePointer(Float.BYTES);
                    writeCentroids(
                        fieldInfo,
                        centroidSupplier,
                        assignments,
                        calculatedGlobalCentroid,
                        centroidOffsetAndLength,
                        ivfCentroids,
                        mergeState
                    );
                    centroidLength = ivfCentroids.getFilePointer() - centroidOffset;
                    long preconditionerOffset = ivfCentroids.getFilePointer();
                    writePreconditioner(preconditioner, ivfCentroids);
                    long preconditionerLength = ivfCentroids.getFilePointer() - preconditionerOffset;
                    assert centroidSlices == null || centroidSlices.sliceOffsets().length > 0;
                    // write meta
                    writeMeta(
                        fieldInfo,
                        centroidSupplier.size(),
                        centroidOffset,
                        centroidLength,
                        postingListOffset,
                        postingListLength,
                        calculatedGlobalCentroid,
                        preconditionerOffset,
                        preconditionerLength,
                        centroidSlices == null ? 0 : centroidSlices.sliceOffsets().length,
                        centroidSlices == null ? 0 : centroidSlices.maxSliceSize()
                    );
                }
            } finally {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, centroidTempName);
            }
        } finally {
            if (docsFileName != null) {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(
                    mergeState.segmentInfo.dir,
                    tempRawVectorsFileName,
                    docsFileName
                );
            } else {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, tempRawVectorsFileName);
            }
        }
    }

    private static KMeansFloatVectorValues getKMeansFloatVectorValues(
        FieldInfo fieldInfo,
        IndexInput docs,
        IndexInput vectors,
        int numVectors
    ) throws IOException {
        return KMeansFloatVectorValues.build(vectors, docs, numVectors, fieldInfo.getVectorDimension());
    }

    private static int writeFloatVectorValues(
        FieldInfo fieldInfo,
        IndexOutput docsOut,
        IndexOutput vectorsOut,
        FloatVectorValues floatVectorValues
    ) throws IOException {
        int numVectors = 0;
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        final KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
        for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
            numVectors++;
            buffer.asFloatBuffer().put(floatVectorValues.vectorValue(iterator.index()));
            vectorsOut.writeBytes(buffer.array(), buffer.array().length);
            if (docsOut != null) {
                docsOut.writeInt(iterator.docID());
            }
        }
        return numVectors;
    }

    private static int distFuncToOrd(VectorSimilarityFunction func) {
        for (int i = 0; i < SIMILARITY_FUNCTIONS.size(); i++) {
            if (SIMILARITY_FUNCTIONS.get(i).equals(func)) {
                return (byte) i;
            }
        }
        throw new IllegalArgumentException("invalid distance function: " + func);
    }

    @Override
    public final void finish() throws IOException {
        rawVectorDelegate.finish();
        if (ivfMeta != null) {
            // write end of fields marker
            ivfMeta.writeInt(-1);
            CodecUtil.writeFooter(ivfMeta);
        }
        if (ivfCentroids != null) {
            CodecUtil.writeFooter(ivfCentroids);
        }
        if (ivfClusters != null) {
            CodecUtil.writeFooter(ivfClusters);
        }
    }

    @Override
    public final void close() throws IOException {
        IOUtils.close(rawVectorDelegate, ivfMeta, ivfCentroids, ivfClusters);
    }

    @Override
    public final long ramBytesUsed() {
        return rawVectorDelegate.ramBytesUsed();
    }

    private record FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<?> delegate) {}

}
