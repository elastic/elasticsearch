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
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringByteVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansByteVectorValues;
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
public abstract class IVFVectorsWriter<CI> extends KnnVectorsWriter {

    private final List<FieldWriter> fieldWriters = new ArrayList<>();
    private final IndexOutput ivfCentroids, ivfClusters;
    private final IndexOutput ivfMeta;
    private final String rawVectorFormatName;
    private final Boolean useDirectIOReads;
    private final FlatVectorsWriter rawVectorDelegate;
    private final int flatVectorThreshold;
    private final boolean shouldWriteDirectIoReads;
    protected final SegmentWriteState segmentWriteState;

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
        this.segmentWriteState = state;
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

<<<<<<< HEAD
    public abstract CentroidInformation<float[]> calculateCentroids(FieldInfo fieldInfo, KMeansFloatVectorValues floatVectorValues)
        throws IOException;

    public abstract CentroidInformation<float[]> calculateCentroids(
=======
    /**
     * Calculate the centroids for the given field and vectors.
     *
     * @param fieldInfo field info
     * @param floatVectorValues float vectors
     * @return centroid information
     * @throws IOException if an I/O error occurs
     */
    public abstract CentroidInformation calculateCentroids(FieldInfo fieldInfo, KMeansFloatVectorValues floatVectorValues)
        throws IOException;

    /**
     * Calculate the centroids for the given field and vectors as part of a merge.
     *
     * @param fieldInfo         field info
     * @param floatVectorValues float vectors
     * @param mergeState        merge information
     * @return centroid information
     * @throws IOException if an I/O error occurs
     */
    public abstract CentroidInformation calculateCentroids(
>>>>>>> upstream/main
        FieldInfo fieldInfo,
        KMeansFloatVectorValues floatVectorValues,
        MergeState mergeState
    ) throws IOException;

    /**
<<<<<<< HEAD
     * Whether this writer supports native byte vector clustering.
     * When {@code false}, byte vectors are clustered via the float path (widening byte→float).
     */
    protected boolean supportsByteVectorClustering() {
        return false;
    }

    /**
     * Calculates centroids for byte-backed vectors during flush.
     */
    public abstract CentroidInformation<byte[]> calculateByteCentroids(FieldInfo fieldInfo, ClusteringByteVectorValues byteVectorValues)
        throws IOException;

    /**
     * Calculates centroids for byte-backed vectors during merge.
     */
    public abstract CentroidInformation<byte[]> calculateByteCentroids(
        FieldInfo fieldInfo,
        ClusteringByteVectorValues byteVectorValues,
        MergeState mergeState
    ) throws IOException;

=======
     * Information on the file offset and length of a set of centroids
     */
>>>>>>> upstream/main
    public record CentroidOffsetAndLength(LongValues offsets, LongValues lengths) {}

    /**
     * Writes any index to {@code centroidOutput}.
     * <p>
     * This is written before the posting lists and the centroid vector data because the centroid data records each
     * centroid's posting-list offset and length, which are not known until the postings have been written. The
     * centroid vector data is written afterwards by {@link #writeCentroidData}.
     * <p>
     * When the centroid index has a two-level (parent/child) structure, child centroids are grouped under their
     * parents and the centroid ordinals are remapped to the grouped ordering; the lookup table is written through
     * that remapping and the grouping is returned so {@code writeCentroidData} can lay out the centroids
     * consistently. When there is no parent structure the lookup is written with an identity mapping and
     * {@code null} is returned.
     *
     * @param centroidSupplier    provides the computed centroids and, via {@link CentroidSupplier#centroidIndex()},
     *                            the optional hierarchical index
     * @param centroidAssignments Array mapping vector ordinal to its assigned centroid ordinal.
     * @param centroidOutput      the centroids file to write to
     * @return Indexing information to be passed to {@code writeCentroidData}, if any
     */
    protected abstract CI writeCentroidIndex(CentroidSupplier centroidSupplier, int[] centroidAssignments, IndexOutput centroidOutput)
        throws IOException;

    /**
     * Builds and writes the per-centroid posting lists for a field during flush.
     * <p>
     * Each vector is grouped into the posting list of the centroid it was first assigned to, and each additional centroid
     * in its overspill assignments. For each centroid, the doc ids of its assigned vectors are written,
     * followed by a quantized version of each vector relative to the centroid and corrections.
     *
     * @param fieldInfo            field info
     * @param centroidSupplier     the computed centroids and centroid index
     * @param floatVectorValues    the raw vectors
     * @param postingsOutput       clusters file output
     * @param fileOffset           base offset in {@code postingsOutput} that the returned offsets and lengths are relative to
     * @param assignments          for each vector ordinal, the ordinal of the centroid it was primarily assigned to
     * @param overspillAssignments additional centroid assignments per vector
     * @param ivfSegmentConfig     IVF segment information
     * @return the per-centroid posting-list offsets and lengths, relative to {@code fileOffset}
     */
    public abstract CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        IvfSegmentConfig ivfSegmentConfig
    ) throws IOException;

    /**
     * Builds and writes the per-centroid posting lists for a field during merge.
     * <p>
     * Each vector is grouped into the posting list of the centroid it was first assigned to, and each additional centroid
     * in its overspill assignments. For each centroid, the doc ids of its assigned vectors are written,
     * followed by a quantized version of each vector relative to the centroid and corrections.
     *
     * @param fieldInfo            field info
     * @param centroidSupplier     the computed centroids and centroid index
     * @param floatVectorValues    the raw vectors
     * @param postingsOutput       clusters file output
     * @param fileOffset           base offset in {@code postingsOutput} that the returned offsets and lengths are relative to
     * @param mergeState           merge information
     * @param assignments          for each vector ordinal, the ordinal of the centroid it was primarily assigned to
     * @param overspillAssignments additional centroid assignments per vector
     * @param ivfSegmentConfig     IVF segment information
     * @return the per-centroid posting-list offsets and lengths, relative to {@code fileOffset}
     */
    public abstract CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        MergeState mergeState,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        IvfSegmentConfig ivfSegmentConfig
    ) throws IOException;

    /**
     * Writes the centroid vector data to {@code centroidOutput}
     * <p>
     * This completes any indexing structure written by {@link #writeCentroidIndex} using data in {@code centroidGroups} (if any),
     * and finishes with the offsets and length of each centroid's postings data in an indexed or flat ordinal order.
     *
     * @param fieldInfo               field info
     * @param centroidSupplier        the computed centroids
     * @param globalCentroid          the global centroid used as the reference point for quantization
     * @param centroidOffsetAndLength the per-centroid posting-list offsets and lengths returned by
     *                                {@link #buildAndWritePostingsLists}
     * @param centroidGroups          Centroid indexing information provided by {@link #writeCentroidIndex}
     * @param centroidOutput          the centroids file to write to
     */
    protected abstract void writeCentroidData(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        CI centroidGroups,
        IndexOutput centroidOutput
    ) throws IOException;

    /**
     * Creates a {@link CentroidSupplier} from off-heap centroid data, usually as part of a merge
     *
     * @param centroidsInput        The centroids as concatenated float32 values
     * @param centroidAssignments   Centroid assignment information
     * @param fieldInfo             field info
     */
    public abstract CentroidSupplier createCentroidSupplier(
        IndexInput centroidsInput,
        CentroidAssignments centroidAssignments,
        FieldInfo fieldInfo
    ) throws IOException;

    /**
     * Creates a {@link CentroidSupplier} from the specified centroids
     */
    public abstract CentroidSupplier createCentroidSupplier(FieldInfo info, float[][] centroids, float[] globalCentroid) throws IOException;

    public abstract CentroidSupplier createCentroidSupplier(FieldInfo info, byte[][] centroids, float[] globalCentroid) throws IOException;

    protected abstract Preconditioner inheritPreconditioner(FieldInfo fieldInfo, MergeState mergeState, IvfSegmentConfig ivfSegmentConfig)
        throws IOException;

    protected abstract Preconditioner createPreconditioner(int dimension, IvfSegmentConfig ivfSegmentConfig);

    protected abstract void writePreconditioner(Preconditioner precondtioner, IndexOutput out) throws IOException;

    protected abstract FloatVectorValues preconditionVectors(
        Preconditioner precondtioner,
        FloatVectorValues vectors,
        IvfSegmentConfig ivfSegmentConfig
    );

    protected abstract Consumer<List<float[]>> preconditionVectors(Preconditioner preconditioner, IvfSegmentConfig ivfSegmentConfig);

    /**
     * Called for each field at the start of {@link #flush} before IVF work.
     * {@link org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsWriter} returns a resolved {@link IvfSegmentConfig};
     * other writers return {@code null}.
     */
    protected IvfSegmentConfig beginIvfFieldFlush(FieldInfo fieldInfo) throws IOException {
        return null;
    }

    /**
     * Called at the start of {@link #mergeOneField} for each field, including non-float
     * encodings, before any IVF or raw vector merge.
     * {@link org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsWriter} returns a resolved {@link IvfSegmentConfig};
     * other writers return {@code null}.
     */
    protected IvfSegmentConfig resolveMergeConfig(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        return null;
    }

    @Override
    public final void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        rawVectorDelegate.flush(maxDoc, sortMap);
        for (FieldWriter fieldWriter : fieldWriters) {
            final IvfSegmentConfig ivfSegmentConfig = beginIvfFieldFlush(fieldWriter.fieldInfo());
            // build preconditioner if necessary, only need one given that this writer is tied to a format that has a fixed dim & block dim
            // write preconditioner subsequently in the centroids file
            Preconditioner preconditioner = createPreconditioner(fieldWriter.fieldInfo().getVectorDimension(), ivfSegmentConfig);
            if (fieldWriter.delegate == null) {
                // field has unknown encoding; just write meta information
                writeMeta(fieldWriter.fieldInfo, 0, 0, 0, 0, 0, null, 0, 0, 0, 0, ivfSegmentConfig);
                continue;
            }
            // build a float vector values with random access
            final KMeansFloatVectorValues floatVectorValues;
            final CentroidSupplier centroidSupplier;
            final int[] clusterAssignments;
            final OverspillAssignments clusterOverspillAssignments;
            final float[] globalCentroid;
            if (fieldWriter.fieldInfo.getVectorEncoding().equals(VectorEncoding.BYTE)) {
                @SuppressWarnings("unchecked")
                final FlatFieldVectorsWriter<byte[]> byteWriter = (FlatFieldVectorsWriter<byte[]>) fieldWriter.delegate;
                boolean normalizeCosine = fieldWriter.fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE;
                floatVectorValues = getKMeansNativeByteVectorValues(
                    fieldWriter.fieldInfo,
                    byteWriter,
                    maxDoc,
                    sortMap,
                    normalizeCosine,
                    preconditioner
                );
                if (supportsByteVectorClustering()
                    && fieldWriter.fieldInfo.getVectorSimilarityFunction() != VectorSimilarityFunction.COSINE) {
                    // Build ClusteringByteVectorValues for native byte clustering
                    final ClusteringByteVectorValues byteVectorValues = getClusteringByteVectorValues(
                        fieldWriter.fieldInfo,
                        byteWriter,
                        maxDoc,
                        sortMap
                    );
                    // build centroids using byte clustering
                    final CentroidInformation<byte[]> centroidInformation = byteVectorValues.size() > 0
                        && flatVectorThreshold > 0
                        && byteVectorValues.size() <= flatVectorThreshold
                            ? buildFlatByteCentroidAssignments(fieldWriter.fieldInfo, byteVectorValues)
                            : calculateByteCentroids(fieldWriter.fieldInfo, byteVectorValues);
                    centroidSupplier = createCentroidSupplier(
                        fieldWriter.fieldInfo,
                        centroidInformation.centroids(),
                        centroidInformation.globalCentroid()
                    );
                    clusterAssignments = centroidInformation.assignments();
                    clusterOverspillAssignments = centroidInformation.overspillAssignments();
                    globalCentroid = centroidInformation.globalCentroid();
                } else {
                    // Fall back to float-based clustering for byte vectors
                    final CentroidInformation<float[]> centroidInformation = floatVectorValues.size() > 0
                        && flatVectorThreshold > 0
                        && floatVectorValues.size() <= flatVectorThreshold
                            ? buildFlatCentroidAssignments(fieldWriter.fieldInfo, floatVectorValues)
                            : calculateCentroids(fieldWriter.fieldInfo, floatVectorValues);
                    centroidSupplier = createCentroidSupplier(
                        fieldWriter.fieldInfo,
                        centroidInformation.centroids(),
                        centroidInformation.globalCentroid()
                    );
                    clusterAssignments = centroidInformation.assignments();
                    clusterOverspillAssignments = centroidInformation.overspillAssignments();
                    globalCentroid = centroidInformation.globalCentroid();
                }
            } else {
                @SuppressWarnings("unchecked")
                final FlatFieldVectorsWriter<float[]> floatWriter = (FlatFieldVectorsWriter<float[]>) fieldWriter.delegate;
                floatVectorValues = getKMeansFloatVectorValues(
                    fieldWriter.fieldInfo,
                    floatWriter,
                    maxDoc,
                    preconditionVectors(preconditioner, ivfSegmentConfig),
                    sortMap
                );
                // build centroids
                final CentroidInformation<float[]> centroidInformation = floatVectorValues.size() > 0
                    && flatVectorThreshold > 0
                    && floatVectorValues.size() <= flatVectorThreshold
                        ? buildFlatCentroidAssignments(fieldWriter.fieldInfo, floatVectorValues)
                        : calculateCentroids(fieldWriter.fieldInfo, floatVectorValues);
                centroidSupplier = createCentroidSupplier(
                    fieldWriter.fieldInfo,
                    centroidInformation.centroids(),
                    centroidInformation.globalCentroid()
                );
                clusterAssignments = centroidInformation.assignments();
                clusterOverspillAssignments = centroidInformation.overspillAssignments();
                globalCentroid = centroidInformation.globalCentroid();
            }

            // write initial centroid index (we might need to read it later for overspilling)
            final long centroidOffset = ivfCentroids.alignFilePointer(Float.BYTES);
            CI centroidIndex = writeCentroidIndex(centroidSupplier, clusterAssignments, ivfCentroids);

            // write posting lists
            final long postingListOffset = ivfClusters.alignFilePointer(Float.BYTES);
            final CentroidOffsetAndLength centroidOffsetAndLength = buildAndWritePostingsLists(
                fieldWriter.fieldInfo,
                centroidSupplier,
                floatVectorValues,
                ivfClusters,
                postingListOffset,
                clusterAssignments,
                clusterOverspillAssignments,
                ivfSegmentConfig
            );
            final long postingListLength = ivfClusters.getFilePointer() - postingListOffset;

            // write the rest of the centroid data now we know the size of the postings
            writeCentroidData(
                fieldWriter.fieldInfo,
                centroidSupplier,
                globalCentroid,
                centroidOffsetAndLength,
                centroidIndex,
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
                0,
                ivfSegmentConfig
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
     * Builds a byte-backed {@link KMeansFloatVectorValues} from a {@link FlatFieldVectorsWriter}
     * of byte vectors. Keeps the raw byte data, enabling native byte[] quantization via
     * {@link KMeansFloatVectorValues#byteVectorValue(int)}.
     * <p>
     * Byte-to-float conversion happens lazily on {@link KMeansFloatVectorValues#vectorValue(int)} calls.
     * When {@code normalize} is true (cosine similarity), the converted float vector is L2-normalized.
     * When a non-null {@code preconditioner} is provided, it is applied lazily during float conversion.
     */
    private static KMeansFloatVectorValues getKMeansNativeByteVectorValues(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<byte[]> fieldVectorsWriter,
        int maxDoc,
        Sorter.DocMap sortMap,
        boolean normalize,
        Preconditioner preconditioner
    ) throws IOException {
        List<byte[]> byteVectors = fieldVectorsWriter.getVectors();
        if (byteVectors.size() == maxDoc && sortMap == null) {
            return KMeansFloatVectorValues.buildFromBytes(byteVectors, null, fieldInfo.getVectorDimension(), normalize, preconditioner);
        } else if (sortMap == null) {
            final DocIdSetIterator iterator = fieldVectorsWriter.getDocsWithFieldSet().iterator();
            final int[] docIds = new int[byteVectors.size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = iterator.nextDoc();
            }
            assert iterator.nextDoc() == NO_MORE_DOCS;
            return KMeansFloatVectorValues.buildFromBytes(byteVectors, docIds, fieldInfo.getVectorDimension(), normalize, preconditioner);
        } else {
            DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
            final int[] ordMap = new int[fieldVectorsWriter.getDocsWithFieldSet().cardinality()];
            KnnVectorsWriter.mapOldOrdToNewOrd(fieldVectorsWriter.getDocsWithFieldSet(), sortMap, null, ordMap, newDocsWithField);
            final DocIdSetIterator iterator = newDocsWithField.iterator();
            final int[] docIds = new int[byteVectors.size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = iterator.nextDoc();
            }
            assert iterator.nextDoc() == NO_MORE_DOCS;
            List<byte[]> orderedVectors = new AbstractList<>() {
                @Override
                public int size() {
                    return byteVectors.size();
                }

                @Override
                public byte[] get(int index) {
                    return byteVectors.get(ordMap[index]);
                }
            };
            return KMeansFloatVectorValues.buildFromBytes(
                orderedVectors,
                docIds,
                fieldInfo.getVectorDimension(),
                normalize,
                preconditioner
            );
        }
    }

    /**
     * Builds a {@link ClusteringByteVectorValues} from a {@link FlatFieldVectorsWriter} of byte vectors.
     * Used for native byte clustering during flush.
     */
    private static ClusteringByteVectorValues getClusteringByteVectorValues(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<byte[]> fieldVectorsWriter,
        int maxDoc,
        Sorter.DocMap sortMap
    ) throws IOException {
        List<byte[]> byteVectors = fieldVectorsWriter.getVectors();
        if (byteVectors.size() == maxDoc && sortMap == null) {
            return KMeansByteVectorValues.build(byteVectors, null, fieldInfo.getVectorDimension());
        } else if (sortMap == null) {
            final DocIdSetIterator iterator = fieldVectorsWriter.getDocsWithFieldSet().iterator();
            final int[] docIds = new int[byteVectors.size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = iterator.nextDoc();
            }
            assert iterator.nextDoc() == NO_MORE_DOCS;
            return KMeansByteVectorValues.build(byteVectors, docIds, fieldInfo.getVectorDimension());
        } else {
            DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
            final int[] ordMap = new int[fieldVectorsWriter.getDocsWithFieldSet().cardinality()];
            KnnVectorsWriter.mapOldOrdToNewOrd(fieldVectorsWriter.getDocsWithFieldSet(), sortMap, null, ordMap, newDocsWithField);
            final DocIdSetIterator iterator = newDocsWithField.iterator();
            final int[] docIds = new int[byteVectors.size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = iterator.nextDoc();
            }
            assert iterator.nextDoc() == NO_MORE_DOCS;
            List<byte[]> orderedVectors = new AbstractList<>() {
                @Override
                public int size() {
                    return byteVectors.size();
                }

                @Override
                public byte[] get(int index) {
                    return byteVectors.get(ordMap[index]);
                }
            };
            return KMeansByteVectorValues.build(orderedVectors, docIds, fieldInfo.getVectorDimension());
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
    protected final CentroidInformation<float[]> buildFlatCentroidAssignments(FieldInfo fieldInfo, FloatVectorValues floatVectorValues)
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
<<<<<<< HEAD
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
            if (Math.abs(avgMagnitude - centroidNorm) > 1e-8) {
                if (centroidNorm > 0) {
                    float scale = (float) (avgMagnitude / centroidNorm);
                    for (int d = 0; d < dimension; d++) {
                        centroid[d] *= scale;
                    }
                }
            }
        }
        // For flat centroid assignments there is a single global centroid and no SOAR (secondary) centroid assignments,
        // so we pass an empty array for soarAssignments.
        int[] assignments = new int[count];
        return new CentroidInformation<>(dimension, new float[][] { centroid }, assignments, new SoarAssignments(new int[0]));
    }

    /**
     * Builds a flat centroid assignment for a small set of byte vectors.
     * Computes the centroid by averaging widened byte values and then rounding back to byte.
     */
    protected final CentroidInformation<byte[]> buildFlatByteCentroidAssignments(
        FieldInfo fieldInfo,
        ClusteringByteVectorValues byteVectorValues
    ) throws IOException {
        int dimension = fieldInfo.getVectorDimension();
        int count = byteVectorValues.size();
        // Accumulate in float for precision
        float[] floatCentroid = new float[dimension];
        for (int i = 0; i < count; i++) {
            byte[] vector = byteVectorValues.vectorValue(i);
            for (int d = 0; d < dimension; d++) {
                floatCentroid[d] += vector[d];
            }
        }
        for (int d = 0; d < dimension; d++) {
            floatCentroid[d] /= count;
        }
        // Scale centroid magnitude to match the average magnitude of assigned vectors.
        VectorSimilarityFunction sim = fieldInfo.getVectorSimilarityFunction();
        if (sim == VectorSimilarityFunction.DOT_PRODUCT || sim == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT) {
            double magnitudeSum = 0;
            for (int i = 0; i < count; i++) {
                byte[] vector = byteVectorValues.vectorValue(i);
                magnitudeSum += Math.sqrt(ESVectorUtil.dotProduct(vector, vector));
            }
            double avgMagnitude = magnitudeSum / count;
            double centroidNorm = Math.sqrt(ESVectorUtil.dotProduct(floatCentroid, floatCentroid));
            if (Math.abs(avgMagnitude - centroidNorm) > 1e-8) {
                if (centroidNorm > 0) {
                    float scale = (float) (avgMagnitude / centroidNorm);
                    for (int d = 0; d < dimension; d++) {
                        floatCentroid[d] *= scale;
                    }
                }
            }
        }
        // Round to byte
        byte[] byteCentroid = new byte[dimension];
        for (int d = 0; d < dimension; d++) {
            byteCentroid[d] = (byte) Math.clamp(Math.round(floatCentroid[d]), -128, 127);
        }
        int[] assignments = new int[count];
        return CentroidInformation.ofBytes(dimension, new byte[][] { byteCentroid }, assignments, new SoarAssignments(new int[0]));
=======
        // For flat centroid assignments there is a single global centroid and no secondary centroid assignments
        return new CentroidInformation(dimension, new float[][] { centroid }, new int[count], OverspillAssignments.NONE);
>>>>>>> upstream/main
    }

    @Override
    public final IORunnable mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
<<<<<<< HEAD
        // Per-field merge hook (see beginIvfFieldMerge): subclasses such as ESNextDiskBBQVectorsWriter resolve their
        // segment config here, and it must run for every field, including non-float encodings. The result is intentionally
        // not used in this base implementation - the float path re-resolves it inside mergeOneFieldIVF and the byte path
        // writes IvfSegmentConfig.NONE below.
        beginIvfFieldMerge(fieldInfo, mergeState);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32) || fieldInfo.getVectorEncoding().equals(VectorEncoding.BYTE)) {
            mergeOneFieldIVF(fieldInfo, mergeState);
=======
        IvfSegmentConfig resolvedConfig = resolveMergeConfig(fieldInfo, mergeState);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            mergeOneFieldIVF(fieldInfo, mergeState, resolvedConfig);
>>>>>>> upstream/main
        } else {
            // we simply write information that the field is present but we don't do anything with it.
            writeMeta(fieldInfo, 0, 0, 0, 0, 0, null, 0, 0, 0, 0, IvfSegmentConfig.NONE);
        }
        // we merge the vectors at the end so we only have two copies of the vectors on disk at the same time.
        rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
        return null;
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
        int maxSliceSize,
        IvfSegmentConfig ivfSegmentConfig
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
        doWriteMeta(
            ivfMeta,
            field,
            numCentroids,
            preconditionerOffset,
            preconditionerLength,
            numberOfSlices,
            maxSliceSize,
            ivfSegmentConfig
        );
    }

    /**
     * Write any additional meta information to the end of {@code metaOutput}
     */
    protected abstract void doWriteMeta(
        IndexOutput metaOutput,
        FieldInfo field,
        int numCentroids,
        long preconditionerOffset,
        long preconditionerLength,
        int numberOfSlices,
        int maxSliceSize,
        IvfSegmentConfig ivfSegmentConfig
    ) throws IOException;

    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    private void mergeOneFieldIVF(FieldInfo fieldInfo, MergeState mergeState, IvfSegmentConfig resolvedConfig) throws IOException {
        final IvfSegmentConfig ivfSegmentConfig = resolvedConfig != null ? resolvedConfig : IvfSegmentConfig.NONE;
        final int numVectors;
        String tempRawVectorsFileName = null;
        String docsFileName = null;
        Preconditioner preconditioner;
        // Track whether we wrote raw bytes (true) or floats (false) to the temp file
        final boolean wroteBytes;
        // For byte fields, track whether cosine normalization is needed for the lazy float conversion
        final boolean normalizeCosine;

        // build a float vector values with random access. In order to do that we dump the vectors to
        // a temporary file and if the segment is not dense, the docs to another file/
        try (
            IndexOutput vectorsOut = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "ivfvec_", IOContext.DEFAULT)
        ) {
            tempRawVectorsFileName = vectorsOut.getName();

            // TODO: we only want to write this once but we'll wind up doing it for every field with the same dim and blockdim
            preconditioner = inheritPreconditioner(fieldInfo, mergeState, ivfSegmentConfig);
            boolean isByteField = fieldInfo.getVectorEncoding().equals(VectorEncoding.BYTE);
            normalizeCosine = isByteField && fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE;

            final int vectorCount;
            if (isByteField) {
                // Write raw bytes (1 byte/dim) — 4x more compact than float path.
                // Preconditioning (if enabled) is applied lazily when reading from the temp file.
                ByteVectorValues mergedByteValues = MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
                boolean dense = mergedByteValues.size() == mergeState.segmentInfo.maxDoc();
                try (
                    IndexOutput docsOut = dense
                        ? null
                        : mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "ivfdoc_", IOContext.DEFAULT)
                ) {
                    if (docsOut != null) {
                        docsFileName = docsOut.getName();
                    }
                    vectorCount = writeByteVectorValues(fieldInfo, docsOut, vectorsOut, mergedByteValues);
                    CodecUtil.writeFooter(vectorsOut);
                    if (docsOut != null) {
                        CodecUtil.writeFooter(docsOut);
                    }
                }
                wroteBytes = true;
            } else {
                FloatVectorValues mergedFloatVectorValues = MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
                mergedFloatVectorValues = preconditionVectors(preconditioner, mergedFloatVectorValues, ivfSegmentConfig);
                boolean dense = mergedFloatVectorValues.size() == mergeState.segmentInfo.maxDoc();
                try (
                    IndexOutput docsOut = dense
                        ? null
                        : mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "ivfdoc_", IOContext.DEFAULT)
                ) {
                    if (docsOut != null) {
                        docsFileName = docsOut.getName();
                    }
                    vectorCount = writeFloatVectorValues(fieldInfo, docsOut, vectorsOut, mergedFloatVectorValues);
                    CodecUtil.writeFooter(vectorsOut);
                    if (docsOut != null) {
                        CodecUtil.writeFooter(docsOut);
                    }
                }
                wroteBytes = false;
            }
            numVectors = vectorCount;
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
            writeMeta(fieldInfo, 0, centroidOffset, 0, 0, 0, null, 0, 0, 0, 0, ivfSegmentConfig);
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
            final KMeansFloatVectorValues floatVectorValues = wroteBytes
                ? getKMeansFloatVectorValuesFromBytes(fieldInfo, docs, vectors, numVectors, normalizeCosine, preconditioner)
                : getKMeansFloatVectorValues(fieldInfo, docs, vectors, numVectors);

            final long centroidOffset;
            final long centroidLength;
            final long postingListOffset;
            final long postingListLength;
            final CentroidAssignments assignments;
            String centroidTempName = null;
            IndexOutput centroidTemp = null;
            try {
                centroidTemp = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "civf_", IOContext.DEFAULT);
                centroidTempName = centroidTemp.getName();
                if (wroteBytes
                    && supportsByteVectorClustering()
                    && fieldInfo.getVectorSimilarityFunction() != VectorSimilarityFunction.COSINE) {
                    // Byte path: cluster with ClusteringByteVectorValues, write byte centroids to temp file.
                    // Clone the IndexInput to avoid sharing seek state with floatVectorValues, which is
                    // accessed later in buildAndWritePostingsLists.
                    ClusteringByteVectorValues byteClusterValues = KMeansByteVectorValues.build(
                        vectors.clone(),
                        docs != null ? docs.clone() : null,
                        numVectors,
                        fieldInfo.getVectorDimension()
                    );
                    CentroidInformation<byte[]> centroidInformation = calculateByteCentroids(fieldInfo, byteClusterValues, mergeState);
                    // write byte centroids to temp file (1 byte per dim)
                    for (byte[] centroid : centroidInformation.centroids()) {
                        centroidTemp.writeBytes(centroid, centroid.length);
                    }
                    assignments = centroidInformation.centroidAssignments();
                } else {
                    // Float path: unchanged (also used as fallback for byte vectors when byte clustering not supported)
                    CentroidInformation<float[]> centroidInformation = calculateCentroids(fieldInfo, floatVectorValues, mergeState);
                    // write float centroids to temp file
                    final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN);
                    for (float[] centroid : centroidInformation.centroids()) {
                        buffer.asFloatBuffer().put(centroid);
                        centroidTemp.writeBytes(buffer.array(), buffer.array().length);
                    }
                    assignments = centroidInformation.centroidAssignments();
                }
            } catch (Throwable t) {
                if (centroidTempName != null) {
                    IOUtils.closeWhileHandlingException(centroidTemp);
                    org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, centroidTempName);
                }
                throw t;
            }
            try {
                if (assignments.numCentroids() == 0) {
                    centroidOffset = ivfCentroids.getFilePointer();
                    writeMeta(fieldInfo, 0, centroidOffset, 0, 0, 0, null, 0, 0, 0, 0, ivfSegmentConfig);
                    CodecUtil.writeFooter(centroidTemp);
                    IOUtils.close(centroidTemp);
                    return;
                }
                CodecUtil.writeFooter(centroidTemp);
                IOUtils.close(centroidTemp);

                try (IndexInput centroidsInput = mergeState.segmentInfo.dir.openInput(centroidTempName, IOContext.DEFAULT)) {
                    CentroidSupplier centroidSupplier = createCentroidSupplier(centroidsInput, assignments, fieldInfo);

                    // write initial centroid index (we might need to read it later for overspilling)
                    centroidOffset = ivfCentroids.alignFilePointer(Float.BYTES);
                    CI centroidIndex = writeCentroidIndex(centroidSupplier, assignments.assignments(), ivfCentroids);

                    // write posting lists
                    postingListOffset = ivfClusters.alignFilePointer(Float.BYTES);
                    final CentroidOffsetAndLength centroidOffsetAndLength = buildAndWritePostingsLists(
                        fieldInfo,
                        centroidSupplier,
                        floatVectorValues,
                        ivfClusters,
                        postingListOffset,
                        mergeState,
                        assignments.assignments(),
                        assignments.overspillAssignments(),
                        ivfSegmentConfig
                    );
                    postingListLength = ivfClusters.getFilePointer() - postingListOffset;

                    // write the rest of the centroid data now we know the size of the postings
                    writeCentroidData(
                        fieldInfo,
                        centroidSupplier,
                        assignments.globalCentroid(),
                        centroidOffsetAndLength,
                        centroidIndex,
                        ivfCentroids
                    );
                    centroidLength = ivfCentroids.getFilePointer() - centroidOffset;

                    long preconditionerOffset = ivfCentroids.getFilePointer();
                    writePreconditioner(preconditioner, ivfCentroids);
                    long preconditionerLength = ivfCentroids.getFilePointer() - preconditionerOffset;

                    assert assignments.centroidSlices() == null || assignments.centroidSlices().sliceOffsets().length > 0;
                    // write meta
                    writeMeta(
                        fieldInfo,
                        centroidSupplier.size(),
                        centroidOffset,
                        centroidLength,
                        postingListOffset,
                        postingListLength,
                        assignments.globalCentroid(),
                        preconditionerOffset,
                        preconditionerLength,
                        assignments.centroidSlices() == null ? 0 : assignments.centroidSlices().sliceOffsets().length,
                        assignments.centroidSlices() == null ? 0 : assignments.centroidSlices().maxSliceSize(),
                        ivfSegmentConfig
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

    /**
     * Opens byte-backed temp files as a {@link KMeansFloatVectorValues} that lazily converts
     * byte vectors to float. When {@code normalize} is true (cosine similarity), each converted
     * float vector is L2-normalized.
     */
    private static KMeansFloatVectorValues getKMeansFloatVectorValuesFromBytes(
        FieldInfo fieldInfo,
        IndexInput docs,
        IndexInput vectors,
        int numVectors,
        boolean normalize,
        Preconditioner preconditioner
    ) throws IOException {
        return KMeansFloatVectorValues.buildFromBytes(vectors, docs, numVectors, fieldInfo.getVectorDimension(), normalize, preconditioner);
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

    /**
     * Writes raw byte vectors (1 byte per dimension) and doc IDs to temporary output streams.
     * This is 4x more compact than {@link #writeFloatVectorValues} for byte-encoded fields.
     */
    private static int writeByteVectorValues(
        FieldInfo fieldInfo,
        IndexOutput docsOut,
        IndexOutput vectorsOut,
        ByteVectorValues byteVectorValues
    ) throws IOException {
        int numVectors = 0;
        final KnnVectorValues.DocIndexIterator iterator = byteVectorValues.iterator();
        for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
            numVectors++;
            byte[] bytes = byteVectorValues.vectorValue(iterator.index());
            vectorsOut.writeBytes(bytes, bytes.length);
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
