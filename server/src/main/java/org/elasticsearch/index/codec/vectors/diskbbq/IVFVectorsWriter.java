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
import org.elasticsearch.index.codec.vectors.cluster.CentroidOps;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringByteVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansByteVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

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
    protected final int flatVectorThreshold;
    private final boolean shouldWriteDirectIoReads;
    protected final SegmentWriteState segmentWriteState;

    /**
     * Returns {@code true} if this codec version supports byte-native clustering and postings.
     */
    protected boolean supportsByteNative() {
        return false;
    }

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
        if (fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE) {
            throw new IllegalArgumentException("IVF does not support cosine similarity");
        }
        final FlatFieldVectorsWriter<?> rawVectorDelegate = this.rawVectorDelegate.addField(fieldInfo);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)
            || (fieldInfo.getVectorEncoding().equals(VectorEncoding.BYTE) && supportsByteNative())) {
            fieldWriters.add(new FieldWriter(fieldInfo, rawVectorDelegate));
        } else {
            // we simply write information that the field is present but we don't do anything with it.
            fieldWriters.add(new FieldWriter(fieldInfo, null));
        }
        return rawVectorDelegate;
    }

    /**
     * Calculate the centroids for the given field and vectors.
     *
     * @param fieldInfo field info
     * @param vectorValues vector values
     * @return centroid information
     * @throws IOException if an I/O error occurs
     */
    public abstract CentroidInformation<?> calculateCentroids(FieldInfo fieldInfo, ClusteringVectorValues<?> vectorValues)
        throws IOException;

    /**
     * Calculate the centroids for the given field and vectors as part of a merge.
     *
     * @param fieldInfo         field info
     * @param vectorValues      vector values
     * @param mergeState        merge information
     * @return centroid information
     * @throws IOException if an I/O error occurs
     */
    public abstract CentroidInformation<?> calculateCentroids(
        FieldInfo fieldInfo,
        ClusteringVectorValues<?> vectorValues,
        MergeState mergeState
    ) throws IOException;

    /**
     * Information on the file offset and length of a set of centroids
     */
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
     * @param vectorValues         the raw vectors
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
        ClusteringVectorValues<?> vectorValues,
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
     * @param vectorValues         the raw vectors (float or byte)
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
        ClusteringVectorValues<?> vectorValues,
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

    /**
     * Creates a {@link CentroidSupplier} from byte centroids. Codec versions that do not support
     * byte-native postings should leave this default (throws).
     */
    protected CentroidSupplier createCentroidSupplier(FieldInfo info, byte[][] centroids, float[] globalCentroid) throws IOException {
        throw new UnsupportedOperationException("Byte centroid supplier not supported by this codec version");
    }

    /**
     * Inherits a preconditioner from one of the merging segments, or creates a new one if none
     * is available. Returns {@code null} if preconditioning is not enabled for this format.
     *
     * <p>During merge, this attempts to reuse an existing preconditioner from a prior segment
     * (via {@link VectorPreconditioner}) so the rotation matrix is consistent across segments.
     * If no prior segment provides one, falls back to {@link #createPreconditioner}.
     */
    protected abstract Preconditioner inheritPreconditioner(FieldInfo fieldInfo, MergeState mergeState, IvfSegmentConfig ivfSegmentConfig)
        throws IOException;

    protected abstract Preconditioner createPreconditioner(int dimension, IvfSegmentConfig ivfSegmentConfig);

    protected abstract void writePreconditioner(Preconditioner precondtioner, IndexOutput out) throws IOException;

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
                // field encoding is not supported, we just write meta information
                writeMeta(fieldWriter.fieldInfo, 0, 0, 0, 0, 0, null, 0, 0, 0, 0, ivfSegmentConfig, false);
                continue;
            }
            // build a float vector values with random access
            final boolean isByte = fieldWriter.fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
            final ClusteringVectorValues<?> clusteringVectorValues;
            if (isByte) {
                @SuppressWarnings("unchecked")
                final FlatFieldVectorsWriter<byte[]> byteWriter = (FlatFieldVectorsWriter<byte[]>) fieldWriter.delegate;
                if (preconditioner != null) {
                    preconditioner.preconditionVectorsInPlace(byteWriter.getVectors(), VectorEncoding.BYTE);
                }
                clusteringVectorValues = getKMeansByteVectorValues(fieldWriter.fieldInfo, byteWriter, maxDoc, sortMap);
            } else {
                @SuppressWarnings("unchecked")
                final FlatFieldVectorsWriter<float[]> floatWriter = (FlatFieldVectorsWriter<float[]>) fieldWriter.delegate;
                if (preconditioner != null) {
                    preconditioner.preconditionVectorsInPlace(floatWriter.getVectors(), VectorEncoding.FLOAT32);
                }
                clusteringVectorValues = getKMeansFloatVectorValues(fieldWriter.fieldInfo, floatWriter, maxDoc, sortMap);
            }

            // build centroids
            final CentroidInformation<?> centroidAssignments;
            centroidAssignments = clusteringVectorValues.size() > 0
                && flatVectorThreshold > 0
                && clusteringVectorValues.size() <= flatVectorThreshold
                    ? buildFlatCentroidAssignments(fieldWriter.fieldInfo, clusteringVectorValues)
                    : calculateCentroids(fieldWriter.fieldInfo, clusteringVectorValues);

            final CentroidOffsetAndLength centroidOffsetAndLength;
            final CentroidSupplier centroidSupplier;
            final long centroidOffset;
            final CI centroidIndex;
            final long postingListOffset;
            final long postingListLength;
            final float[] globalCentroid = centroidAssignments.globalCentroid();

            if (supportsByteNative() && centroidAssignments.centroids() instanceof byte[][] byteCentroidArrays) {
                centroidSupplier = createCentroidSupplier(fieldWriter.fieldInfo, byteCentroidArrays, globalCentroid);
            } else {
                @SuppressWarnings("unchecked")
                CentroidInformation<float[]> floatCentroidInfo = (CentroidInformation<float[]>) centroidAssignments;
                centroidSupplier = createCentroidSupplier(fieldWriter.fieldInfo, floatCentroidInfo.centroids(), globalCentroid);
            }
            centroidOffset = ivfCentroids.alignFilePointer(Float.BYTES);
            // write initial centroid index (we might need to read it later for overspilling)
            centroidIndex = writeCentroidIndex(centroidSupplier, centroidAssignments.assignments(), ivfCentroids);
            postingListOffset = ivfClusters.alignFilePointer(Float.BYTES);

            // For byte fields with float centroids (e.g. flat threshold), widen byte vectors to float
            final ClusteringVectorValues<?> postingsVectorValues;
            if (clusteringVectorValues instanceof ByteVectorValues && centroidSupplier.byteCentroid(0) == null) {
                postingsVectorValues = asFloatVectorValues(fieldWriter.fieldInfo, clusteringVectorValues);
            } else {
                postingsVectorValues = clusteringVectorValues;
            }

            // write posting lists
            centroidOffsetAndLength = buildAndWritePostingsLists(
                fieldWriter.fieldInfo,
                centroidSupplier,
                postingsVectorValues,
                ivfClusters,
                postingListOffset,
                centroidAssignments.assignments(),
                centroidAssignments.overspillAssignments(),
                ivfSegmentConfig
            );
            postingListLength = ivfClusters.getFilePointer() - postingListOffset;

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
                ivfSegmentConfig,
                supportsByteNative() && centroidAssignments.centroids() instanceof byte[][]
            );
        }
    }

    /**
     * Computes doc IDs and ordinal mapping for vectors from a flush writer, handling dense, sparse, and sorted cases.
     */
    private record FlushVectorOrdering(int[] docIds, int[] ordMap) {
        static FlushVectorOrdering compute(FlatFieldVectorsWriter<?> fieldVectorsWriter, int maxDoc, int vectorCount, Sorter.DocMap sortMap)
            throws IOException {
            if (vectorCount == maxDoc && sortMap == null) {
                return new FlushVectorOrdering(null, null);
            } else if (sortMap == null) {
                final DocIdSetIterator iterator = fieldVectorsWriter.getDocsWithFieldSet().iterator();
                final int[] docIds = new int[vectorCount];
                for (int i = 0; i < docIds.length; i++) {
                    docIds[i] = iterator.nextDoc();
                }
                assert iterator.nextDoc() == NO_MORE_DOCS;
                return new FlushVectorOrdering(docIds, null);
            } else {
                DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
                final int[] ordMap = new int[fieldVectorsWriter.getDocsWithFieldSet().cardinality()];
                KnnVectorsWriter.mapOldOrdToNewOrd(fieldVectorsWriter.getDocsWithFieldSet(), sortMap, null, ordMap, newDocsWithField);
                final DocIdSetIterator iterator = newDocsWithField.iterator();
                final int[] docIds = new int[vectorCount];
                for (int i = 0; i < docIds.length; i++) {
                    docIds[i] = iterator.nextDoc();
                }
                assert iterator.nextDoc() == NO_MORE_DOCS;
                return new FlushVectorOrdering(docIds, ordMap);
            }
        }
    }

    private static KMeansFloatVectorValues getKMeansFloatVectorValues(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<float[]> fieldVectorsWriter,
        int maxDoc,
        Sorter.DocMap sortMap
    ) throws IOException {
        List<float[]> vectors = fieldVectorsWriter.getVectors();
        FlushVectorOrdering ordering = FlushVectorOrdering.compute(fieldVectorsWriter, maxDoc, vectors.size(), sortMap);
        if (ordering.ordMap() != null) {
            final int[] ordMap = ordering.ordMap();
            vectors = new AbstractList<>() {
                private final List<float[]> delegate = fieldVectorsWriter.getVectors();

                @Override
                public int size() {
                    return delegate.size();
                }

                @Override
                public float[] get(int index) {
                    return delegate.get(ordMap[index]);
                }
            };
        }
        return KMeansFloatVectorValues.build(vectors, ordering.docIds(), fieldInfo.getVectorDimension());
    }

    private static KMeansByteVectorValues getKMeansByteVectorValues(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<byte[]> fieldVectorsWriter,
        int maxDoc,
        Sorter.DocMap sortMap
    ) throws IOException {
        List<byte[]> vectors = fieldVectorsWriter.getVectors();
        FlushVectorOrdering ordering = FlushVectorOrdering.compute(fieldVectorsWriter, maxDoc, vectors.size(), sortMap);
        if (ordering.ordMap() != null) {
            final int[] ordMap = ordering.ordMap();
            vectors = new AbstractList<>() {
                private final List<byte[]> delegate = fieldVectorsWriter.getVectors();

                @Override
                public int size() {
                    return delegate.size();
                }

                @Override
                public byte[] get(int index) {
                    return delegate.get(ordMap[index]);
                }
            };
        }
        return KMeansByteVectorValues.build(vectors, ordering.docIds(), fieldInfo.getVectorDimension());
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
     * @param vectorValues       the vectors to summarize into a single centroid
     * @return a {@link CentroidAssignments} instance with one centroid and
     *         all vectors assigned to it
     */
    protected final CentroidInformation<float[]> buildFlatCentroidAssignments(FieldInfo fieldInfo, ClusteringVectorValues<?> vectorValues)
        throws IOException {
        int dimension = fieldInfo.getVectorDimension();
        int count = vectorValues.size();
        float[] centroid = new float[dimension];
        accumulateVectors(fieldInfo.getVectorEncoding(), vectorValues, centroid);
        for (int d = 0; d < dimension; d++) {
            centroid[d] /= count;
        }
        // For flat centroid assignments there is a single global centroid and no secondary centroid assignments
        return CentroidInformation.ofFloat(dimension, new float[][] { centroid }, new int[count], OverspillAssignments.NONE);
    }

    /**
     * Accumulates all vector components into a float buffer, dispatching to the appropriate
     * {@link CentroidOps} based on the field's vector encoding.
     */
    @SuppressWarnings("unchecked")
    private static void accumulateVectors(VectorEncoding encoding, ClusteringVectorValues<?> vectorValues, float[] accumulator)
        throws IOException {
        switch (encoding) {
            case BYTE -> CentroidOps.BYTE.accumulateAll((ClusteringVectorValues<byte[]>) vectorValues, accumulator);
            case FLOAT32 -> CentroidOps.FLOAT.accumulateAll((ClusteringVectorValues<float[]>) vectorValues, accumulator);
        }
    }

    /**
     * Widens byte vector values to float. Used when a byte field produces float centroids
     * (e.g., the flat threshold path with too few vectors for clustering) and the float
     * postings path needs {@link FloatVectorValues}. Also used as a fallback in the ESNext
     * merge path when float clustering is selected for byte fields.
     * <p>
     * Not used by legacy codecs — they skip byte IVF indexing entirely.
     * <p>
     * This method materializes all vectors on the heap. In practice, it is only called in bounded contexts:
     * during flush it is bounded by {@code flatVectorThreshold} (small vector count), and during merge
     * it is a no-op pass-through (the input is already {@link KMeansFloatVectorValues} for float fields).
     */
    protected static KMeansFloatVectorValues asFloatVectorValues(FieldInfo fieldInfo, ClusteringVectorValues<?> vectorValues)
        throws IOException {
        if (vectorValues instanceof KMeansFloatVectorValues fvv) {
            return fvv;
        } else if (vectorValues instanceof ClusteringByteVectorValues byteValues) {
            int size = byteValues.size();
            int dim = fieldInfo.getVectorDimension();
            List<float[]> widened = new ArrayList<>(size);
            int[] docs = new int[size];
            for (int i = 0; i < size; i++) {
                byte[] bv = byteValues.vectorValue(i);
                float[] fv = new float[dim];
                for (int d = 0; d < dim; d++) {
                    fv[d] = bv[d];
                }
                widened.add(fv);
                docs[i] = byteValues.ordToDoc(i);
            }
            return KMeansFloatVectorValues.build(widened, docs, dim);
        }
        throw new IllegalArgumentException("Unsupported vector values type: " + vectorValues.getClass());
    }

    @Override
    public final IORunnable mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        IvfSegmentConfig resolvedConfig = resolveMergeConfig(fieldInfo, mergeState);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)
            || (fieldInfo.getVectorEncoding().equals(VectorEncoding.BYTE) && supportsByteNative())) {
            mergeOneFieldIVF(fieldInfo, mergeState, resolvedConfig);
        } else {
            // we simply write information that the field is present but we don't do anything with it.
            writeMeta(fieldInfo, 0, 0, 0, 0, 0, null, 0, 0, 0, 0, IvfSegmentConfig.NONE, false);
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
        IvfSegmentConfig ivfSegmentConfig,
        boolean byteCentroids
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
            ivfSegmentConfig,
            byteCentroids
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
        IvfSegmentConfig ivfSegmentConfig,
        boolean byteCentroids
    ) throws IOException;

    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    private void mergeOneFieldIVF(FieldInfo fieldInfo, MergeState mergeState, IvfSegmentConfig resolvedConfig) throws IOException {
        final IvfSegmentConfig ivfSegmentConfig = resolvedConfig != null ? resolvedConfig : IvfSegmentConfig.NONE;
        final boolean isByte = fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
        final int numVectors;
        String tempRawVectorsFileName = null;
        String docsFileName = null;
        Preconditioner preconditioner;
        // Build vector values with random access by dumping vectors to a temporary file.
        // If the segment is not dense, doc IDs are written to a separate file.
        // For byte fields with supportsByteNative(), vectors are written as bytes;
        // otherwise byte IVF indexing is skipped entirely.
        try (
            IndexOutput vectorsOut = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "ivfvec_", IOContext.DEFAULT)
        ) {
            tempRawVectorsFileName = vectorsOut.getName();
            // TODO: we only want to write this once but we'll wind up doing it for every field with the same dim and blockdim
            preconditioner = inheritPreconditioner(fieldInfo, mergeState, ivfSegmentConfig);

            final KnnVectorValues mergedVectorValues;
            if (isByte) {
                mergedVectorValues = MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
            } else {
                mergedVectorValues = MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
            }
            // if the segment is dense, we don't need to do anything with docIds.
            boolean dense = mergedVectorValues.size() == mergeState.segmentInfo.maxDoc();
            final int vectorCount;
            try (
                IndexOutput docsOut = dense
                    ? null
                    : mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "ivfdoc_", IOContext.DEFAULT)
            ) {
                if (docsOut != null) {
                    docsFileName = docsOut.getName();
                }
                vectorCount = writeVectorValues(fieldInfo, docsOut, vectorsOut, mergedVectorValues, preconditioner);
                CodecUtil.writeFooter(vectorsOut);
                if (docsOut != null) {
                    CodecUtil.writeFooter(docsOut);
                }
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
            writeMeta(fieldInfo, 0, centroidOffset, 0, 0, 0, null, 0, 0, 0, 0, ivfSegmentConfig, false);
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
            final KMeansFloatVectorValues floatVectorValues;
            final KMeansByteVectorValues byteVectorValues;
            final ClusteringVectorValues<?> vectorValues;
            if (isByte) {
                floatVectorValues = null;
                byteVectorValues = KMeansByteVectorValues.build(vectors, docs, numVectors, fieldInfo.getVectorDimension());
                vectorValues = byteVectorValues;
            } else {
                byteVectorValues = null;
                floatVectorValues = getKMeansFloatVectorValues(fieldInfo, docs, vectors, numVectors);
                vectorValues = floatVectorValues;
            }

            final long centroidOffset;
            final long centroidLength;
            final long postingListOffset;
            final long postingListLength;
            final CentroidAssignments assignments;
            String centroidTempName = null;
            IndexOutput centroidTemp = null;
            boolean byteCentroidsWritten = false;
            try {
                // TODO do this better, we shouldn't have to write to a temp file, we should be able to
                // just from the merged vector values, the tricky part is the random access.
                centroidTemp = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "civf_", IOContext.DEFAULT);
                centroidTempName = centroidTemp.getName();
                CentroidInformation<?> centroidAssignments = calculateCentroids(fieldInfo, vectorValues, mergeState);
                // write the centroids to a temporary file so we are not holding them on heap
                if (supportsByteNative() && centroidAssignments.centroids() instanceof byte[][] byteCentroidArrays) {
                    // Write byte centroids to temp file
                    for (byte[] centroid : byteCentroidArrays) {
                        centroidTemp.writeBytes(centroid, centroid.length);
                    }
                    assignments = centroidAssignments.centroidAssignments();
                    byteCentroidsWritten = true;
                } else {
                    // Merge always produces float centroids for float fields
                    @SuppressWarnings("unchecked")
                    CentroidInformation<float[]> floatCentroidAssignments = (CentroidInformation<float[]>) centroidAssignments;
                    final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN);
                    for (float[] centroid : floatCentroidAssignments.centroids()) {
                        buffer.asFloatBuffer().put(centroid);
                        centroidTemp.writeBytes(buffer.array(), buffer.array().length);
                    }
                    assignments = centroidAssignments.centroidAssignments();
                    byteCentroidsWritten = false;
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
                    writeMeta(fieldInfo, 0, centroidOffset, 0, 0, 0, null, 0, 0, 0, 0, ivfSegmentConfig, false);
                    CodecUtil.writeFooter(centroidTemp);
                    IOUtils.close(centroidTemp);
                    return;
                }
                CodecUtil.writeFooter(centroidTemp);
                IOUtils.close(centroidTemp);

                try (IndexInput centroidsInput = mergeState.segmentInfo.dir.openInput(centroidTempName, IOContext.DEFAULT)) {
                    final CentroidSupplier centroidSupplier;
                    final CentroidOffsetAndLength centroidOffsetAndLength;

                    centroidSupplier = createCentroidSupplier(centroidsInput, assignments, fieldInfo);

                    // write initial centroid index (we might need to read it later for overspilling)
                    centroidOffset = ivfCentroids.alignFilePointer(Float.BYTES);
                    CI centroidIndex = writeCentroidIndex(centroidSupplier, assignments.assignments(), ivfCentroids);

                    // write posting lists
                    postingListOffset = ivfClusters.alignFilePointer(Float.BYTES);
                    centroidOffsetAndLength = buildAndWritePostingsLists(
                        fieldInfo,
                        centroidSupplier,
                        vectorValues,
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
                        ivfSegmentConfig,
                        byteCentroidsWritten && byteVectorValues != null
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

    private static int writeVectorValues(
        FieldInfo fieldInfo,
        IndexOutput docsOut,
        IndexOutput vectorsOut,
        KnnVectorValues mergedVectorValues,
        Preconditioner preconditioner
    ) throws IOException {
        int numVectors = 0;
        int dim = fieldInfo.getVectorDimension();
        VectorEncoding encoding = fieldInfo.getVectorEncoding();

        // Encoding-specific scratch buffers
        final ByteBuffer floatBuffer = encoding == VectorEncoding.FLOAT32
            ? ByteBuffer.allocate(dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN)
            : null;
        final float[] floatScratch = preconditioner != null ? new float[dim] : null;
        final byte[] byteScratch = (encoding == VectorEncoding.BYTE && preconditioner != null) ? new byte[dim] : null;

        final KnnVectorValues.DocIndexIterator iterator = mergedVectorValues.iterator();
        for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
            numVectors++;
            switch (encoding) {
                case BYTE -> {
                    byte[] vector = ((ByteVectorValues) mergedVectorValues).vectorValue(iterator.index());
                    if (preconditioner != null) {
                        preconditioner.applyTransformToBytes(vector, byteScratch, floatScratch);
                        vectorsOut.writeBytes(byteScratch, dim);
                    } else {
                        vectorsOut.writeBytes(vector, dim);
                    }
                }
                case FLOAT32 -> {
                    float[] vector = ((FloatVectorValues) mergedVectorValues).vectorValue(iterator.index());
                    if (preconditioner != null) {
                        preconditioner.applyTransform(vector, floatScratch);
                        floatBuffer.asFloatBuffer().put(floatScratch);
                    } else {
                        floatBuffer.asFloatBuffer().put(vector);
                    }
                    vectorsOut.writeBytes(floatBuffer.array(), floatBuffer.array().length);
                }
            }
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
