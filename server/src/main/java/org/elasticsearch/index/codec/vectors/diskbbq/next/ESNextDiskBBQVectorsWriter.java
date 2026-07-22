/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.WelfordVariance;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.CentroidOps;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringFloatVectorValuesSlice;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansNeighbors;
import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;
import org.elasticsearch.index.codec.vectors.cluster.KMeansWithOverspill;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidAssignments;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndex;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidInformation;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidSlices;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidSupplier;
import org.elasticsearch.index.codec.vectors.diskbbq.DiskBBQBulkWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.DocIdsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.FlatCentroidClusters;
import org.elasticsearch.index.codec.vectors.diskbbq.FlatCentroidIndexWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IntSorter;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfFlushConfigSource;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.codec.vectors.diskbbq.OverspillAssignments;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantizedVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.TieredMergeStrategy;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * Default implementation of {@link IVFVectorsWriter}. It uses {@link HierarchicalKMeans} algorithm to
 * partition the vector space, and then stores the centroids and posting list in a sequential
 * fashion.
 */
public class ESNextDiskBBQVectorsWriter extends IVFVectorsWriter<FlatCentroidIndexWriter.CentroidGroups> {
    private static final Logger logger = LogManager.getLogger(ESNextDiskBBQVectorsWriter.class);

    private final int vectorPerCluster;
    private final CentroidIndexFormat centroidIndexFormat;
    private final int centroidsPerParentCluster;
    private final QuantEncoding quantEncoding;
    private final TaskExecutor mergeExec;
    private final int numMergeWorkers;
    private final int blockDimension;
    private final boolean doPrecondition;
    // field for slicing, null for no slicing
    private final String sliceField;
    private final IvfFlushConfigSource flushConfigSource;
    private final IvfMergeConfigResolver mergeConfigResolver;

    public ESNextDiskBBQVectorsWriter(
        SegmentWriteState state,
        String rawVectorFormatName,
        boolean useDirectIOReads,
        FlatVectorsWriter rawVectorDelegate,
        CentroidIndexFormat centroidIndexFormat,
        QuantEncoding encoding,
        int vectorPerCluster,
        int centroidsPerParentCluster,
        TaskExecutor mergeExec,
        int numMergeWorkers,
        int blockDimension,
        boolean doPrecondition,
        int flatVectorThreshold,
        String sliceField,
        IvfFlushConfigSource flushConfigSource,
        IvfMergeConfigResolver mergeConfigResolver
    ) throws IOException {
        super(
            state,
            rawVectorFormatName,
            useDirectIOReads,
            rawVectorDelegate,
            ESNextDiskBBQVectorsFormat.VERSION_CURRENT,
            ESNextDiskBBQVectorsFormat.NAME,
            ESNextDiskBBQVectorsFormat.IVF_META_EXTENSION,
            ESNextDiskBBQVectorsFormat.CENTROID_EXTENSION,
            ESNextDiskBBQVectorsFormat.CLUSTER_EXTENSION,
            true,
            flatVectorThreshold
        );
        this.vectorPerCluster = vectorPerCluster;
        this.centroidIndexFormat = centroidIndexFormat;
        this.centroidsPerParentCluster = centroidsPerParentCluster;
        this.quantEncoding = encoding;
        this.mergeExec = mergeExec;
        this.numMergeWorkers = numMergeWorkers;
        this.blockDimension = blockDimension;
        this.doPrecondition = doPrecondition;
        this.sliceField = sliceField;
        this.flushConfigSource = flushConfigSource != null ? flushConfigSource : IvfFlushConfigSource.empty();
        this.mergeConfigResolver = mergeConfigResolver != null ? mergeConfigResolver : IvfMergeConfigResolver.useCodecDefault();
        if (sliceField != null) {
            Sort sort = state.segmentInfo.getIndexSort();
            if (sort == null || sort.getSort().length == 0) {
                throw new IllegalStateException("sliceField requires index sort");
            }
            SortField primary = sort.getSort()[0];
            if (sliceField.equals(primary.getField()) == false) {
                throw new IllegalStateException("sliceField must be primary index sort");
            }
            if (primary.getType() != SortField.Type.STRING) {
                throw new IllegalStateException("sliceField requires primary index sort");
            }
        }
    }

    @Override
    protected IvfSegmentConfig beginIvfFieldFlush(FieldInfo fieldInfo) throws IOException {
        IvfSegmentConfig codec = IvfSegmentConfig.fromCodecDefaults(centroidIndexFormat, quantEncoding, doPrecondition);
        return flushConfigSource.load(segmentWriteState, fieldInfo).orElse(codec);
    }

    @Override
    protected IvfSegmentConfig resolveMergeConfig(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        return mergeConfigResolver.resolve(
            fieldInfo,
            mergeState,
            IvfSegmentConfig.fromCodecDefaults(centroidIndexFormat, quantEncoding, doPrecondition)
        );
    }

    private static IvfSegmentConfig requireSegmentConfig(IvfSegmentConfig cfg) {
        return Objects.requireNonNull(cfg, "ivf segment config must not be null");
    }

    @Override
    protected Preconditioner inheritPreconditioner(FieldInfo fieldInfo, MergeState mergeState, IvfSegmentConfig fieldWritingContext)
        throws IOException {
        if (requireSegmentConfig(fieldWritingContext).usePrecondition()) {
            for (KnnVectorsReader reader : mergeState.knnVectorsReaders) {
                if (reader instanceof VectorPreconditioner vp) {
                    Preconditioner preconditioner = vp.getPreconditioner(fieldInfo);
                    if (preconditioner != null) {
                        return preconditioner;
                    }
                }
            }
            // else
            return createPreconditioner(fieldInfo.getVectorDimension(), fieldWritingContext);
        }
        return null;
    }

    @Override
    protected Preconditioner createPreconditioner(int dimension, IvfSegmentConfig ivfSegmentConfig) {
        if (requireSegmentConfig(ivfSegmentConfig).usePrecondition()) {
            return Preconditioner.createPreconditioner(dimension, blockDimension);
        } else {
            return null;
        }
    }

    @Override
    protected void writePreconditioner(Preconditioner preconditioner, IndexOutput out) throws IOException {
        if (preconditioner != null) {
            preconditioner.write(out);
        }
    }

    @Override
    protected Consumer<List<float[]>> preconditionVectors(Preconditioner preconditioner, IvfSegmentConfig fieldWritingContext) {
        return (vectors) -> {
            if (requireSegmentConfig(fieldWritingContext).usePrecondition() == false || vectors.isEmpty()) {
                return;
            }
            if (preconditioner == null) {
                throw new IllegalStateException("preconditioner was not created but should be first");
            }
            float[] out = new float[vectors.getFirst().length];
            for (float[] vector : vectors) {
                preconditioner.applyTransform(vector, out);
                System.arraycopy(out, 0, vector, 0, vector.length);
            }
        };
    }

    @Override
    protected FloatVectorValues preconditionVectors(
        Preconditioner preconditioner,
        FloatVectorValues vectors,
        IvfSegmentConfig fieldWritingContext
    ) {
        if (requireSegmentConfig(fieldWritingContext).usePrecondition() == false) {
            return vectors;
        }
        if (preconditioner == null) {
            throw new IllegalStateException("preconditioner was not created but should be first");
        }

        // TODO: batch apply preconditioner for better performance and keep a batch on heap at a time
        return new FloatVectorValues() {
            final float[] preconditionedVectorValue = new float[vectors.dimension()];
            int cachedOrd = -1;

            @Override
            public int getVectorByteLength() {
                return vectors.getVectorByteLength();
            }

            @Override
            public float[] vectorValue(int ord) throws IOException {
                assert ord != -1;
                if (ord != cachedOrd) {
                    float[] vectorValue = vectors.vectorValue(ord);
                    preconditioner.applyTransform(vectorValue, this.preconditionedVectorValue);
                    cachedOrd = ord;
                }
                return this.preconditionedVectorValue;
            }

            @Override
            public FloatVectorValues copy() throws IOException {
                return vectors.copy();
            }

            @Override
            public int dimension() {
                return vectors.dimension();
            }

            @Override
            public int size() {
                return vectors.size();
            }

            @Override
            public DocIndexIterator iterator() {
                return vectors.iterator();
            }
        };
    }

    @Override
    public CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        IvfSegmentConfig fieldWritingContext
    ) throws IOException {
        final IvfSegmentConfig segmentConfig = requireSegmentConfig(fieldWritingContext);
        final QuantEncoding effectiveQuantEncoding = segmentConfig.quantEncoding();
        FlatCentroidClusters centroidClusters = (FlatCentroidClusters) centroidSupplier.centroidIndex();
        int[] centroidVectorCount = new int[centroidSupplier.size()];
        for (int i = 0; i < assignments.length; i++) {
            centroidVectorCount[assignments[i]]++;

            // if overspill assignments are present, count them as well
            for (var it = overspillAssignments.getAssignmentsFor(i); it.hasNext();) {
                centroidVectorCount[it.nextInt()]++;
            }
        }

        int maxPostingListSize = 0;
        int[][] assignmentsByCluster = new int[centroidSupplier.size()][];
        for (int c = 0; c < centroidSupplier.size(); c++) {
            int size = centroidVectorCount[c];
            maxPostingListSize = Math.max(maxPostingListSize, size);
            assignmentsByCluster[c] = new int[size];
        }
        Arrays.fill(centroidVectorCount, 0);

        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            assignmentsByCluster[c][centroidVectorCount[c]++] = i;

            // if overspill assignments are present, add them to the cluster as well
            for (var it = overspillAssignments.getAssignmentsFor(i); it.hasNext();) {
                int s = it.nextInt();
                assignmentsByCluster[s][centroidVectorCount[s]++] = i;
            }
        }
        // write the posting lists
        final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        final PackedLongValues.Builder lengths = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(effectiveQuantEncoding.bits(), BULK_SIZE, postingsOutput, true, true);
        OnHeapQuantizedVectors onHeapQuantizedVectors = new OnHeapQuantizedVectors(
            floatVectorValues,
            fieldInfo.getVectorSimilarityFunction(),
            effectiveQuantEncoding,
            fieldInfo.getVectorDimension(),
            new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction())
        );
        final int[] docIds = new int[maxPostingListSize];
        final int[] docDeltas = new int[maxPostingListSize];
        final int[] clusterOrds = new int[maxPostingListSize];
        DocIdsWriter idsWriter = new DocIdsWriter();
        for (int c = 0; c < centroidSupplier.size(); c++) {
            float[] centroid = centroidSupplier.centroid(c);
            int[] cluster = assignmentsByCluster[c];
            long offset = postingsOutput.alignFilePointer(Float.BYTES) - fileOffset;
            offsets.add(offset);
            postingsOutput.writeInt(Float.floatToIntBits(ESVectorUtil.squareDistance(centroid, centroidClusters.getCentroid(c))));
            int size = cluster.length;
            // write docIds
            postingsOutput.writeVInt(size);
            for (int j = 0; j < size; j++) {
                docIds[j] = floatVectorValues.ordToDoc(cluster[j]);
                clusterOrds[j] = j;
            }
            // sort cluster.buffer by docIds values, this way cluster ordinals are sorted by docIds
            new IntSorter(clusterOrds, i -> docIds[i]).sort(0, size);
            // encode doc deltas
            for (int j = 0; j < size; j++) {
                docDeltas[j] = j == 0 ? docIds[clusterOrds[j]] : docIds[clusterOrds[j]] - docIds[clusterOrds[j - 1]];
            }
            onHeapQuantizedVectors.reset(centroid, centroidClusters.getCentroid(c), size, ord -> cluster[clusterOrds[ord]]);
            byte encoding = idsWriter.calculateBlockEncoding(i -> docDeltas[i], size, BULK_SIZE);
            postingsOutput.writeByte(encoding);
            if (sliceField != null) {
                // We are not writing the docIds as we know they are writing in vector ord order.
                // we will ise the delegated FloatVectorValue instance on read to do the translation for us.
                assert centroidSupplier.size() == 1;
                bulkWriter.writeVectors(onHeapQuantizedVectors, null);
            } else {
                bulkWriter.writeVectors(onHeapQuantizedVectors, i -> {
                    // for vector i we write `bulk` size docs or the remaining docs
                    idsWriter.writeDocIds(d -> docDeltas[i + d], Math.min(BULK_SIZE, size - i), encoding, postingsOutput);
                });
            }
            lengths.add(postingsOutput.getFilePointer() - fileOffset - offset);
        }

        if (logger.isDebugEnabled()) {
            printClusterQualityStatistics(assignmentsByCluster);
        }

        return new CentroidOffsetAndLength(offsets.build(), lengths.build());
    }

    @Override
    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    public CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        MergeState mergeState,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        IvfSegmentConfig fieldWritingContext
    ) throws IOException {
        final IvfSegmentConfig segmentConfig = requireSegmentConfig(fieldWritingContext);
        final QuantEncoding effectiveQuantEncoding = segmentConfig.quantEncoding();
        // first, quantize all the vectors into a temporary file
        var vectorSimilarityFunction = fieldInfo.getVectorSimilarityFunction();
        FlatCentroidClusters centroidClusters = (FlatCentroidClusters) centroidSupplier.centroidIndex();
        PackedLongValues.Builder vectorCentroidOffsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        String quantizedVectorsTempName = null;
        try (
            IndexOutput quantizedVectorsTemp = mergeState.segmentInfo.dir.createTempOutput(
                mergeState.segmentInfo.name,
                "qvec_",
                IOContext.DEFAULT
            )
        ) {
            quantizedVectorsTempName = quantizedVectorsTemp.getName();
            OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(vectorSimilarityFunction);
            int[] quantized = new int[effectiveQuantEncoding.discretizedDimensions(fieldInfo.getVectorDimension())];
            byte[] binary = new byte[effectiveQuantEncoding.getDocPackedLength(fieldInfo.getVectorDimension())];
            float[] scratch = new float[fieldInfo.getVectorDimension()];
            for (int i = 0; i < assignments.length; i++) {
                // record where this vector's centroid data starts
                vectorCentroidOffsets.add(quantizedVectorsTemp.getFilePointer());
                int c = assignments[i];
                float[] centroid = centroidSupplier.centroid(c);
                float[] parentCentroid = centroidClusters.getCentroid(c);
                float[] vector = floatVectorValues.vectorValue(i);
                OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(
                    vector,
                    scratch,
                    quantized,
                    effectiveQuantEncoding.bits(),
                    centroid
                );
                if (parentCentroid != null) {
                    float additionalCorrection = switch (vectorSimilarityFunction) {
                        case EUCLIDEAN -> ESVectorUtil.squareDistance(vector, parentCentroid);
                        case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> ESVectorUtil.dotProduct(scratch, parentCentroid);
                        default -> throw new AssertionError(vectorSimilarityFunction);
                    };
                    result = new OptimizedScalarQuantizer.QuantizationResult(
                        result.lowerInterval(),
                        result.upperInterval(),
                        additionalCorrection,
                        result.quantizedComponentSum()
                    );
                }
                effectiveQuantEncoding.pack(quantized, binary);
                writeQuantizedValue(quantizedVectorsTemp, binary, result);

                // write the overspill vectors immediately afterwards
                for (var it = overspillAssignments.getAssignmentsFor(i); it.hasNext();) {
                    int s = it.nextInt();
                    float[] overspillCentroid = centroidSupplier.centroid(s);
                    float[] overspillParentCentroid = centroidClusters.getCentroid(s);
                    result = quantizer.scalarQuantize(vector, scratch, quantized, effectiveQuantEncoding.bits(), overspillCentroid);
                    if (overspillParentCentroid != null) {
                        float additionalCorrection = switch (vectorSimilarityFunction) {
                            case EUCLIDEAN -> ESVectorUtil.squareDistance(vector, overspillParentCentroid);
                            case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> ESVectorUtil.dotProduct(scratch, overspillParentCentroid);
                            default -> throw new AssertionError(vectorSimilarityFunction);
                        };
                        result = new OptimizedScalarQuantizer.QuantizationResult(
                            result.lowerInterval(),
                            result.upperInterval(),
                            additionalCorrection,
                            result.quantizedComponentSum()
                        );
                    }
                    effectiveQuantEncoding.pack(quantized, binary);
                    writeQuantizedValue(quantizedVectorsTemp, binary, result);
                }
            }
        } catch (Throwable t) {
            if (quantizedVectorsTempName != null) {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, quantizedVectorsTempName);
            }
            throw t;
        }
        int[] centroidVectorCount = new int[centroidSupplier.size()];
        for (int i = 0; i < assignments.length; i++) {
            centroidVectorCount[assignments[i]]++;

            // if overspill assignments are present, count them as well
            for (var it = overspillAssignments.getAssignmentsFor(i); it.hasNext();) {
                centroidVectorCount[it.nextInt()]++;
            }
        }

        int maxPostingListSize = 0;
        // centroid id -> array of vector ids for that centroid
        int[][] assignmentsByCluster = new int[centroidSupplier.size()][];
        // centroid id -> index of centroid in the vector's centroid data in the temporary file for vector i in assignmentsByCluster
        int[][] overspillVectorIdx = new int[centroidSupplier.size()][];
        for (int c = 0; c < centroidSupplier.size(); c++) {
            int size = centroidVectorCount[c];
            maxPostingListSize = Math.max(maxPostingListSize, size);
            assignmentsByCluster[c] = new int[size];
            overspillVectorIdx[c] = new int[size];
        }
        Arrays.fill(centroidVectorCount, 0);

        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            assignmentsByCluster[c][centroidVectorCount[c]++] = i;

            // if overspill assignments are present, add them to the cluster as well
            int vIdx = 1;  // don't need to set for the main centroid, it's initialized to 0 anyway
            for (var it = overspillAssignments.getAssignmentsFor(i); it.hasNext(); vIdx++) {
                int s = it.nextInt();
                assignmentsByCluster[s][centroidVectorCount[s]] = i;
                overspillVectorIdx[s][centroidVectorCount[s]] = vIdx;
                centroidVectorCount[s]++;
            }
        }
        // now we can read the quantized vectors from the temporary file
        try (IndexInput quantizedVectorsInput = mergeState.segmentInfo.dir.openInput(quantizedVectorsTempName, IOContext.DEFAULT)) {
            final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
            final PackedLongValues.Builder lengths = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
            OffHeapQuantizedVectors offHeapQuantizedVectors = new OffHeapQuantizedVectors(
                quantizedVectorsInput,
                effectiveQuantEncoding,
                fieldInfo.getVectorDimension(),
                vectorCentroidOffsets.build()
            );
            DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(
                effectiveQuantEncoding.bits(),
                BULK_SIZE,
                postingsOutput,
                true,
                true
            );
            // write the posting lists
            final int[] docIds = new int[maxPostingListSize];
            final int[] docDeltas = new int[maxPostingListSize];
            final int[] clusterOrds = new int[maxPostingListSize];
            DocIdsWriter idsWriter = new DocIdsWriter();
            for (int c = 0; c < centroidSupplier.size(); c++) {
                float[] centroid = centroidSupplier.centroid(c);
                int[] cluster = assignmentsByCluster[c];
                int[] vectorCentroidIdx = overspillVectorIdx[c];
                long offset = postingsOutput.alignFilePointer(Float.BYTES) - fileOffset;
                offsets.add(offset);
                postingsOutput.writeInt(Float.floatToIntBits(ESVectorUtil.squareDistance(centroid, centroidClusters.getCentroid(c))));
                // write docIds
                int size = cluster.length;
                postingsOutput.writeVInt(size);
                for (int j = 0; j < size; j++) {
                    docIds[j] = floatVectorValues.ordToDoc(cluster[j]);
                    clusterOrds[j] = j;
                }
                // sort cluster.buffer by docIds values, this way cluster ordinals are sorted by docIds
                new IntSorter(clusterOrds, i -> docIds[i]).sort(0, size);
                // encode doc deltas
                for (int j = 0; j < size; j++) {
                    docDeltas[j] = j == 0 ? docIds[clusterOrds[j]] : docIds[clusterOrds[j]] - docIds[clusterOrds[j - 1]];
                }
                byte encoding = idsWriter.calculateBlockEncoding(i -> docDeltas[i], size, BULK_SIZE);
                postingsOutput.writeByte(encoding);
                offHeapQuantizedVectors.reset(size, ord -> vectorCentroidIdx[clusterOrds[ord]], ord -> cluster[clusterOrds[ord]]);
                // write vectors
                bulkWriter.writeVectors(offHeapQuantizedVectors, i -> {
                    // for vector i we write `bulk` size docs or the remaining docs
                    idsWriter.writeDocIds(d -> docDeltas[d + i], Math.min(BULK_SIZE, size - i), encoding, postingsOutput);
                });
                lengths.add(postingsOutput.getFilePointer() - fileOffset - offset);
            }

            if (logger.isDebugEnabled()) {
                printClusterQualityStatistics(assignmentsByCluster);
            }
            return new CentroidOffsetAndLength(offsets.build(), lengths.build());
        } finally {
            org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, quantizedVectorsTempName);
        }
    }

    private static void printClusterQualityStatistics(int[][] clusters) {
        int[] sizes = new int[clusters.length];
        for (int i = 0; i < clusters.length; i++) {
            sizes[i] = clusters[i] == null ? 0 : clusters[i].length;
        }
        printClusterQualityStatistics(sizes);
    }

    private static void printClusterQualityStatistics(int[] clusterSizes) {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        WelfordVariance clusterSizeStats = new WelfordVariance();
        for (int size : clusterSizes) {
            clusterSizeStats.add(size);
            min = Math.min(min, size);
            max = Math.max(max, size);
        }
        double variance = clusterSizeStats.m2() / (clusterSizes.length - 1);
        logger.debug(
            "Centroid count: {} min: {} max: {} mean: {} stdDev: {} variance: {}",
            clusterSizes.length,
            min,
            max,
            clusterSizeStats.mean(),
            Math.sqrt(variance),
            variance
        );
    }

    @Override
    public CentroidSupplier createCentroidSupplier(IndexInput centroidsInput, CentroidAssignments centroidAssignments, FieldInfo fieldInfo)
        throws IOException {
        int numCentroids = centroidAssignments.numCentroids();
        float[] globalCentroid = centroidAssignments.globalCentroid();
        CentroidSlices centroidSlices = centroidAssignments.centroidSlices();

        CentroidSupplier centroidSupplier = new OffHeapCentroidSupplier(
            centroidsInput,
            numCentroids,
            fieldInfo,
            KMeansResult.singleCluster(globalCentroid, numCentroids),
            centroidSlices
        );
        if (centroidSupplier.size() > centroidsPerParentCluster * centroidsPerParentCluster) {
            ClusteringFloatVectorValues floatVectorValues = centroidSupplier.asKmeansFloatVectorValues();
            if (centroidSlices == null) {
                KMeansResult<float[]> centroidClusters = buildSecondLevelClusters(fieldInfo, floatVectorValues, true);
                return new OffHeapCentroidSupplier(centroidsInput, numCentroids, fieldInfo, centroidClusters, null);
            } else {
                List<KMeansResult<float[]>> centroidClusters = new ArrayList<>(centroidSlices.sliceOffsets().length);
                int start = 0;
                for (int i = 0; i < centroidSlices.sliceOffsets().length; i++) {
                    final int offset = start;
                    start = centroidSlices.sliceOffsets()[i];
                    int count = start - offset;
                    ClusteringFloatVectorValues slice = new ClusteringFloatVectorValuesSlice(floatVectorValues, j -> offset + j, count);
                    KMeansResult<float[]> result = buildSecondLevelClusters(fieldInfo, slice, true);
                    centroidClusters.add(result);
                    if (i == 0) {
                        centroidSlices.sliceOffsets()[i] = result.centroids().length;
                    } else {
                        centroidSlices.sliceOffsets()[i] = centroidSlices.sliceOffsets()[i - 1] + result.centroids().length;
                    }
                }
                KMeansResult<float[]> result = KMeansResult.merge(centroidClusters, CentroidOps.FLOAT);
                assert CentroidSlices.assertSliceOffsets(centroidSlices.sliceOffsets(), result.centroids().length);
                return new OffHeapCentroidSupplier(centroidsInput, numCentroids, fieldInfo, result, centroidSlices);
            }
        }
        return centroidSupplier;
    }

    @Override
    public CentroidSupplier createCentroidSupplier(FieldInfo info, float[][] centroids, float[] globalCentroid) throws IOException {
        CentroidSupplier centroidSupplier = CentroidSupplier.fromArray(
            centroids,
            new FlatCentroidClusters(KMeansResult.singleCluster(globalCentroid, centroids.length)),
            info.getVectorDimension()
        );
        if (centroidSupplier.size() > centroidsPerParentCluster * centroidsPerParentCluster) {
            KMeansResult<float[]> centroidClusters = buildSecondLevelClusters(info, centroidSupplier.asKmeansFloatVectorValues(), false);
            return CentroidSupplier.fromArray(centroids, new FlatCentroidClusters(centroidClusters), info.getVectorDimension());
        }
        return centroidSupplier;
    }

    private KMeansResult<float[]> buildSecondLevelClusters(
        FieldInfo fieldInfo,
        ClusteringFloatVectorValues floatVectorValues,
        boolean isMerge
    ) throws IOException {
        // we use the HierarchicalKMeans to partition the space of all vectors across merging segments
        // this are small numbers so we run it wih all the centroids.
        HierarchicalKMeans<float[]> hierarchicalKMeans;
        if (isMerge && mergeExec != null) {
            hierarchicalKMeans = HierarchicalKMeans.ofConcurrent(
                CentroidOps.FLOAT,
                fieldInfo.getVectorDimension(),
                mergeExec,
                numMergeWorkers,
                HierarchicalKMeans.MAX_ITERATIONS_DEFAULT,
                HierarchicalKMeans.SAMPLES_PER_CLUSTER_DEFAULT,
                HierarchicalKMeans.MAXK
            );
        } else {
            hierarchicalKMeans = HierarchicalKMeans.ofSerial(
                CentroidOps.FLOAT,
                fieldInfo.getVectorDimension(),
                HierarchicalKMeans.MAX_ITERATIONS_DEFAULT,
                HierarchicalKMeans.SAMPLES_PER_CLUSTER_DEFAULT,
                HierarchicalKMeans.MAXK
            );
        }
        return hierarchicalKMeans.cluster(floatVectorValues, centroidsPerParentCluster).result();
    }

    @Override
    protected void doWriteMeta(
        IndexOutput metaOutput,
        FieldInfo field,
        int numCentroids,
        long preconditionerOffset,
        long preconditionerLength,
        int numberOfSlices,
        int maxSliceSize,
        IvfSegmentConfig ivfSegmentConfig
    ) throws IOException {
        final IvfSegmentConfig segmentConfig = requireSegmentConfig(ivfSegmentConfig);
        metaOutput.writeInt(ES940OSQVectorsScorer.BULK_SIZE);
        metaOutput.writeInt(segmentConfig.centroidIndexFormat().id());
        metaOutput.writeInt(segmentConfig.quantEncoding().id());
        metaOutput.writeLong(preconditionerLength);
        if (preconditionerLength > 0) {
            metaOutput.writeLong(preconditionerOffset);
        }
        if (sliceField == null) {
            assert numberOfSlices == 0;
            metaOutput.writeInt(-1);
        } else {
            metaOutput.writeInt(numberOfSlices);
            if (numberOfSlices > 0) {
                metaOutput.writeVInt(maxSliceSize);
            }
        }
        metaOutput.writeInt(Float.floatToIntBits(segmentConfig.rescoreOversample()));
    }

    @Override
    protected FlatCentroidIndexWriter.CentroidGroups writeCentroidIndex(
        CentroidSupplier centroidSupplier,
        int[] centroidAssignments,
        IndexOutput centroidOutput
    ) throws IOException {
        return switch (centroidIndexFormat) {
            case FLAT -> FlatCentroidIndexWriter.writeCentroidIndex(centroidSupplier, centroidAssignments, centroidOutput);
        };
    }

    @Override
    protected void writeCentroidData(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        FlatCentroidIndexWriter.CentroidGroups centroidGroups,
        IndexOutput centroidOutput
    ) throws IOException {
        switch (centroidIndexFormat) {
            case FLAT -> FlatCentroidIndexWriter.writeCentroidData(
                fieldInfo,
                centroidSupplier,
                globalCentroid,
                centroidOffsetAndLength,
                centroidGroups,
                centroidOutput
            );
        }
    }

    @Override
    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#closeWhileHandlingException(...)")
    public CentroidInformation calculateCentroids(FieldInfo fieldInfo, KMeansFloatVectorValues floatVectorValues, MergeState mergeState)
        throws IOException {
        // Sliced indices treat each slice as an independent partition that must be clustered on its
        // own. The tiered merge strategy operates on the merged segment as a flat whole, which would
        // silently collapse slice boundaries, so always fall back to the sliced full rebuild here.
        // TODO: teach the tiered strategy about slices and reuse per-slice priors.
        if (sliceField != null) {
            return calculateCentroidsFullRebuildSliced(floatVectorValues, fieldInfo, mergeState);
        }

        // Gather prior segment statistics for tiered merge strategy selection
        int numSegments = mergeState.knnVectorsReaders.length;
        int[] segmentSizes = new int[numSegments];
        int[] segmentCentroidCounts = new int[numSegments];
        IVFVectorsReader.CentroidData[] segmentCentroidData = new IVFVectorsReader.CentroidData[numSegments];

        try {
            for (int i = 0; i < numSegments; i++) {
                KnnVectorsReader reader = mergeState.knnVectorsReaders[i];
                if (reader instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldReader) {
                    reader = perFieldReader.getFieldReader(fieldInfo.name);
                }
                if (reader instanceof IVFVectorsReader<?> ivfReader && mergeState.fieldInfos[i].fieldInfo(fieldInfo.name) != null) {
                    segmentSizes[i] = ivfReader.getFloatVectorValues(fieldInfo.name).size();
                    segmentCentroidData[i] = ivfReader.readCentroidData(fieldInfo.name);
                    segmentCentroidCounts[i] = segmentCentroidData[i] != null ? segmentCentroidData[i].numCentroids() : 0;
                } else {
                    segmentSizes[i] = 0;
                    segmentCentroidCounts[i] = 0;
                }
            }

            // Select merge strategy
            TieredMergeStrategy<float[]> tieredStrategy = new TieredMergeStrategy<>(vectorPerCluster, CentroidOps.FLOAT);
            TieredMergeStrategy.MergeAction<float[]> action = tieredStrategy.selectAction(
                segmentSizes,
                segmentCentroidCounts,
                segmentCentroidData
            );

            if (logger.isDebugEnabled()) {
                int totalVectors = 0;
                int totalCentroids = 0;
                for (int s : segmentSizes) {
                    totalVectors += s;
                }
                for (int c : segmentCentroidCounts) {
                    totalCentroids += c;
                }
                logger.debug(
                    "DiskBBQ merge for field [{}]: selected strategy [{}], segments={}, totalVectors={}, totalCentroids={}",
                    fieldInfo.name,
                    action.strategy(),
                    numSegments,
                    totalVectors,
                    totalCentroids
                );
            }

            HierarchicalKMeans<float[]> hierarchicalKMeans;
            if (mergeExec != null) {
                hierarchicalKMeans = HierarchicalKMeans.ofConcurrent(
                    CentroidOps.FLOAT,
                    floatVectorValues.dimension(),
                    mergeExec,
                    numMergeWorkers
                );
            } else {
                hierarchicalKMeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, floatVectorValues.dimension());
            }
            KMeansWithOverspill<float[]> kMeansResult = action.execute(hierarchicalKMeans, floatVectorValues, vectorPerCluster);
            if (logger.isDebugEnabled()) {
                int[] clusterSizes = new int[kMeansResult.centroids().length];
                for (int a : kMeansResult.assignments()) {
                    clusterSizes[a]++;
                }
                printClusterQualityStatistics(clusterSizes);
            }

            // TODO: swap out SOAR for SRAIR when HNSW graphs are used for the centroids
            return new CentroidInformation(
                fieldInfo.getVectorDimension(),
                kMeansResult.centroids(),
                kMeansResult.assignments(),
                kMeansResult.overspill()
            );
        } finally {
            // CentroidData owns the IndexInput backing the streaming centroid view; close once
            // the clustering pass has consumed it (and on any failure mid-way).
            org.apache.lucene.util.IOUtils.closeWhileHandlingException(segmentCentroidData);
        }
    }

    private CentroidInformation calculateCentroidsFullRebuildSliced(
        KMeansFloatVectorValues floatVectorValues,
        FieldInfo fieldInfo,
        MergeState mergeState
    ) throws IOException {
        HierarchicalKMeans<float[]> hierarchicalKMeans;
        if (mergeExec != null) {
            hierarchicalKMeans = HierarchicalKMeans.ofConcurrent(
                CentroidOps.FLOAT,
                floatVectorValues.dimension(),
                mergeExec,
                numMergeWorkers
            );
        } else {
            hierarchicalKMeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, floatVectorValues.dimension());
        }
        final FieldInfo slicedFieldInfo = mergeState.mergeFieldInfos.fieldInfo(sliceField);
        assert slicedFieldInfo != null;
        assert slicedFieldInfo.getDocValuesType() == DocValuesType.SORTED : "sliceField must be SortedDocValues";
        final SortedDocValues values = DocValueConsumerHelper.INSTANCE.getMergeSortedField(slicedFieldInfo, mergeState);
        final int numSlices = values.getValueCount();
        final KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
        iterator.advance(0);
        values.nextDoc();
        // slice field must be dense populated, but we might have documents without a vector.
        final int[] sliceOffsets = new int[numSlices];
        final int[] sliceLengths = new int[numSlices];
        List<KMeansWithOverspill<float[]>> kmeansResults = new ArrayList<>();
        for (int i = 0; i < numSlices; i++) {
            if (iterator.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                // no more vectors, we are done
                sliceLengths[i] = 0;
                sliceOffsets[i] = i == 0 ? 0 : sliceOffsets[i - 1];
                continue;
            }
            // get start and end of an slice
            int sliceDocStart = values.docID();
            while (values.docID() != DocIdSetIterator.NO_MORE_DOCS && values.ordValue() == i) {
                values.nextDoc();
            }
            final int sliceDocEnd = values.docID();
            // get the vector ordinals for the slice
            int vectorDocStart = iterator.docID();
            if (vectorDocStart < sliceDocStart) {
                // advance iterator to the beginning of the slice
                vectorDocStart = iterator.advance(sliceDocStart);
            }
            if (vectorDocStart > sliceDocEnd) {
                // no vectors in this slice
                sliceLengths[i] = 0;
                sliceOffsets[i] = i == 0 ? 0 : sliceOffsets[i - 1];
                continue;
            }
            final int vectorOrdStart = iterator.index();
            final int docEnd = vectorDocStart == sliceDocEnd ? sliceDocEnd : iterator.advance(sliceDocEnd);
            final int vectorOrdEnd = docEnd == KnnVectorValues.DocIndexIterator.NO_MORE_DOCS ? floatVectorValues.size() : iterator.index();
            final int sliceNumVectors = vectorOrdEnd - vectorOrdStart;
            final ClusteringFloatVectorValuesSlice slice = new ClusteringFloatVectorValuesSlice(
                floatVectorValues,
                j -> vectorOrdStart + j,
                sliceNumVectors
            );
            final KMeansNeighbors<float[]> kMeansResult = hierarchicalKMeans.cluster(slice, vectorPerCluster);
            final OverspillAssignments overspill = hierarchicalKMeans.computeSoar(
                slice,
                kMeansResult.result(),
                kMeansResult.neighborHoods()
            );
            kmeansResults.add(new KMeansWithOverspill<>(kMeansResult.result(), overspill));
            sliceLengths[i] = sliceNumVectors;
            sliceOffsets[i] = i == 0 ? kMeansResult.centroids().length : sliceOffsets[i - 1] + kMeansResult.centroids().length;
        }
        final KMeansWithOverspill<float[]> merged = KMeansWithOverspill.merge(kmeansResults, CentroidOps.FLOAT);
        if (logger.isDebugEnabled()) {
            logger.debug("final centroid count: {}", merged.centroids().length);
        }
        final CentroidSlices centroidSlices = new CentroidSlices(sliceOffsets, sliceLengths);
        return new CentroidInformation(
            floatVectorValues.dimension(),
            merged.centroids(),
            merged.assignments(),
            merged.overspill(),
            centroidSlices
        );
    }

    // This class helps to access the merged view of a slice.
    private static class DocValueConsumerHelper extends DocValuesConsumer {

        static final DocValueConsumerHelper INSTANCE = new DocValueConsumerHelper();

        public SortedDocValues getMergeSortedField(FieldInfo fieldInfo, final MergeState mergeState) throws IOException {
            // This is the magic to get a merged view from the segments.
            final OrdinalMap map = createOrdinalMapForSortedDV(fieldInfo, mergeState);
            return getMergedSortedSetDocValues(fieldInfo, mergeState, map);
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void close() {
            throw new AssertionError("Method should not be called");
        }
    }

    @Override
    public CentroidInformation calculateCentroids(FieldInfo fieldInfo, KMeansFloatVectorValues floatVectorValues) throws IOException {
        if (sliceField != null) {
            // for sliced indexed, we don't cluster the data during flush so we can search our vectors by docId range
            return buildFlatCentroidAssignments(fieldInfo, floatVectorValues);
        }
        HierarchicalKMeans<float[]> hierarchicalKMeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, floatVectorValues.dimension());
        KMeansNeighbors<float[]> kMeansResult = hierarchicalKMeans.cluster(floatVectorValues, vectorPerCluster);
        OverspillAssignments soarOverspill = hierarchicalKMeans.computeSoar(
            floatVectorValues,
            kMeansResult.result(),
            kMeansResult.neighborHoods()
        );
        if (logger.isDebugEnabled()) {
            logger.debug("final centroid count: {}", kMeansResult.centroids().length);
        }

        // TODO: swap out SOAR for SRAIR when HNSW graphs are used for the centroids
        return new CentroidInformation(fieldInfo.getVectorDimension(), kMeansResult.centroids(), kMeansResult.assignments(), soarOverspill);
    }

    static void writeQuantizedValue(IndexOutput indexOutput, byte[] binaryValue, OptimizedScalarQuantizer.QuantizationResult corrections)
        throws IOException {
        indexOutput.writeBytes(binaryValue, binaryValue.length);
        indexOutput.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
        indexOutput.writeInt(Float.floatToIntBits(corrections.upperInterval()));
        indexOutput.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
        indexOutput.writeInt(corrections.quantizedComponentSum());
    }

    static class OffHeapCentroidSupplier implements CentroidSupplier {
        private final IndexInput centroidsInput;
        private final int numCentroids;
        private final int dimension;
        private final float[] scratch;
        private final KMeansResult<float[]> clusters;
        private final CentroidSlices centroidSlices;
        private int currOrd = -1;

        OffHeapCentroidSupplier(
            IndexInput centroidsInput,
            int numCentroids,
            FieldInfo info,
            KMeansResult<float[]> clusters,
            CentroidSlices centroidSlices
        ) {
            this.centroidsInput = centroidsInput;
            this.numCentroids = numCentroids;
            this.dimension = info.getVectorDimension();
            this.scratch = new float[dimension];
            this.clusters = clusters;
            this.centroidSlices = centroidSlices;
        }

        @Override
        public int size() {
            return numCentroids;
        }

        @Override
        public float[] centroid(int centroidOrdinal) throws IOException {
            if (centroidOrdinal == currOrd) {
                return scratch;
            }
            centroidsInput.seek((long) centroidOrdinal * dimension * Float.BYTES);
            centroidsInput.readFloats(scratch, 0, dimension);
            this.currOrd = centroidOrdinal;
            return scratch;
        }

        @Override
        public CentroidIndex centroidIndex() {
            return new FlatCentroidClusters(clusters);
        }

        @Override
        public CentroidSlices slices() throws IOException {
            return centroidSlices;
        }

        @Override
        public KMeansFloatVectorValues asKmeansFloatVectorValues() throws IOException {
            return KMeansFloatVectorValues.build(centroidsInput, null, numCentroids, dimension);
        }
    }

    static class OnHeapQuantizedVectors implements QuantizedVectorValues {
        private final FloatVectorValues vectorValues;
        private final OptimizedScalarQuantizer quantizer;
        private final byte[] quantizedVector;
        private final int[] quantizedVectorScratch;
        private final float[] floatVectorScratch;
        private final QuantEncoding encoding;
        private OptimizedScalarQuantizer.QuantizationResult corrections;
        private final VectorSimilarityFunction similarityFunction;
        private float[] currentCentroid, currentParentCentroid;
        private IntToIntFunction ordTransformer = null;
        private int currOrd = -1;
        private int count;

        OnHeapQuantizedVectors(
            FloatVectorValues vectorValues,
            VectorSimilarityFunction similarityFunction,
            QuantEncoding encoding,
            int dimension,
            OptimizedScalarQuantizer quantizer
        ) {
            this.vectorValues = vectorValues;
            this.similarityFunction = similarityFunction;
            this.encoding = encoding;
            this.quantizer = quantizer;
            this.quantizedVector = new byte[encoding.getDocPackedLength(dimension)];
            this.floatVectorScratch = new float[dimension];
            this.quantizedVectorScratch = new int[encoding.discretizedDimensions(dimension)];
            this.corrections = null;
            this.currentParentCentroid = null;
        }

        private void reset(float[] centroid, float[] currentParentCentroid, int count, IntToIntFunction ordTransformer) {
            this.currentCentroid = centroid;
            this.ordTransformer = ordTransformer;
            this.currOrd = -1;
            this.count = count;
            this.currentParentCentroid = currentParentCentroid;
        }

        @Override
        public int count() {
            return count;
        }

        @Override
        public byte[] next() throws IOException {
            if (currOrd >= count() - 1) {
                throw new IllegalStateException("No more vectors to read, current ord: " + currOrd + ", count: " + count());
            }
            currOrd++;
            int ord = ordTransformer.apply(currOrd);
            float[] vector = vectorValues.vectorValue(ord);
            corrections = quantizer.scalarQuantize(vector, floatVectorScratch, quantizedVectorScratch, encoding.bits(), currentCentroid);
            // note, with a parent centroid, our correction needs to take it into account
            if (currentParentCentroid != null) {
                float additionalCorrection = switch (similarityFunction) {
                    case EUCLIDEAN -> ESVectorUtil.squareDistance(vector, currentParentCentroid);
                    case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> ESVectorUtil.dotProduct(floatVectorScratch, currentParentCentroid);
                    default -> throw new AssertionError(similarityFunction);
                };
                corrections = new OptimizedScalarQuantizer.QuantizationResult(
                    corrections.lowerInterval(),
                    corrections.upperInterval(),
                    additionalCorrection,
                    corrections.quantizedComponentSum()
                );
            }
            encoding.pack(quantizedVectorScratch, quantizedVector);
            return quantizedVector;
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrections() {
            if (currOrd == -1) {
                throw new IllegalStateException("No vector read yet, call next first");
            }
            return corrections;
        }
    }

    static class OffHeapQuantizedVectors implements QuantizedVectorValues {
        private final IndexInput quantizedVectorsInput;
        private final PackedLongValues vectorCentroidOffsets;
        private final byte[] binaryScratch;
        private final float[] corrections = new float[3];

        private final long vectorByteSize;
        private int bitSum;
        private int currOrd = -1;
        private int count;
        private IntToIntFunction overspillIdx = null;
        private IntToIntFunction ordTransformer = null;

        OffHeapQuantizedVectors(
            IndexInput quantizedVectorsInput,
            QuantEncoding encoding,
            int dimension,
            PackedLongValues vectorCentroidOffsets
        ) {
            this.quantizedVectorsInput = quantizedVectorsInput;
            this.vectorCentroidOffsets = vectorCentroidOffsets;
            this.binaryScratch = new byte[encoding.getDocPackedLength(dimension)];
            this.vectorByteSize = (binaryScratch.length + 3 * Float.BYTES + Integer.BYTES);
        }

        private void reset(int count, IntToIntFunction isOverspill, IntToIntFunction ordTransformer) {
            this.count = count;
            this.overspillIdx = isOverspill;
            this.ordTransformer = ordTransformer;
            this.currOrd = -1;
        }

        @Override
        public int count() {
            return count;
        }

        @Override
        public byte[] next() throws IOException {
            if (currOrd >= count - 1) {
                throw new IllegalStateException("No more vectors to read, current ord: " + currOrd + ", count: " + count);
            }
            currOrd++;
            int ord = ordTransformer.apply(currOrd);
            int centroidIdx = this.overspillIdx.apply(currOrd);
            return getVector(ord, centroidIdx);
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrections() {
            if (currOrd == -1) {
                throw new IllegalStateException("No vector read yet, call readQuantizedVector first");
            }
            return new OptimizedScalarQuantizer.QuantizationResult(corrections[0], corrections[1], corrections[2], bitSum);
        }

        byte[] getVector(int ord, int centroidIdx) throws IOException {
            readQuantizedVector(ord, centroidIdx);
            return binaryScratch;
        }

        public void readQuantizedVector(int ord, int centroidIdx) throws IOException {
            long offset = vectorCentroidOffsets.get(ord) + centroidIdx * vectorByteSize;
            quantizedVectorsInput.seek(offset);
            quantizedVectorsInput.readBytes(binaryScratch, 0, binaryScratch.length);
            quantizedVectorsInput.readFloats(corrections, 0, 3);
            bitSum = quantizedVectorsInput.readInt();
        }
    }
}
