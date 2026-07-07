/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.es94;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.WelfordVariance;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.CentroidOps;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;
import org.elasticsearch.index.codec.vectors.cluster.KMeansWithOverspill;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidAssignments;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndex;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidInformation;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidSupplier;
import org.elasticsearch.index.codec.vectors.diskbbq.DiskBBQBulkWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.DocIdsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.FlatCentroidClusters;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IntSorter;
import org.elasticsearch.index.codec.vectors.diskbbq.IntToBooleanFunction;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.codec.vectors.diskbbq.OverspillAssignments;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantizedVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.SoarAssignments;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;

import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * Default implementation of {@link IVFVectorsWriter}. It uses {@link HierarchicalKMeans} algorithm to
 * partition the vector space, and then stores the centroids and posting list in a sequential
 * fashion.
 */
public class ES940DiskBBQVectorsWriter extends IVFVectorsWriter<ES940DiskBBQVectorsWriter.CentroidGroups> {
    private static final Logger logger = LogManager.getLogger(ES940DiskBBQVectorsWriter.class);

    private final int vectorPerCluster;
    private final int centroidsPerParentCluster;
    private final ES940DiskBBQVectorsFormat.QuantEncoding quantEncoding;
    private final TaskExecutor mergeExec;
    private final int numMergeWorkers;
    private final int blockDimension;
    private final boolean doPrecondition;

    public ES940DiskBBQVectorsWriter(
        SegmentWriteState state,
        String rawVectorFormatName,
        boolean useDirectIOReads,
        FlatVectorsWriter rawVectorDelegate,
        ES940DiskBBQVectorsFormat.QuantEncoding encoding,
        int vectorPerCluster,
        int centroidsPerParentCluster,
        TaskExecutor mergeExec,
        int numMergeWorkers,
        int blockDimension,
        boolean doPrecondition,
        int flatVectorThreshold
    ) throws IOException {
        this(
            state,
            rawVectorFormatName,
            useDirectIOReads,
            rawVectorDelegate,
            encoding,
            vectorPerCluster,
            centroidsPerParentCluster,
            mergeExec,
            numMergeWorkers,
            blockDimension,
            doPrecondition,
            flatVectorThreshold,
            ES940DiskBBQVectorsFormat.VERSION_CURRENT
        );
    }

    ES940DiskBBQVectorsWriter(
        SegmentWriteState state,
        String rawVectorFormatName,
        boolean useDirectIOReads,
        FlatVectorsWriter rawVectorDelegate,
        ES940DiskBBQVectorsFormat.QuantEncoding encoding,
        int vectorPerCluster,
        int centroidsPerParentCluster,
        TaskExecutor mergeExec,
        int numMergeWorkers,
        int blockDimension,
        boolean doPrecondition,
        int flatVectorThreshold,
        int writeVersion
    ) throws IOException {
        super(
            state,
            rawVectorFormatName,
            useDirectIOReads,
            rawVectorDelegate,
            writeVersion,
            ES940DiskBBQVectorsFormat.NAME,
            ES940DiskBBQVectorsFormat.IVF_META_EXTENSION,
            ES940DiskBBQVectorsFormat.CENTROID_EXTENSION,
            ES940DiskBBQVectorsFormat.CLUSTER_EXTENSION,
            true,
            flatVectorThreshold
        );
        this.vectorPerCluster = vectorPerCluster;
        this.centroidsPerParentCluster = centroidsPerParentCluster;
        this.quantEncoding = encoding;
        this.mergeExec = mergeExec;
        this.numMergeWorkers = numMergeWorkers;
        this.blockDimension = blockDimension;
        this.doPrecondition = doPrecondition;
    }

    @Override
    protected Preconditioner inheritPreconditioner(FieldInfo fieldInfo, MergeState mergeState, IvfSegmentConfig ivfSegmentConfig)
        throws IOException {
        if (doPrecondition) {
            for (KnnVectorsReader reader : mergeState.knnVectorsReaders) {
                if (reader instanceof VectorPreconditioner) {
                    Preconditioner preconditioner = ((VectorPreconditioner) reader).getPreconditioner(fieldInfo);
                    if (preconditioner != null) {
                        return preconditioner;
                    }
                }
            }
            // else
            return createPreconditioner(fieldInfo.getVectorDimension(), ivfSegmentConfig);
        }
        return null;
    }

    @Override
    protected Preconditioner createPreconditioner(int dimension, IvfSegmentConfig ivfSegmentConfig) {
        if (doPrecondition) {
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
    protected Consumer<List<float[]>> preconditionVectors(Preconditioner preconditioner, IvfSegmentConfig ivfSegmentConfig) {
        return (vectors) -> {
            if (doPrecondition == false || vectors.isEmpty()) {
                return;
            }
            if (preconditioner == null) {
                throw new IllegalStateException("preconditioner was not created but should be first");
            }
            float[] out = new float[vectors.getFirst().length];
            for (int i = 0; i < vectors.size(); i++) {
                float[] vector = vectors.get(i);
                preconditioner.applyTransform(vector, out);
                System.arraycopy(out, 0, vector, 0, vector.length);
            }
        };
    }

    @Override
    protected FloatVectorValues preconditionVectors(
        Preconditioner preconditioner,
        FloatVectorValues vectors,
        IvfSegmentConfig ivfSegmentConfig
    ) {
        if (doPrecondition == false) {
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
        IvfSegmentConfig ivfSegmentConfig
    ) throws IOException {
        FlatCentroidClusters centroidClusters = (FlatCentroidClusters) centroidSupplier.centroidIndex();
        int[] centroidVectorCount = new int[centroidSupplier.size()];
        for (int i = 0; i < assignments.length; i++) {
            centroidVectorCount[assignments[i]]++;

            // if soar assignments are present, count them as well
            var overspills = overspillAssignments.getAssignmentsFor(i);
            if (overspills.hasNext()) {
                centroidVectorCount[overspills.nextInt()]++;
            }
            assert !overspills.hasNext();
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

            // if soar assignments are present, add them to the cluster as well
            var overspills = overspillAssignments.getAssignmentsFor(i);
            if (overspills.hasNext()) {
                int s = overspills.nextInt();
                assignmentsByCluster[s][centroidVectorCount[s]++] = i;
            }
            assert !overspills.hasNext();
        }
        // write the posting lists
        final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        final PackedLongValues.Builder lengths = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(quantEncoding.bits(), BULK_SIZE, postingsOutput, true, true);
        OnHeapQuantizedVectors onHeapQuantizedVectors = new OnHeapQuantizedVectors(
            floatVectorValues,
            fieldInfo.getVectorSimilarityFunction(),
            quantEncoding,
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
            bulkWriter.writeVectors(onHeapQuantizedVectors, i -> {
                // for vector i we write `bulk` size docs or the remaining docs
                idsWriter.writeDocIds(d -> docDeltas[i + d], Math.min(BULK_SIZE, size - i), encoding, postingsOutput);
            });
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
        IvfSegmentConfig ivfSegmentConfig
    ) throws IOException {
        // first, quantize all the vectors into a temporary file
        var vectorSimilarityFunction = fieldInfo.getVectorSimilarityFunction();
        FlatCentroidClusters centroidClusters = (FlatCentroidClusters) centroidSupplier.centroidIndex();
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
            int[] quantized = new int[quantEncoding.discretizedDimensions(fieldInfo.getVectorDimension())];
            byte[] binary = new byte[quantEncoding.getDocPackedLength(fieldInfo.getVectorDimension())];
            float[] scratch = new float[fieldInfo.getVectorDimension()];
            for (int i = 0; i < assignments.length; i++) {
                int c = assignments[i];
                float[] centroid = centroidSupplier.centroid(c);
                float[] parentCentroid = centroidClusters.getCentroid(c);
                float[] vector = floatVectorValues.vectorValue(i);
                OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(
                    vector,
                    scratch,
                    quantized,
                    quantEncoding.bits(),
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
                quantEncoding.pack(quantized, binary);
                writeQuantizedValue(quantizedVectorsTemp, binary, result);

                var overspills = overspillAssignments.getAssignmentsFor(i);
                if (overspills.hasNext()) {
                    int s = overspills.nextInt();
                    float[] overspillCentroid = centroidSupplier.centroid(s);
                    float[] overspillParentCentroid = centroidClusters.getCentroid(s);
                    // write the overspill vector as well
                    result = quantizer.scalarQuantize(vector, scratch, quantized, quantEncoding.bits(), overspillCentroid);
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
                    quantEncoding.pack(quantized, binary);
                    writeQuantizedValue(quantizedVectorsTemp, binary, result);
                    assert !overspills.hasNext();
                } else {
                    // write a zero vector for the overspill
                    Arrays.fill(binary, (byte) 0);
                    OptimizedScalarQuantizer.QuantizationResult zeroResult = new OptimizedScalarQuantizer.QuantizationResult(0f, 0f, 0f, 0);
                    writeQuantizedValue(quantizedVectorsTemp, binary, zeroResult);
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

            // if soar assignments are present, count them as well
            var overspills = overspillAssignments.getAssignmentsFor(i);
            if (overspills.hasNext()) {
                centroidVectorCount[overspills.nextInt()]++;
            }
        }

        int maxPostingListSize = 0;
        int[][] assignmentsByCluster = new int[centroidSupplier.size()][];
        boolean[][] isOverspillByCluster = new boolean[centroidSupplier.size()][];
        for (int c = 0; c < centroidSupplier.size(); c++) {
            int size = centroidVectorCount[c];
            maxPostingListSize = Math.max(maxPostingListSize, size);
            assignmentsByCluster[c] = new int[size];
            isOverspillByCluster[c] = new boolean[size];
        }
        Arrays.fill(centroidVectorCount, 0);

        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            assignmentsByCluster[c][centroidVectorCount[c]++] = i;

            // if soar assignments are present, add them to the cluster as well
            var overspills = overspillAssignments.getAssignmentsFor(i);
            if (overspills.hasNext()) {
                int s = overspills.nextInt();
                assignmentsByCluster[s][centroidVectorCount[s]] = i;
                isOverspillByCluster[s][centroidVectorCount[s]++] = true;
            }
        }
        // now we can read the quantized vectors from the temporary file
        try (IndexInput quantizedVectorsInput = mergeState.segmentInfo.dir.openInput(quantizedVectorsTempName, IOContext.DEFAULT)) {
            final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
            final PackedLongValues.Builder lengths = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
            OffHeapQuantizedVectors offHeapQuantizedVectors = new OffHeapQuantizedVectors(
                quantizedVectorsInput,
                quantEncoding.getDocPackedLength(fieldInfo.getVectorDimension())
            );
            DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(quantEncoding.bits(), BULK_SIZE, postingsOutput, true, true);
            // write the posting lists
            final int[] docIds = new int[maxPostingListSize];
            final int[] docDeltas = new int[maxPostingListSize];
            final int[] clusterOrds = new int[maxPostingListSize];
            DocIdsWriter idsWriter = new DocIdsWriter();
            for (int c = 0; c < centroidSupplier.size(); c++) {
                float[] centroid = centroidSupplier.centroid(c);
                int[] cluster = assignmentsByCluster[c];
                boolean[] isOverspill = isOverspillByCluster[c];
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
                offHeapQuantizedVectors.reset(size, ord -> isOverspill[clusterOrds[ord]], ord -> cluster[clusterOrds[ord]]);
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
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        WelfordVariance clusterSizeStats = new WelfordVariance();
        int observationNumber = 0;
        for (int[] cluster : clusters) {
            observationNumber += 1;
            if (cluster == null) {
                clusterSizeStats.advanceToObservation(observationNumber);
                continue;
            }
            clusterSizeStats.addAsObservation(cluster.length, observationNumber);
            min = Math.min(min, cluster.length);
            max = Math.max(max, cluster.length);
        }
        double variance = clusterSizeStats.m2() / (clusters.length - 1);
        logger.debug(
            "Centroid count: {} min: {} max: {} mean: {} stdDev: {} variance: {}",
            clusters.length,
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
        CentroidSupplier centroidSupplier = new OffHeapCentroidSupplier(
            centroidsInput,
            centroidAssignments.numCentroids(),
            fieldInfo,
            KMeansResult.singleCluster(centroidAssignments.globalCentroid(), centroidAssignments.numCentroids())
        );
        if (centroidSupplier.size() > centroidsPerParentCluster * centroidsPerParentCluster) {
            KMeansResult<float[]> centroidClusters = buildSecondLevelClusters(fieldInfo, centroidSupplier, true);
            return new OffHeapCentroidSupplier(centroidsInput, centroidAssignments.numCentroids(), fieldInfo, centroidClusters);
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
            KMeansResult<float[]> centroidClusters = buildSecondLevelClusters(info, centroidSupplier, false);
            return CentroidSupplier.fromArray(centroids, new FlatCentroidClusters(centroidClusters), info.getVectorDimension());
        }
        return centroidSupplier;
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
        metaOutput.writeInt(ES940OSQVectorsScorer.BULK_SIZE);
        metaOutput.writeInt(quantEncoding.id());
        metaOutput.writeLong(preconditionerLength);
        if (preconditionerLength > 0) {
            metaOutput.writeLong(preconditionerOffset);
        }
    }

    record CentroidGroups(float[][] centroids, int[][] vectors, int maxVectorsPerCentroidLength) {}

    @Override
    protected CentroidGroups writeCentroidIndex(CentroidSupplier centroidSupplier, int[] centroidAssignments, IndexOutput centroidOutput)
        throws IOException {
        if (centroidSupplier.centroidIndex().hasData()) {
            final CentroidGroups centroidGroups = buildCentroidGroups((FlatCentroidClusters) centroidSupplier.centroidIndex());
            // write vector ord -> centroid lookup table. We need to remap current centroid ordinals
            // to the ordinals on the parent / child structure.
            final int[] centroidOrdinalMap = new int[centroidSupplier.size()];
            int idx = 0;
            for (int[] centroidVectors : centroidGroups.vectors()) {
                for (int assignment : centroidVectors) {
                    centroidOrdinalMap[assignment] = idx++;
                }
            }
            assert idx == centroidSupplier.size() : "Expected [" + centroidSupplier.size() + "], got [" + idx + "]";
            writeCentroidLookup(centroidOutput, centroidAssignments, i -> centroidOrdinalMap[i], centroidSupplier.size());
            return centroidGroups;
        } else {
            writeCentroidLookup(centroidOutput, centroidAssignments, IntUnaryOperator.identity(), centroidSupplier.size());
            return null;
        }
    }

    @Override
    protected void writeCentroidData(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        CentroidGroups centroidGroups,
        IndexOutput centroidOutput
    ) throws IOException {
        if (centroidGroups != null) {
            writeCentroidsWithParents(fieldInfo, centroidSupplier, globalCentroid, centroidOffsetAndLength, centroidOutput, centroidGroups);
        } else {
            writeCentroidsWithoutParents(fieldInfo, centroidSupplier, globalCentroid, centroidOffsetAndLength, centroidOutput);
        }
    }

    private void writeCentroidLookup(IndexOutput out, int[] centroidAssignments, IntUnaryOperator ordinalMap, int numberCentroids)
        throws IOException {
        final int bitsRequired = DirectWriter.bitsRequired(numberCentroids);
        final long bytesRequired = DirectWriter.bytesRequired(centroidAssignments.length, bitsRequired);
        final ByteBuffersDataOutput memory = new ByteBuffersDataOutput(bytesRequired);
        final DirectWriter writer = DirectWriter.getInstance(memory, centroidAssignments.length, bitsRequired);
        for (int centroidAssignment : centroidAssignments) {
            writer.add(ordinalMap.applyAsInt(centroidAssignment));
        }
        writer.finish();
        out.copyBytes(memory.toDataInput(), memory.size());
    }

    private void writeCentroidsWithParents(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput,
        CentroidGroups centroidGroups
    ) throws IOException {
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(7, BULK_SIZE, centroidOutput, true, true);
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        centroidOutput.writeVInt(centroidGroups.centroids().length);
        centroidOutput.writeVInt(centroidGroups.maxVectorsPerCentroidLength());
        // let's also write the raw parent centroids
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < centroidGroups.centroids().length; i++) {
            float[] centroid = centroidGroups.centroids()[i];
            buffer.asFloatBuffer().put(centroid);
            centroidOutput.writeBytes(buffer.array(), buffer.array().length);
        }
        QuantizedCentroids parentQuantizeCentroid = new QuantizedCentroids(
            CentroidSupplier.fromArray(centroidGroups.centroids, CentroidIndex.NO_INDEX, fieldInfo.getVectorDimension()),
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        bulkWriter.writeVectors(parentQuantizeCentroid, null);
        int offset = 0;
        for (int[] centroidVectors : centroidGroups.vectors()) {
            centroidOutput.writeInt(offset);
            centroidOutput.writeInt(centroidVectors.length);
            offset += centroidVectors.length;
        }

        QuantizedCentroids childrenQuantizeCentroid = new QuantizedCentroids(
            centroidSupplier,
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        for (int[] centroidVectors : centroidGroups.vectors()) {
            childrenQuantizeCentroid.reset(idx -> centroidVectors[idx], centroidVectors.length);
            bulkWriter.writeVectors(childrenQuantizeCentroid, null);
        }
        // write the centroid offsets at the end of the file
        int parentOrd = 0;
        for (int[] centroidVectors : centroidGroups.vectors()) {
            for (int assignment : centroidVectors) {
                centroidOutput.writeLong(centroidOffsetAndLength.offsets().get(assignment));
                centroidOutput.writeLong(centroidOffsetAndLength.lengths().get(assignment));
                centroidOutput.writeInt(parentOrd);
            }
            parentOrd++;
        }
    }

    private void writeCentroidsWithoutParents(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput
    ) throws IOException {
        centroidOutput.writeVInt(0);
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(7, BULK_SIZE, centroidOutput, true, true);
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        QuantizedCentroids quantizedCentroids = new QuantizedCentroids(
            centroidSupplier,
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        bulkWriter.writeVectors(quantizedCentroids, null);
        // write the centroid offsets at the end of the file
        for (int i = 0; i < centroidSupplier.size(); i++) {
            centroidOutput.writeLong(centroidOffsetAndLength.offsets().get(i));
            centroidOutput.writeLong(centroidOffsetAndLength.lengths().get(i));
        }
    }

    private KMeansResult<float[]> buildSecondLevelClusters(FieldInfo fieldInfo, CentroidSupplier centroidSupplier, boolean isMerge)
        throws IOException {
        final KMeansFloatVectorValues floatVectorValues = centroidSupplier.asKmeansFloatVectorValues();
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
                HierarchicalKMeans.MAXK,
                -1 // disable SOAR assignments
            );
        } else {
            hierarchicalKMeans = HierarchicalKMeans.ofSerial(
                CentroidOps.FLOAT,
                fieldInfo.getVectorDimension(),
                HierarchicalKMeans.MAX_ITERATIONS_DEFAULT,
                HierarchicalKMeans.SAMPLES_PER_CLUSTER_DEFAULT,
                HierarchicalKMeans.MAXK,
                -1 // disable SOAR assignments
            );
        }
        var result = hierarchicalKMeans.cluster(floatVectorValues, centroidsPerParentCluster);
        assert result.overspill() == null;
        return result.result();
    }

    private CentroidGroups buildCentroidGroups(FlatCentroidClusters centroidClusters) {
        final int[] centroidVectorCount = new int[centroidClusters.size()];
        for (int i = 0; i < centroidClusters.assignments().length; i++) {
            centroidVectorCount[centroidClusters.assignments()[i]]++;
        }
        final int[][] vectorsPerCentroid = new int[centroidClusters.size()][];
        int maxVectorsPerCentroidLength = 0;
        for (int i = 0; i < centroidClusters.size(); i++) {
            vectorsPerCentroid[i] = new int[centroidVectorCount[i]];
            maxVectorsPerCentroidLength = Math.max(maxVectorsPerCentroidLength, centroidVectorCount[i]);
        }
        Arrays.fill(centroidVectorCount, 0);
        for (int i = 0; i < centroidClusters.assignments().length; i++) {
            final int c = centroidClusters.assignments()[i];
            vectorsPerCentroid[c][centroidVectorCount[c]++] = i;
        }
        return new CentroidGroups(centroidClusters.centroids(), vectorsPerCentroid, maxVectorsPerCentroidLength);
    }

    @Override
    public CentroidInformation calculateCentroids(FieldInfo fieldInfo, KMeansFloatVectorValues floatVectorValues, MergeState mergeState)
        throws IOException {
        // TODO: consider hinting / bootstrapping hierarchical kmeans with the prior segments centroids
        // TODO: for flush we are doing this over the vectors and here centroids which seems duplicative
        // preliminary tests suggest recall is good using only centroids but need to do further evaluation
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
        return calculateCentroids(hierarchicalKMeans, floatVectorValues, fieldInfo);
    }

    /**
     * Calculate the centroids for the given field.
     * We use the {@link HierarchicalKMeans} algorithm to partition the space of all vectors across merging segments
     *
     * @param fieldInfo merging field info
     * @param floatVectorValues the float vector values to merge
     * @return the vector assignments, soar assignments, and if asked the centroids themselves that were computed
     * @throws IOException if an I/O error occurs
     */
    @Override
    public CentroidInformation calculateCentroids(FieldInfo fieldInfo, KMeansFloatVectorValues floatVectorValues) throws IOException {
        HierarchicalKMeans<float[]> hierarchicalKMeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, floatVectorValues.dimension());
        return calculateCentroids(hierarchicalKMeans, floatVectorValues, fieldInfo);
    }

    private CentroidInformation calculateCentroids(
        HierarchicalKMeans<float[]> hierarchicalKMeans,
        KMeansFloatVectorValues floatVectorValues,
        FieldInfo fieldInfo
    ) throws IOException {
        KMeansWithOverspill<float[]> kMeansResult = hierarchicalKMeans.cluster(floatVectorValues, vectorPerCluster);
        float[][] centroids = kMeansResult.centroids();
        if (logger.isDebugEnabled()) {
            logger.debug("final centroid count: {}", centroids.length);
        }
        int[] assignments = kMeansResult.assignments();
        int[] soarAssignments = kMeansResult.soarAssignments();
        return new CentroidInformation(fieldInfo.getVectorDimension(), centroids, assignments, new SoarAssignments(soarAssignments));
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
        private int currOrd = -1;

        OffHeapCentroidSupplier(IndexInput centroidsInput, int numCentroids, FieldInfo info, KMeansResult<float[]> clusters) {
            this.centroidsInput = centroidsInput;
            this.numCentroids = numCentroids;
            this.dimension = info.getVectorDimension();
            this.scratch = new float[dimension];
            this.clusters = clusters;
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
        public KMeansFloatVectorValues asKmeansFloatVectorValues() throws IOException {
            return KMeansFloatVectorValues.build(centroidsInput, null, numCentroids, dimension);
        }
    }

    static class QuantizedCentroids implements QuantizedVectorValues {
        private final CentroidSupplier supplier;
        private final OptimizedScalarQuantizer quantizer;
        private final byte[] quantizedVector;
        private final int[] quantizedVectorScratch;
        private final float[] floatVectorScratch;
        private OptimizedScalarQuantizer.QuantizationResult corrections;
        private final float[] centroid;
        private int currOrd = -1;
        private IntToIntFunction ordTransformer = i -> i;
        int size;

        QuantizedCentroids(CentroidSupplier supplier, int dimension, OptimizedScalarQuantizer quantizer, float[] centroid) {
            this.supplier = supplier;
            this.quantizer = quantizer;
            this.quantizedVector = new byte[dimension];
            this.floatVectorScratch = new float[dimension];
            this.quantizedVectorScratch = new int[dimension];
            this.centroid = centroid;
            size = supplier.size();
        }

        @Override
        public int count() {
            return size;
        }

        void reset(IntToIntFunction ordTransformer, int size) {
            this.ordTransformer = ordTransformer;
            this.currOrd = -1;
            this.size = size;
            this.corrections = null;
        }

        @Override
        public byte[] next() throws IOException {
            if (currOrd >= count() - 1) {
                throw new IllegalStateException("No more vectors to read, current ord: " + currOrd + ", count: " + count());
            }
            currOrd++;
            float[] vector = supplier.centroid(ordTransformer.apply(currOrd));
            corrections = quantizer.scalarQuantize(vector, floatVectorScratch, quantizedVectorScratch, (byte) 7, centroid);
            for (int i = 0; i < quantizedVectorScratch.length; i++) {
                quantizedVector[i] = (byte) quantizedVectorScratch[i];
            }
            return quantizedVector;
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrections() {
            return corrections;
        }
    }

    static class OnHeapQuantizedVectors implements QuantizedVectorValues {
        private final FloatVectorValues vectorValues;
        private final OptimizedScalarQuantizer quantizer;
        private final byte[] quantizedVector;
        private final int[] quantizedVectorScratch;
        private final float[] floatVectorScratch;
        private final ES940DiskBBQVectorsFormat.QuantEncoding encoding;
        private OptimizedScalarQuantizer.QuantizationResult corrections;
        private final VectorSimilarityFunction similarityFunction;
        private float[] currentCentroid, currentParentCentroid;
        private IntToIntFunction ordTransformer = null;
        private int currOrd = -1;
        private int count;

        OnHeapQuantizedVectors(
            FloatVectorValues vectorValues,
            VectorSimilarityFunction similarityFunction,
            ES940DiskBBQVectorsFormat.QuantEncoding encoding,
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
        private final byte[] binaryScratch;
        private final float[] corrections = new float[3];

        private final int vectorByteSize;
        private int bitSum;
        private int currOrd = -1;
        private int count;
        private IntToBooleanFunction isOverspill = null;
        private IntToIntFunction ordTransformer = null;

        OffHeapQuantizedVectors(IndexInput quantizedVectorsInput, int vectorByteSize) {
            this.quantizedVectorsInput = quantizedVectorsInput;
            this.binaryScratch = new byte[vectorByteSize];
            this.vectorByteSize = (binaryScratch.length + 3 * Float.BYTES + Integer.BYTES);
        }

        private void reset(int count, IntToBooleanFunction isOverspill, IntToIntFunction ordTransformer) {
            this.count = count;
            this.isOverspill = isOverspill;
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
            boolean isOverspill = this.isOverspill.apply(currOrd);
            return getVector(ord, isOverspill);
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrections() {
            if (currOrd == -1) {
                throw new IllegalStateException("No vector read yet, call readQuantizedVector first");
            }
            return new OptimizedScalarQuantizer.QuantizationResult(corrections[0], corrections[1], corrections[2], bitSum);
        }

        byte[] getVector(int ord, boolean isOverspill) throws IOException {
            readQuantizedVector(ord, isOverspill);
            return binaryScratch;
        }

        public void readQuantizedVector(int ord, boolean isOverspill) throws IOException {
            long offset = (long) ord * (vectorByteSize * 2L) + (isOverspill ? vectorByteSize : 0);
            quantizedVectorsInput.seek(offset);
            quantizedVectorsInput.readBytes(binaryScratch, 0, binaryScratch.length);
            quantizedVectorsInput.readFloats(corrections, 0, 3);
            bitSum = quantizedVectorsInput.readInt();
        }
    }
}
