/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidAssignments;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidSupplier;
import org.elasticsearch.index.codec.vectors.diskbbq.DiskBBQBulkWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.DocIdsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IntSorter;
import org.elasticsearch.index.codec.vectors.diskbbq.IntToBooleanFunction;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantizedVectorValues;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.function.IntUnaryOperator;

import static org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans.NO_SOAR_ASSIGNMENT;

/**
 * Default implementation of {@link IVFVectorsWriter}. It uses {@link HierarchicalKMeans} algorithm to
 * partition the vector space, and then stores the centroids and posting list in a sequential
 * fashion.
 */
public class ESNextDiskBBQVectorsWriter extends IVFVectorsWriter {
    private static final Logger logger = LogManager.getLogger(ESNextDiskBBQVectorsWriter.class);

    private final int vectorPerCluster;
    private final int centroidsPerParentCluster;
    private final ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding;

    public ESNextDiskBBQVectorsWriter(
        SegmentWriteState state,
        String rawVectorFormatName,
        boolean useDirectIOReads,
        FlatVectorsWriter rawVectorDelegate,
        ESNextDiskBBQVectorsFormat.QuantEncoding encoding,
        int vectorPerCluster,
        int centroidsPerParentCluster
    ) throws IOException {
        super(state, rawVectorFormatName, useDirectIOReads, rawVectorDelegate, ESNextDiskBBQVectorsFormat.VERSION_CURRENT);
        this.vectorPerCluster = vectorPerCluster;
        this.centroidsPerParentCluster = centroidsPerParentCluster;
        this.quantEncoding = encoding;
    }

    @Override
    public CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        int[] assignments,
        int[] overspillAssignments
    ) throws IOException {
        int[] centroidVectorCount = new int[centroidSupplier.size()];
        for (int i = 0; i < assignments.length; i++) {
            centroidVectorCount[assignments[i]]++;
            // if soar assignments are present, count them as well
            if (overspillAssignments.length > i && overspillAssignments[i] != NO_SOAR_ASSIGNMENT) {
                centroidVectorCount[overspillAssignments[i]]++;
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
            // if soar assignments are present, add them to the cluster as well
            if (overspillAssignments.length > i) {
                int s = overspillAssignments[i];
                if (s != NO_SOAR_ASSIGNMENT) {
                    assignmentsByCluster[s][centroidVectorCount[s]++] = i;
                }
            }
        }
        // write the posting lists
        final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        final PackedLongValues.Builder lengths = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(quantEncoding.bits(), ES91OSQVectorsScorer.BULK_SIZE, postingsOutput);
        OnHeapQuantizedVectors onHeapQuantizedVectors = new OnHeapQuantizedVectors(
            floatVectorValues,
            quantEncoding,
            fieldInfo.getVectorDimension(),
            new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction())
        );
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        final int[] docIds = new int[maxPostingListSize];
        final int[] docDeltas = new int[maxPostingListSize];
        final int[] clusterOrds = new int[maxPostingListSize];
        DocIdsWriter idsWriter = new DocIdsWriter();
        for (int c = 0; c < centroidSupplier.size(); c++) {
            float[] centroid = centroidSupplier.centroid(c);
            int[] cluster = assignmentsByCluster[c];
            long offset = postingsOutput.alignFilePointer(Float.BYTES) - fileOffset;
            offsets.add(offset);
            buffer.asFloatBuffer().put(centroid);
            // write raw centroid for quantizing the query vectors
            postingsOutput.writeBytes(buffer.array(), buffer.array().length);
            // write centroid dot product for quantizing the query vectors
            postingsOutput.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(centroid, centroid)));
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
            onHeapQuantizedVectors.reset(centroid, size, ord -> cluster[clusterOrds[ord]]);
            byte encoding = idsWriter.calculateBlockEncoding(i -> docDeltas[i], size, ES91OSQVectorsScorer.BULK_SIZE);
            postingsOutput.writeByte(encoding);
            bulkWriter.writeVectors(onHeapQuantizedVectors, i -> {
                // for vector i we write `bulk` size docs or the remaining docs
                idsWriter.writeDocIds(d -> docDeltas[i + d], Math.min(ES91OSQVectorsScorer.BULK_SIZE, size - i), encoding, postingsOutput);
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
        int[] overspillAssignments
    ) throws IOException {
        // first, quantize all the vectors into a temporary file
        String quantizedVectorsTempName = null;
        boolean success = false;
        try (
            IndexOutput quantizedVectorsTemp = mergeState.segmentInfo.dir.createTempOutput(
                mergeState.segmentInfo.name,
                "qvec_",
                IOContext.DEFAULT
            )
        ) {
            quantizedVectorsTempName = quantizedVectorsTemp.getName();
            OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
            int[] quantized = new int[quantEncoding.discretizedDimensions(fieldInfo.getVectorDimension())];
            byte[] binary = new byte[quantEncoding.getDocPackedLength(fieldInfo.getVectorDimension())];
            float[] scratch = new float[fieldInfo.getVectorDimension()];
            for (int i = 0; i < assignments.length; i++) {
                int c = assignments[i];
                float[] centroid = centroidSupplier.centroid(c);
                float[] vector = floatVectorValues.vectorValue(i);
                boolean overspill = overspillAssignments.length > i && overspillAssignments[i] != NO_SOAR_ASSIGNMENT;
                OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(
                    vector,
                    scratch,
                    quantized,
                    quantEncoding.bits(),
                    centroid
                );
                quantEncoding.pack(quantized, binary);
                writeQuantizedValue(quantizedVectorsTemp, binary, result);
                if (overspill) {
                    int s = overspillAssignments[i];
                    // write the overspill vector as well
                    result = quantizer.scalarQuantize(vector, scratch, quantized, quantEncoding.bits(), centroidSupplier.centroid(s));
                    quantEncoding.pack(quantized, binary);
                    writeQuantizedValue(quantizedVectorsTemp, binary, result);
                } else {
                    // write a zero vector for the overspill
                    Arrays.fill(binary, (byte) 0);
                    OptimizedScalarQuantizer.QuantizationResult zeroResult = new OptimizedScalarQuantizer.QuantizationResult(0f, 0f, 0f, 0);
                    writeQuantizedValue(quantizedVectorsTemp, binary, zeroResult);
                }
            }
            success = true;
        } finally {
            if (success == false && quantizedVectorsTempName != null) {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, quantizedVectorsTempName);
            }
        }
        int[] centroidVectorCount = new int[centroidSupplier.size()];
        for (int i = 0; i < assignments.length; i++) {
            centroidVectorCount[assignments[i]]++;
            // if soar assignments are present, count them as well
            if (overspillAssignments.length > i && overspillAssignments[i] != NO_SOAR_ASSIGNMENT) {
                centroidVectorCount[overspillAssignments[i]]++;
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
            if (overspillAssignments.length > i) {
                int s = overspillAssignments[i];
                if (s != NO_SOAR_ASSIGNMENT) {
                    assignmentsByCluster[s][centroidVectorCount[s]] = i;
                    isOverspillByCluster[s][centroidVectorCount[s]++] = true;
                }
            }
        }
        // now we can read the quantized vectors from the temporary file
        try (IndexInput quantizedVectorsInput = mergeState.segmentInfo.dir.openInput(quantizedVectorsTempName, IOContext.DEFAULT)) {
            final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
            final PackedLongValues.Builder lengths = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
            OffHeapQuantizedVectors offHeapQuantizedVectors = new OffHeapQuantizedVectors(
                quantizedVectorsInput,
                quantEncoding,
                fieldInfo.getVectorDimension()
            );
            DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(
                quantEncoding.bits(),
                ES91OSQVectorsScorer.BULK_SIZE,
                postingsOutput
            );
            final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
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
                // write raw centroid for quantizing the query vectors
                buffer.asFloatBuffer().put(centroid);
                postingsOutput.writeBytes(buffer.array(), buffer.array().length);
                // write centroid dot product for quantizing the query vectors
                postingsOutput.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(centroid, centroid)));
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
                byte encoding = idsWriter.calculateBlockEncoding(i -> docDeltas[i], size, ES91OSQVectorsScorer.BULK_SIZE);
                postingsOutput.writeByte(encoding);
                offHeapQuantizedVectors.reset(size, ord -> isOverspill[clusterOrds[ord]], ord -> cluster[clusterOrds[ord]]);
                // write vectors
                bulkWriter.writeVectors(offHeapQuantizedVectors, i -> {
                    // for vector i we write `bulk` size docs or the remaining docs
                    idsWriter.writeDocIds(
                        d -> docDeltas[d + i],
                        Math.min(ESNextOSQVectorsScorer.BULK_SIZE, size - i),
                        encoding,
                        postingsOutput
                    );
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
        float mean = 0;
        float m2 = 0;
        // iteratively compute the variance & mean
        int count = 0;
        for (int[] cluster : clusters) {
            count += 1;
            if (cluster == null) {
                continue;
            }
            float delta = cluster.length - mean;
            mean += delta / count;
            m2 += delta * (cluster.length - mean);
            min = Math.min(min, cluster.length);
            max = Math.max(max, cluster.length);
        }
        float variance = m2 / (clusters.length - 1);
        logger.debug(
            "Centroid count: {} min: {} max: {} mean: {} stdDev: {} variance: {}",
            clusters.length,
            min,
            max,
            mean,
            Math.sqrt(variance),
            variance
        );
    }

    @Override
    public CentroidSupplier createCentroidSupplier(
        IndexInput centroidsInput,
        int numCentroids,
        FieldInfo fieldInfo,
        float[] globalCentroid
    ) {
        return new OffHeapCentroidSupplier(centroidsInput, numCentroids, fieldInfo);
    }

    @Override
    protected void doWriteMeta(IndexOutput metaOutput, FieldInfo field, int numCentroids) throws IOException {
        metaOutput.writeInt(quantEncoding.id());
    }

    @Override
    public void writeCentroids(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        int[] centroidAssignments,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput
    ) throws IOException {
        // TODO do we want to store these distances as well for future use?
        // TODO: sort centroids by global centroid (was doing so previously here)
        // TODO: sorting tanks recall possibly because centroids ordinals no longer are aligned
        if (centroidSupplier.size() > centroidsPerParentCluster * centroidsPerParentCluster) {

            final CentroidGroups centroidGroups = buildCentroidGroups(fieldInfo, centroidSupplier);
            {
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
            }
            writeCentroidsWithParents(fieldInfo, centroidSupplier, globalCentroid, centroidOffsetAndLength, centroidOutput, centroidGroups);
        } else {
            writeCentroidLookup(centroidOutput, centroidAssignments, IntUnaryOperator.identity(), centroidSupplier.size());
            writeCentroidsWithoutParents(fieldInfo, centroidSupplier, globalCentroid, centroidOffsetAndLength, centroidOutput);
        }
    }

    private void writeCentroidLookup(IndexOutput out, int[] centroidAssignments, IntUnaryOperator OrdinalMap, int numberCentroids)
        throws IOException {
        final int bitsRequired = DirectWriter.bitsRequired(numberCentroids);
        final long bytesRequired = ESNextDiskBBQVectorsReader.directWriterSizeOnDisk(centroidAssignments.length, bitsRequired);
        final ByteBuffersDataOutput memory = new ByteBuffersDataOutput(bytesRequired);
        final DirectWriter writer = DirectWriter.getInstance(memory, centroidAssignments.length, bitsRequired);
        for (int centroidAssignment : centroidAssignments) {
            writer.add(OrdinalMap.applyAsInt(centroidAssignment));
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
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(7, ES92Int7VectorsScorer.BULK_SIZE, centroidOutput);
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        centroidOutput.writeVInt(centroidGroups.centroids.length);
        centroidOutput.writeVInt(centroidGroups.maxVectorsPerCentroidLength);
        QuantizedCentroids parentQuantizeCentroid = new QuantizedCentroids(
            CentroidSupplier.fromArray(centroidGroups.centroids),
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
        for (int[] centroidVectors : centroidGroups.vectors()) {
            for (int assignment : centroidVectors) {
                centroidOutput.writeLong(centroidOffsetAndLength.offsets().get(assignment));
                centroidOutput.writeLong(centroidOffsetAndLength.lengths().get(assignment));
            }
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
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(7, ES92Int7VectorsScorer.BULK_SIZE, centroidOutput);
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

    private record CentroidGroups(float[][] centroids, int[][] vectors, int maxVectorsPerCentroidLength) {}

    private CentroidGroups buildCentroidGroups(FieldInfo fieldInfo, CentroidSupplier centroidSupplier) throws IOException {
        final FloatVectorValues floatVectorValues = FloatVectorValues.fromFloats(new AbstractList<>() {
            @Override
            public float[] get(int index) {
                try {
                    return centroidSupplier.centroid(index);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public int size() {
                return centroidSupplier.size();
            }
        }, fieldInfo.getVectorDimension());
        // we use the HierarchicalKMeans to partition the space of all vectors across merging segments
        // this are small numbers so we run it wih all the centroids.
        final KMeansResult kMeansResult = new HierarchicalKMeans(
            fieldInfo.getVectorDimension(),
            HierarchicalKMeans.MAX_ITERATIONS_DEFAULT,
            HierarchicalKMeans.SAMPLES_PER_CLUSTER_DEFAULT,
            HierarchicalKMeans.MAXK,
            -1 // disable SOAR assignments
        ).cluster(floatVectorValues, centroidsPerParentCluster);
        final int[] centroidVectorCount = new int[kMeansResult.centroids().length];
        for (int i = 0; i < kMeansResult.assignments().length; i++) {
            centroidVectorCount[kMeansResult.assignments()[i]]++;
        }
        final int[][] vectorsPerCentroid = new int[kMeansResult.centroids().length][];
        int maxVectorsPerCentroidLength = 0;
        for (int i = 0; i < kMeansResult.centroids().length; i++) {
            vectorsPerCentroid[i] = new int[centroidVectorCount[i]];
            maxVectorsPerCentroidLength = Math.max(maxVectorsPerCentroidLength, centroidVectorCount[i]);
        }
        Arrays.fill(centroidVectorCount, 0);
        for (int i = 0; i < kMeansResult.assignments().length; i++) {
            final int c = kMeansResult.assignments()[i];
            vectorsPerCentroid[c][centroidVectorCount[c]++] = i;
        }
        return new CentroidGroups(kMeansResult.centroids(), vectorsPerCentroid, maxVectorsPerCentroidLength);
    }

    @Override
    public CentroidAssignments calculateCentroids(FieldInfo fieldInfo, FloatVectorValues floatVectorValues, MergeState mergeState)
        throws IOException {
        return calculateCentroids(fieldInfo, floatVectorValues);
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
    public CentroidAssignments calculateCentroids(FieldInfo fieldInfo, FloatVectorValues floatVectorValues) throws IOException {
        // TODO: consider hinting / bootstrapping hierarchical kmeans with the prior segments centroids
        // TODO: for flush we are doing this over the vectors and here centroids which seems duplicative
        // preliminary tests suggest recall is good using only centroids but need to do further evaluation
        KMeansResult kMeansResult = new HierarchicalKMeans(floatVectorValues.dimension()).cluster(floatVectorValues, vectorPerCluster);
        float[][] centroids = kMeansResult.centroids();
        if (logger.isDebugEnabled()) {
            logger.debug("final centroid count: {}", centroids.length);
        }
        int[] assignments = kMeansResult.assignments();
        int[] soarAssignments = kMeansResult.soarAssignments();
        return new CentroidAssignments(fieldInfo.getVectorDimension(), centroids, assignments, soarAssignments);
    }

    static void writeQuantizedValue(IndexOutput indexOutput, byte[] binaryValue, OptimizedScalarQuantizer.QuantizationResult corrections)
        throws IOException {
        indexOutput.writeBytes(binaryValue, binaryValue.length);
        indexOutput.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
        indexOutput.writeInt(Float.floatToIntBits(corrections.upperInterval()));
        indexOutput.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
        assert corrections.quantizedComponentSum() >= 0 && corrections.quantizedComponentSum() <= 0xffff;
        indexOutput.writeShort((short) corrections.quantizedComponentSum());
    }

    static class OffHeapCentroidSupplier implements CentroidSupplier {
        private final IndexInput centroidsInput;
        private final int numCentroids;
        private final int dimension;
        private final float[] scratch;
        private int currOrd = -1;

        OffHeapCentroidSupplier(IndexInput centroidsInput, int numCentroids, FieldInfo info) {
            this.centroidsInput = centroidsInput;
            this.numCentroids = numCentroids;
            this.dimension = info.getVectorDimension();
            this.scratch = new float[dimension];
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
        private final ESNextDiskBBQVectorsFormat.QuantEncoding encoding;
        private OptimizedScalarQuantizer.QuantizationResult corrections;
        private float[] currentCentroid;
        private IntToIntFunction ordTransformer = null;
        private int currOrd = -1;
        private int count;

        OnHeapQuantizedVectors(
            FloatVectorValues vectorValues,
            ESNextDiskBBQVectorsFormat.QuantEncoding encoding,
            int dimension,
            OptimizedScalarQuantizer quantizer
        ) {
            this.vectorValues = vectorValues;
            this.encoding = encoding;
            this.quantizer = quantizer;
            this.quantizedVector = new byte[encoding.getDocPackedLength(dimension)];
            this.floatVectorScratch = new float[dimension];
            this.quantizedVectorScratch = new int[encoding.discretizedDimensions(dimension)];
            this.corrections = null;
        }

        private void reset(float[] centroid, int count, IntToIntFunction ordTransformer) {
            this.currentCentroid = centroid;
            this.ordTransformer = ordTransformer;
            this.currOrd = -1;
            this.count = count;
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

        OffHeapQuantizedVectors(IndexInput quantizedVectorsInput, ESNextDiskBBQVectorsFormat.QuantEncoding encoding, int dimension) {
            this.quantizedVectorsInput = quantizedVectorsInput;
            this.binaryScratch = new byte[encoding.getDocPackedLength(dimension)];
            this.vectorByteSize = (binaryScratch.length + 3 * Float.BYTES + Short.BYTES);
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
            bitSum = Short.toUnsignedInt(quantizedVectorsInput.readShort());
        }
    }
}
