/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntIntMap;

import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Default implementation of {@link IVFVectorsWriter}. It uses {@link HierarchicalKMeans} algorithm to
 * partition the vector space, and then stores the centroids and posting list in a sequential
 * fashion.
 */
public class DefaultIVFVectorsWriter extends IVFVectorsWriter {
    private static final Logger logger = LogManager.getLogger(DefaultIVFVectorsWriter.class);

    private final int vectorPerCluster;

    public DefaultIVFVectorsWriter(SegmentWriteState state, FlatVectorsWriter rawVectorDelegate, int vectorPerCluster) throws IOException {
        super(state, rawVectorDelegate);
        this.vectorPerCluster = vectorPerCluster;
    }

    @Override
    long[] buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        int[][] assignmentsByCluster
    ) throws IOException {
        // write the posting lists
        final long[] offsets = new long[centroidSupplier.size()];
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        DocIdsWriter docIdsWriter = new DocIdsWriter();
        DiskBBQBulkWriter bulkWriter = new DiskBBQBulkWriter.OneBitDiskBBQBulkWriter(
            ES91OSQVectorsScorer.BULK_SIZE,
            quantizer,
            floatVectorValues,
            postingsOutput
        );
        for (int c = 0; c < centroidSupplier.size(); c++) {
            float[] centroid = centroidSupplier.centroid(c);
            // TODO: add back in sorting vectors by distance to centroid
            int[] cluster = assignmentsByCluster[c];
            // TODO align???
            offsets[c] = postingsOutput.getFilePointer();
            int size = cluster.length;
            postingsOutput.writeVInt(size);
            postingsOutput.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(centroid, centroid)));
            // TODO we might want to consider putting the docIds in a separate file
            // to aid with only having to fetch vectors from slower storage when they are required
            // keeping them in the same file indicates we pull the entire file into cache
            docIdsWriter.writeDocIds(j -> floatVectorValues.ordToDoc(cluster[j]), size, postingsOutput);
            bulkWriter.writeOrds(j -> cluster[j], cluster.length, centroid);
        }

        if (logger.isDebugEnabled()) {
            printClusterQualityStatistics(assignmentsByCluster);
        }

        return offsets;
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
    CentroidSupplier createCentroidSupplier(
        IndexInput centroidsInput,
        int numParentCentroids,
        int numCentroids,
        int numClusters,
        FieldInfo fieldInfo,
        float[] globalCentroid,
        IntIntMap clusterToCentroidMap
    ) {
        return new OffHeapCentroidSupplier(centroidsInput, numParentCentroids, numCentroids, numClusters, fieldInfo, clusterToCentroidMap);
    }

    private static void writeQuantizedCentroid(
        float[] centroid,
        float[] centroidScratch,
        OptimizedScalarQuantizer osq,
        int[] quantizedScratch,
        float[] globalCentroid,
        byte[] quantized,
        IndexOutput centroidOutput
    ) throws IOException {
        System.arraycopy(centroid, 0, centroidScratch, 0, centroid.length);
        OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(
            centroidScratch,
            quantizedScratch,
            (byte) 4,
            globalCentroid
        );
        for (int i = 0; i < quantizedScratch.length; i++) {
            quantized[i] = (byte) quantizedScratch[i];
        }
        writeQuantizedValue(centroidOutput, quantized, result);
    }

    static IntIntMap writePartitionsAndCentroids(
        CentroidPartition[] centroidPartitions,
        float[][] centroids,
        FieldInfo fieldInfo,
        float[] globalCentroid,
        IndexOutput centroidOutput
    ) throws IOException {
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        int[] quantizedScratch = new int[fieldInfo.getVectorDimension()];
        float[] centroidScratch = new float[fieldInfo.getVectorDimension()];
        final byte[] quantized = new byte[fieldInfo.getVectorDimension()];
        IntIntMap clusterToCentroidMap = new IntIntHashMap(centroids.length);
        // TODO do we want to store these distances as well for future use?
        // TODO: sort centroids by global centroid (was doing so previously here)

        if (centroidPartitions != null) {
            // write the top level partition parent nodes and their pointers to the centroids within the partition
            // a size of 1 indicates a leaf node that did not have a parent node (orphans)
            for (CentroidPartition centroidPartition : centroidPartitions) {
                writeQuantizedCentroid(
                    centroidPartition.centroid(),
                    centroidScratch,
                    osq,
                    quantizedScratch,
                    globalCentroid,
                    quantized,
                    centroidOutput
                );
                // TODO: put at the end of the parents region
                centroidOutput.writeInt(centroidPartition.childOrdinal());
                centroidOutput.writeInt(centroidPartition.size());
            }
        }

        // write the quantized centroids for the partitions which should include an assignment and a SOAR assignment
        // it may also include duplicates from the parent nodes written above in the case of orphans
        int centroidOrd = 0;
        for (CentroidPartition centroidPartition : centroidPartitions) {
            int[] parentAssignments = centroidPartition.assignments();
            for (int parentAssignment : parentAssignments) {
                float[] centroid = centroids[parentAssignment];
                writeQuantizedCentroid(centroid, centroidScratch, osq, quantizedScratch, globalCentroid, quantized, centroidOutput);
                // TODO: put at the end of the centroids region
                centroidOutput.writeInt(parentAssignment); // clusterOrdinal
                clusterToCentroidMap.put(parentAssignment, centroidOrd);
                centroidOrd++;
            }
        }

        // TODO: don't write the raw centroids twice (because of SOAR)
        // write the raw float vectors so we can quantize the query vector relative to the centroid on read
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (CentroidPartition centroidPartition : centroidPartitions) {
            int[] parentAssignments = centroidPartition.assignments();
            for (int parentAssignment : parentAssignments) {
                float[] centroid = centroids[parentAssignment];
                buffer.asFloatBuffer().put(centroid);
                centroidOutput.writeBytes(buffer.array(), buffer.array().length);
            }
        }

        return clusterToCentroidMap;
    }

    static void writeCentroids(float[][] centroids, FieldInfo fieldInfo, float[] globalCentroid, IndexOutput centroidOutput)
        throws IOException {
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        int[] quantizedScratch = new int[fieldInfo.getVectorDimension()];
        float[] centroidScratch = new float[fieldInfo.getVectorDimension()];
        final byte[] quantized = new byte[fieldInfo.getVectorDimension()];
        // TODO do we want to store these distances as well for future use?
        // TODO: sort centroids by global centroid (was doing so previously here)

        for (int c = 0; c < centroids.length; c++) {
            float[] centroid = centroids[c];
            writeQuantizedCentroid(centroid, centroidScratch, osq, quantizedScratch, globalCentroid, quantized, centroidOutput);
            // TODO: put at the end of the centroids region
            centroidOutput.writeInt(c); // clusterOrdinal
        }

        // write the raw float vectors so we can quantize the query vector relative to the centroid on read
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (float[] centroid : centroids) {
            buffer.asFloatBuffer().put(centroid);
            centroidOutput.writeBytes(buffer.array(), buffer.array().length);
        }
    }

    @Override
    CentroidAssignments calculateAndWriteCentroids(
        FieldInfo fieldInfo,
        FloatVectorValues floatVectorValues,
        IndexOutput centroidOutput,
        MergeState mergeState,
        float[] globalCentroid
    ) throws IOException {
        // TODO: take advantage of prior generated clusters from mergeState in the future
        return calculateAndWriteCentroids(fieldInfo, floatVectorValues, centroidOutput, globalCentroid);
    }

    record CentroidPartition(float[] centroid, int childOrdinal, int size, int[] assignments) {}

    /**
     * Calculate the centroids for the given field and write them to the given centroid output.
     * We use the {@link HierarchicalKMeans} algorithm to partition the space of all vectors across merging segments
     *
     * @param fieldInfo merging field info
     * @param floatVectorValues the float vector values to merge
     * @param centroidOutput the centroid output
     * @param globalCentroid the global centroid, calculated by this method and used to quantize the centroids
     * @return the vector assignments, soar assignments, and if asked the centroids themselves that were computed
     * @throws IOException if an I/O error occurs
     */
    @Override
    CentroidAssignments calculateAndWriteCentroids(
        FieldInfo fieldInfo,
        FloatVectorValues floatVectorValues,
        IndexOutput centroidOutput,
        float[] globalCentroid
    ) throws IOException {

        long nanoTime = System.nanoTime();

        // TODO: consider hinting / bootstrapping hierarchical kmeans with the prior segments centroids
        KMeansResult kMeansResult = new HierarchicalKMeans(floatVectorValues.dimension()).cluster(floatVectorValues, vectorPerCluster);
        float[][] centroids = kMeansResult.centroids();
        // TODO: for flush we are doing this over the vectors and here centroids which seems duplicative
        // preliminary tests suggest recall is good using only centroids but need to do further evaluation
        // TODO: push this logic into vector util?
        for (float[] centroid : centroids) {
            for (int j = 0; j < centroid.length; j++) {
                globalCentroid[j] += centroid[j];
            }
        }
        for (int j = 0; j < globalCentroid.length; j++) {
            globalCentroid[j] /= centroids.length;
        }

        // TODO: generate this strucutre while constructing the hkmeans structure (do it all in one hkmeans pass)

        CentroidPartition[] centroidPartitions = null;
        int[][] assignmentsByParentCluster;
        int partitionsCount = 0;
        int totalCentroidCount = 0;

        // FIXME: last thing out ... set this really high for now to allow merging to main
        if (centroids.length > IVFVectorsFormat.DEFAULT_VECTORS_PER_CLUSTER) {
            KMeansResult result = clusterParentCentroids(fieldInfo, centroids);
            float[][] parentCentroids = result.centroids();
            int[] parentChildAssignments = result.assignments();
            int[] parentChildSoarAssignments = result.soarAssignments();

            centroidPartitions = new CentroidPartition[parentCentroids.length];

            assignmentsByParentCluster = buildCentroidAssignments(
                parentCentroids.length,
                parentChildAssignments,
                parentChildSoarAssignments
            );

            // TODO: refactor and push this logic into writeCentroidsAndPartitions, CentroidPartition is not needed anymore?
            int childOffset = 0;
            for (int i = 0; i < assignmentsByParentCluster.length; i++) {
                int label = i;
                int centroidCount = assignmentsByParentCluster[i].length;
                assert centroidCount > 0;
                float[] parentCentroid = parentCentroids[label];
                centroidPartitions[partitionsCount++] = new CentroidPartition(
                    parentCentroid,
                    childOffset,
                    centroidCount,
                    assignmentsByParentCluster[i]
                );
                childOffset += centroidCount;
                totalCentroidCount += centroidCount;
            }
        } else {
            totalCentroidCount = centroids.length;
        }

        IntIntMap clusterToCentroidMap = null;
        if (centroidPartitions != null) {
            clusterToCentroidMap = writePartitionsAndCentroids(centroidPartitions, centroids, fieldInfo, globalCentroid, centroidOutput);
        } else {
            writeCentroids(centroids, fieldInfo, globalCentroid, centroidOutput);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("calculate centroids and assign vectors time ms: {}", (System.nanoTime() - nanoTime) / 1000000.0);
            logger.debug("final parent centroid count {}: ", partitionsCount);
            logger.debug("final centroid count: {}", centroids.length);
        }

        int[] assignments = kMeansResult.assignments();
        int[] soarAssignments = kMeansResult.soarAssignments();
        int[][] assignmentsByCluster = buildCentroidAssignments(centroids.length, assignments, soarAssignments);
        return new CentroidAssignments(partitionsCount, totalCentroidCount, centroids, assignmentsByCluster, clusterToCentroidMap);
    }

    private KMeansResult clusterParentCentroids(FieldInfo fieldInfo, float[][] centroids) throws IOException {
        FloatVectorValues centroidsAsFVV = new FloatVectorValues() {
            @Override
            public int size() {
                return centroids.length;
            }

            @Override
            public int dimension() {
                return fieldInfo.getVectorDimension();
            }

            @Override
            public float[] vectorValue(int targetOrd) {
                return centroids[targetOrd];
            }

            @Override
            public FloatVectorValues copy() {
                return this;
            }

            @Override
            public DocIndexIterator iterator() {
                return createDenseIterator();
            }
        };

        HierarchicalKMeans hierarchicalKMeans = new HierarchicalKMeans(fieldInfo.getVectorDimension());
        // TODO: evaluate further whether 512 is better than other sizes like 32 or sqrt(centroids.length)
        return hierarchicalKMeans.cluster(centroidsAsFVV, 8); // centroids.length / (int) Math.sqrt(centroids.length));
    }

    static int[][] buildCentroidAssignments(int centroidCount, int[] assignments, int[] soarAssignments) {
        int[] centroidVectorCount = new int[centroidCount];
        for (int i = 0; i < assignments.length; i++) {
            centroidVectorCount[assignments[i]]++;
            // if soar assignments are present, count them as well
            if (soarAssignments.length > i && soarAssignments[i] != -1) {
                centroidVectorCount[soarAssignments[i]]++;
            }
        }

        int[][] assignmentsByCluster = new int[centroidCount][];
        for (int c = 0; c < centroidCount; c++) {
            assignmentsByCluster[c] = new int[centroidVectorCount[c]];
        }
        Arrays.fill(centroidVectorCount, 0);

        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            assignmentsByCluster[c][centroidVectorCount[c]++] = i;
            // if soar assignments are present, add them to the cluster as well
            if (soarAssignments.length > i) {
                int s = soarAssignments[i];
                if (s != -1) {
                    assignmentsByCluster[s][centroidVectorCount[s]++] = i;
                }
            }
        }
        return assignmentsByCluster;
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
        private final int numClusters;
        private final int dimension;
        private final float[] scratch;
        private final long rawCentroidOffset;
        private int currOrd = -1;
        private final IntIntMap clusterToCentroidMap;

        OffHeapCentroidSupplier(
            IndexInput centroidsInput,
            int numParentCentroids,
            int numCentroids,
            int numClusters,
            FieldInfo info,
            IntIntMap clusterToCentroidMap
        ) {
            this.centroidsInput = centroidsInput;
            this.numClusters = numClusters;
            this.dimension = info.getVectorDimension();
            this.scratch = new float[dimension];
            long quantizedVectorByteSize = dimension + 3 * Float.BYTES + Short.BYTES;
            long quantizedVectorNodeByteSize = quantizedVectorByteSize + Integer.BYTES;
            long parentNodeByteSize = quantizedVectorByteSize + 2 * Integer.BYTES;
            this.rawCentroidOffset = numParentCentroids * parentNodeByteSize + numCentroids * quantizedVectorNodeByteSize;
            this.clusterToCentroidMap = clusterToCentroidMap;
        }

        @Override
        public int size() {
            return numClusters;
        }

        @Override
        public float[] centroid(int clusterOrdinal) throws IOException {
            if (clusterOrdinal == currOrd) {
                return scratch;
            }
            int centroidOrdinal;
            if (clusterToCentroidMap != null) {
                centroidOrdinal = clusterToCentroidMap.get(clusterOrdinal);
            } else {
                centroidOrdinal = clusterOrdinal;
            }
            centroidsInput.seek(rawCentroidOffset + (long) centroidOrdinal * dimension * Float.BYTES);
            centroidsInput.readFloats(scratch, 0, dimension);
            this.currOrd = clusterOrdinal;
            return scratch;
        }
    }
}
