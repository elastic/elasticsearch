/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.internal.hppc.IntArrayList;
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
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.codecs.lucene102.Lucene102BinaryQuantizedVectorsFormat.INDEX_BITS;
import static org.elasticsearch.index.codec.vectors.BQVectorUtils.discretize;
import static org.elasticsearch.index.codec.vectors.BQVectorUtils.packAsBinary;
import static org.elasticsearch.index.codec.vectors.IVFVectorsFormat.DEFAULT_VECTORS_PER_CLUSTER;

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
        IntArrayList[] assignmentsByCluster
    ) throws IOException {
        // write the posting lists
        final long[] offsets = new long[centroidSupplier.size()];
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        BinarizedFloatVectorValues binarizedByteVectorValues = new BinarizedFloatVectorValues(floatVectorValues, quantizer);
        DocIdsWriter docIdsWriter = new DocIdsWriter();

        for (int c = 0; c < centroidSupplier.size(); c++) {
            float[] centroid = centroidSupplier.centroid(c);
            binarizedByteVectorValues.centroid = centroid;
            // TODO: add back in sorting vectors by distance to centroid
            IntArrayList cluster = assignmentsByCluster[c];
            // TODO align???
            offsets[c] = postingsOutput.getFilePointer();
            int size = cluster.size();
            postingsOutput.writeVInt(size);
            postingsOutput.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(centroid, centroid)));
            // TODO we might want to consider putting the docIds in a separate file
            // to aid with only having to fetch vectors from slower storage when they are required
            // keeping them in the same file indicates we pull the entire file into cache
            docIdsWriter.writeDocIds(j -> floatVectorValues.ordToDoc(cluster.get(j)), size, postingsOutput);
            writePostingList(cluster, postingsOutput, binarizedByteVectorValues);
        }

        if (logger.isDebugEnabled()) {
            printClusterQualityStatistics(assignmentsByCluster);
        }

        return offsets;
    }

    private static void printClusterQualityStatistics(IntArrayList[] clusters) {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        float mean = 0;
        float m2 = 0;
        // iteratively compute the variance & mean
        int count = 0;
        for (IntArrayList cluster : clusters) {
            count += 1;
            if (cluster == null) {
                continue;
            }
            float delta = cluster.size() - mean;
            mean += delta / count;
            m2 += delta * (cluster.size() - mean);
            min = Math.min(min, cluster.size());
            max = Math.max(max, cluster.size());
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

    private void writePostingList(IntArrayList cluster, IndexOutput postingsOutput, BinarizedFloatVectorValues binarizedByteVectorValues)
        throws IOException {
        int limit = cluster.size() - ES91OSQVectorsScorer.BULK_SIZE + 1;
        int cidx = 0;
        OptimizedScalarQuantizer.QuantizationResult[] corrections =
            new OptimizedScalarQuantizer.QuantizationResult[ES91OSQVectorsScorer.BULK_SIZE];
        // Write vectors in bulks of ES91OSQVectorsScorer.BULK_SIZE.
        for (; cidx < limit; cidx += ES91OSQVectorsScorer.BULK_SIZE) {
            for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
                int ord = cluster.get(cidx + j);
                byte[] binaryValue = binarizedByteVectorValues.vectorValue(ord);
                // write vector
                postingsOutput.writeBytes(binaryValue, 0, binaryValue.length);
                corrections[j] = binarizedByteVectorValues.getCorrectiveTerms(ord);
            }
            // write corrections
            for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
                postingsOutput.writeInt(Float.floatToIntBits(corrections[j].lowerInterval()));
            }
            for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
                postingsOutput.writeInt(Float.floatToIntBits(corrections[j].upperInterval()));
            }
            for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
                int targetComponentSum = corrections[j].quantizedComponentSum();
                assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
                postingsOutput.writeShort((short) targetComponentSum);
            }
            for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
                postingsOutput.writeInt(Float.floatToIntBits(corrections[j].additionalCorrection()));
            }
        }
        // write tail
        for (; cidx < cluster.size(); cidx++) {
            int ord = cluster.get(cidx);
            // write vector
            byte[] binaryValue = binarizedByteVectorValues.vectorValue(ord);
            OptimizedScalarQuantizer.QuantizationResult correction = binarizedByteVectorValues.getCorrectiveTerms(ord);
            writeQuantizedValue(postingsOutput, binaryValue, correction);
            binarizedByteVectorValues.getCorrectiveTerms(ord);
            postingsOutput.writeBytes(binaryValue, 0, binaryValue.length);
            postingsOutput.writeInt(Float.floatToIntBits(correction.lowerInterval()));
            postingsOutput.writeInt(Float.floatToIntBits(correction.upperInterval()));
            postingsOutput.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
            assert correction.quantizedComponentSum() >= 0 && correction.quantizedComponentSum() <= 0xffff;
            postingsOutput.writeShort((short) correction.quantizedComponentSum());
        }
    }

    @Override
    CentroidSupplier createCentroidSupplier(
        IndexInput centroidsInput,
        int numParentCentroids,
        int numCentroids,
        FieldInfo fieldInfo,
        float[] globalCentroid
    ) {
        return new OffHeapCentroidSupplier(centroidsInput, numParentCentroids, numCentroids, fieldInfo);
    }

    static void writeCentroidsAndPartitions(
        List<CentroidPartition> centroidPartitions,
        float[][] centroids,
        FieldInfo fieldInfo,
        float[] globalCentroid,
        IndexOutput centroidOutput
    ) throws IOException {
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        byte[] quantizedScratch = new byte[fieldInfo.getVectorDimension()];
        float[] centroidScratch = new float[fieldInfo.getVectorDimension()];
        // TODO do we want to store these distances as well for future use?
        // TODO: sort centroids by global centroid (was doing so previously here)

        // write the top level partition parent nodes and their pointers to the centroids within the partition
        // a size of 1 indicates a leaf node that did not have a parent node (orphans)
        for (CentroidPartition centroidPartition : centroidPartitions) {
            System.arraycopy(centroidPartition.centroid(), 0, centroidScratch, 0, centroidPartition.centroid().length);
            OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(
                centroidScratch,
                quantizedScratch,
                (byte) 4,
                globalCentroid
            );
            writeQuantizedValue(centroidOutput, quantizedScratch, result);
            centroidOutput.writeInt(centroidPartition.childOrdinal());
            centroidOutput.writeInt(centroidPartition.size());
        }

        // write the quantized centroids which will be duplicate for orphans
        for (float[] centroid : centroids) {
            System.arraycopy(centroid, 0, centroidScratch, 0, centroid.length);
            OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(
                centroidScratch,
                quantizedScratch,
                (byte) 4,
                globalCentroid
            );
            writeQuantizedValue(centroidOutput, quantizedScratch, result);
        }

        // write the raw float vectors so we can quantize the query vector relative to the centroid on read
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (float[] centroid : centroids) {
            buffer.asFloatBuffer().put(centroid);
            centroidOutput.writeBytes(buffer.array(), buffer.array().length);
        }
    }

    CentroidAssignments calculateAndWriteCentroids(
        FieldInfo fieldInfo,
        FloatVectorValues floatVectorValues,
        IndexOutput centroidOutput,
        MergeState mergeState,
        float[] globalCentroid
    ) throws IOException {
        // TODO: take advantage of prior generated clusters from mergeState in the future
        return calculateAndWriteCentroids(fieldInfo, floatVectorValues, centroidOutput, globalCentroid, false);
    }

    CentroidAssignments calculateAndWriteCentroids(
        FieldInfo fieldInfo,
        FloatVectorValues floatVectorValues,
        IndexOutput centroidOutput,
        float[] globalCentroid
    ) throws IOException {
        return calculateAndWriteCentroids(fieldInfo, floatVectorValues, centroidOutput, globalCentroid, true);
    }

    record CentroidPartition(float[] centroid, int childOrdinal, int size) {}

    /**
     * Calculate the centroids for the given field and write them to the given centroid output.
     * We use the {@link HierarchicalKMeans} algorithm to partition the space of all vectors across merging segments
     *
     * @param fieldInfo merging field info
     * @param floatVectorValues the float vector values to merge
     * @param centroidOutput the centroid output
     * @param globalCentroid the global centroid, calculated by this method and used to quantize the centroids
     * @param cacheCentroids whether the centroids are kept or discarded once computed
     * @return the vector assignments, soar assignments, and if asked the centroids themselves that were computed
     * @throws IOException if an I/O error occurs
     */
    CentroidAssignments calculateAndWriteCentroids(
        FieldInfo fieldInfo,
        FloatVectorValues floatVectorValues,
        IndexOutput centroidOutput,
        float[] globalCentroid,
        boolean cacheCentroids
    ) throws IOException {

        long nanoTime = System.nanoTime();

        // TODO: consider hinting / bootstrapping hierarchical kmeans with the prior segments centroids
        KMeansResult kMeansResult = new HierarchicalKMeans(floatVectorValues.dimension()).cluster(floatVectorValues, vectorPerCluster);
        float[][] centroids = kMeansResult.centroids();
        int[] assignments = kMeansResult.assignments();
        int[] soarAssignments = kMeansResult.soarAssignments();

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

        // TODO: sort while constructing the hkmeans structure
        // we do this so we don't have to sort the assignments which is much more expensive
        int[] centroidOrds = new int[centroids.length];
        for (int i = 0; i < centroidOrds.length; i++) {
            centroidOrds[i] = i;
        }

        List<CentroidPartition> centroidPartitions = new ArrayList<>();

        // TODO: make this configurable
        if (centroids.length > DEFAULT_VECTORS_PER_CLUSTER) {
            // TODO: sort by global centroids as well
            // TODO: have this take a function instead of just an int[] for sorting
            AssignmentArraySorter sorter = new AssignmentArraySorter(centroids, centroidOrds, kMeansResult.parentLayer());
            sorter.sort(0, centroids.length);

            for (int i = 0; i < kMeansResult.parentLayer().length;) {
                // for any layer that was not partitioned we treat it duplicatively as a parent and child
                if (kMeansResult.parentLayer()[i] == -1) {
                    centroidPartitions.add(new CentroidPartition(centroids[i], i, 1));
                    i++;
                } else {
                    int label = kMeansResult.parentLayer()[i];
                    int centroidCount = 0;
                    float[] parentPartitionCentroid = new float[fieldInfo.getVectorDimension()];
                    int j = i;
                    for (; j < kMeansResult.parentLayer().length; j++) {
                        if (kMeansResult.parentLayer()[j] != label) {
                            break;
                        }
                        for (int k = 0; k < parentPartitionCentroid.length; k++) {
                            parentPartitionCentroid[k] += centroids[i][k];
                        }
                        centroidCount++;
                    }
                    int childOrdinal = i;
                    i = j;
                    for (int d = 0; d < parentPartitionCentroid.length; d++) {
                        parentPartitionCentroid[d] /= centroidCount;
                    }
                    centroidPartitions.add(new CentroidPartition(parentPartitionCentroid, childOrdinal, centroidCount));
                }
            }
        }

        writeCentroidsAndPartitions(centroidPartitions, centroids, fieldInfo, globalCentroid, centroidOutput);

        System.out.println("total parent centroids: " + centroidPartitions.size());
        System.out.println("total child centroids: " + centroids.length);

        if (logger.isDebugEnabled()) {
            logger.debug("calculate centroids and assign vectors time ms: {}", (System.nanoTime() - nanoTime) / 1000000.0);
            logger.debug("final centroid count: {}", centroids.length);
        }

        IntArrayList[] assignmentsByCluster = new IntArrayList[centroids.length];
        for (int c = 0; c < assignmentsByCluster.length; c++) {
            IntArrayList cluster = new IntArrayList(vectorPerCluster);
            for (int j = 0; j < assignments.length; j++) {
                if (assignments[j] == -1) {
                    continue;
                }
                if (assignments[j] == centroidOrds[c]) {
                    cluster.add(j);
                }
            }

            for (int j = 0; j < soarAssignments.length; j++) {
                if (soarAssignments[j] == -1) {
                    continue;
                }
                if (soarAssignments[j] == centroidOrds[c]) {
                    cluster.add(j);
                }
            }

            cluster.trimToSize();
            assignmentsByCluster[c] = cluster;
        }

        if (cacheCentroids) {
            return new CentroidAssignments(centroidPartitions.size(), centroids, assignmentsByCluster);
        } else {
            return new CentroidAssignments(centroidPartitions.size(), centroids.length, assignmentsByCluster);
        }
    }

    // TODO unify with OSQ format
    static class BinarizedFloatVectorValues {
        private OptimizedScalarQuantizer.QuantizationResult corrections;
        private final byte[] binarized;
        private final byte[] initQuantized;
        private float[] centroid;
        private final FloatVectorValues values;
        private final OptimizedScalarQuantizer quantizer;

        private int lastOrd = -1;

        BinarizedFloatVectorValues(FloatVectorValues delegate, OptimizedScalarQuantizer quantizer) {
            this.values = delegate;
            this.quantizer = quantizer;
            this.binarized = new byte[discretize(delegate.dimension(), 64) / 8];
            this.initQuantized = new byte[delegate.dimension()];
        }

        public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int ord) {
            if (ord != lastOrd) {
                throw new IllegalStateException(
                    "attempt to retrieve corrective terms for different ord " + ord + " than the quantization was done for: " + lastOrd
                );
            }
            return corrections;
        }

        public byte[] vectorValue(int ord) throws IOException {
            if (ord != lastOrd) {
                binarize(ord);
                lastOrd = ord;
            }
            return binarized;
        }

        private void binarize(int ord) throws IOException {
            corrections = quantizer.scalarQuantize(values.vectorValue(ord), initQuantized, INDEX_BITS, centroid);
            packAsBinary(initQuantized, binarized);
        }
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
        private final long rawCentroidOffset;
        private int currOrd = -1;

        OffHeapCentroidSupplier(IndexInput centroidsInput, int numParentCentroids, int numCentroids, FieldInfo info) {
            this.centroidsInput = centroidsInput;
            this.numCentroids = numCentroids;
            this.dimension = info.getVectorDimension();
            this.scratch = new float[dimension];
            long quantizedVectorByteSize = dimension + 3 * Float.BYTES + Short.BYTES;
            long parentNodeByteSize = quantizedVectorByteSize + 2 * Integer.BYTES;
            this.rawCentroidOffset = quantizedVectorByteSize * numCentroids + parentNodeByteSize * numParentCentroids;
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
            centroidsInput.seek(rawCentroidOffset + (long) centroidOrdinal * dimension * Float.BYTES);
            centroidsInput.readFloats(scratch, 0, dimension);
            this.currOrd = centroidOrdinal;
            return scratch;
        }
    }
}
