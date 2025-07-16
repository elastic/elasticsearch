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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.IntToIntFunction;
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
        int[] assignments,
        int[] overspillAssignments
    ) throws IOException {
        int[] centroidVectorCount = new int[centroidSupplier.size()];
        for (int i = 0; i < assignments.length; i++) {
            centroidVectorCount[assignments[i]]++;
            // if soar assignments are present, count them as well
            if (overspillAssignments.length > i && overspillAssignments[i] != -1) {
                centroidVectorCount[overspillAssignments[i]]++;
            }
        }

        int[][] assignmentsByCluster = new int[centroidSupplier.size()][];
        for (int c = 0; c < centroidSupplier.size(); c++) {
            assignmentsByCluster[c] = new int[centroidVectorCount[c]];
        }
        Arrays.fill(centroidVectorCount, 0);

        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            assignmentsByCluster[c][centroidVectorCount[c]++] = i;
            // if soar assignments are present, add them to the cluster as well
            if (overspillAssignments.length > i) {
                int s = overspillAssignments[i];
                if (s != -1) {
                    assignmentsByCluster[s][centroidVectorCount[s]++] = i;
                }
            }
        }
        // write the posting lists
        final long[] offsets = new long[centroidSupplier.size()];
        DocIdsWriter docIdsWriter = new DocIdsWriter();
        DiskBBQBulkWriter bulkWriter = new DiskBBQBulkWriter.OneBitDiskBBQBulkWriter(ES91OSQVectorsScorer.BULK_SIZE, postingsOutput);
        OnHeapQuantizedVectors onHeapQuantizedVectors = new OnHeapQuantizedVectors(
            floatVectorValues,
            fieldInfo.getVectorDimension(),
            new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction())
        );
        for (int c = 0; c < centroidSupplier.size(); c++) {
            float[] centroid = centroidSupplier.centroid(c);
            int[] cluster = assignmentsByCluster[c];
            // TODO align???
            offsets[c] = postingsOutput.getFilePointer();
            int size = cluster.length;
            postingsOutput.writeVInt(size);
            postingsOutput.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(centroid, centroid)));
            onHeapQuantizedVectors.reset(centroid, size, ord -> cluster[ord]);
            // TODO we might want to consider putting the docIds in a separate file
            // to aid with only having to fetch vectors from slower storage when they are required
            // keeping them in the same file indicates we pull the entire file into cache
            docIdsWriter.writeDocIds(j -> floatVectorValues.ordToDoc(cluster[j]), size, postingsOutput);
            bulkWriter.writeVectors(onHeapQuantizedVectors);
        }

        if (logger.isDebugEnabled()) {
            printClusterQualityStatistics(assignmentsByCluster);
        }

        return offsets;
    }

    @Override
    long[] buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        MergeState mergeState,
        int[] assignments,
        int[] overspillAssignments
    ) throws IOException {
        // first, quantize all the vectors into a temporary file
        String quantizedVectorsTempName = null;
        IndexOutput quantizedVectorsTemp = null;
        boolean success = false;
        try {
            quantizedVectorsTemp = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "qvec_", IOContext.DEFAULT);
            quantizedVectorsTempName = quantizedVectorsTemp.getName();
            OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
            int[] quantized = new int[fieldInfo.getVectorDimension()];
            byte[] binary = new byte[BQVectorUtils.discretize(fieldInfo.getVectorDimension(), 64) / 8];
            float[] overspillScratch = new float[fieldInfo.getVectorDimension()];
            for (int i = 0; i < assignments.length; i++) {
                int c = assignments[i];
                float[] centroid = centroidSupplier.centroid(c);
                float[] vector = floatVectorValues.vectorValue(i);
                boolean overspill = overspillAssignments.length > i && overspillAssignments[i] != -1;
                // if overspilling, this means we quantize twice, and quantization mutates the in-memory representation of the vector
                // so, make a copy of the vector to avoid mutating it
                if (overspill) {
                    System.arraycopy(vector, 0, overspillScratch, 0, fieldInfo.getVectorDimension());
                }

                OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(vector, quantized, (byte) 1, centroid);
                BQVectorUtils.packAsBinary(quantized, binary);
                writeQuantizedValue(quantizedVectorsTemp, binary, result);
                if (overspill) {
                    int s = overspillAssignments[i];
                    // write the overspill vector as well
                    result = quantizer.scalarQuantize(overspillScratch, quantized, (byte) 1, centroidSupplier.centroid(s));
                    BQVectorUtils.packAsBinary(quantized, binary);
                    writeQuantizedValue(quantizedVectorsTemp, binary, result);
                } else {
                    // write a zero vector for the overspill
                    Arrays.fill(binary, (byte) 0);
                    OptimizedScalarQuantizer.QuantizationResult zeroResult = new OptimizedScalarQuantizer.QuantizationResult(0f, 0f, 0f, 0);
                    writeQuantizedValue(quantizedVectorsTemp, binary, zeroResult);
                }
            }
            // close the temporary file so we can read it later
            quantizedVectorsTemp.close();
            success = true;
        } finally {
            if (success == false && quantizedVectorsTemp != null) {
                mergeState.segmentInfo.dir.deleteFile(quantizedVectorsTemp.getName());
            }
        }
        int[] centroidVectorCount = new int[centroidSupplier.size()];
        for (int i = 0; i < assignments.length; i++) {
            centroidVectorCount[assignments[i]]++;
            // if soar assignments are present, count them as well
            if (overspillAssignments.length > i && overspillAssignments[i] != -1) {
                centroidVectorCount[overspillAssignments[i]]++;
            }
        }

        int[][] assignmentsByCluster = new int[centroidSupplier.size()][];
        boolean[][] isOverspillByCluster = new boolean[centroidSupplier.size()][];
        for (int c = 0; c < centroidSupplier.size(); c++) {
            assignmentsByCluster[c] = new int[centroidVectorCount[c]];
            isOverspillByCluster[c] = new boolean[centroidVectorCount[c]];
        }
        Arrays.fill(centroidVectorCount, 0);

        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            assignmentsByCluster[c][centroidVectorCount[c]++] = i;
            // if soar assignments are present, add them to the cluster as well
            if (overspillAssignments.length > i) {
                int s = overspillAssignments[i];
                if (s != -1) {
                    assignmentsByCluster[s][centroidVectorCount[s]] = i;
                    isOverspillByCluster[s][centroidVectorCount[s]++] = true;
                }
            }
        }
        // now we can read the quantized vectors from the temporary file
        try (IndexInput quantizedVectorsInput = mergeState.segmentInfo.dir.openInput(quantizedVectorsTempName, IOContext.DEFAULT)) {
            final long[] offsets = new long[centroidSupplier.size()];
            OffHeapQuantizedVectors offHeapQuantizedVectors = new OffHeapQuantizedVectors(
                quantizedVectorsInput,
                fieldInfo.getVectorDimension()
            );
            DocIdsWriter docIdsWriter = new DocIdsWriter();
            DiskBBQBulkWriter bulkWriter = new DiskBBQBulkWriter.OneBitDiskBBQBulkWriter(ES91OSQVectorsScorer.BULK_SIZE, postingsOutput);
            for (int c = 0; c < centroidSupplier.size(); c++) {
                float[] centroid = centroidSupplier.centroid(c);
                int[] cluster = assignmentsByCluster[c];
                boolean[] isOverspill = isOverspillByCluster[c];
                // TODO align???
                offsets[c] = postingsOutput.getFilePointer();
                int size = cluster.length;
                postingsOutput.writeVInt(size);
                postingsOutput.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(centroid, centroid)));
                offHeapQuantizedVectors.reset(size, ord -> isOverspill[ord], ord -> cluster[ord]);
                // TODO we might want to consider putting the docIds in a separate file
                // to aid with only having to fetch vectors from slower storage when they are required
                // keeping them in the same file indicates we pull the entire file into cache
                docIdsWriter.writeDocIds(j -> floatVectorValues.ordToDoc(cluster[j]), size, postingsOutput);
                bulkWriter.writeVectors(offHeapQuantizedVectors);
            }

            if (logger.isDebugEnabled()) {
                printClusterQualityStatistics(assignmentsByCluster);
            }
            return offsets;
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
    CentroidSupplier createCentroidSupplier(IndexInput centroidsInput, int numCentroids, FieldInfo fieldInfo, float[] globalCentroid) {
        return new OffHeapCentroidSupplier(centroidsInput, numCentroids, fieldInfo);
    }

    static void writeCentroids(float[][] centroids, FieldInfo fieldInfo, float[] globalCentroid, IndexOutput centroidOutput)
        throws IOException {
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        int[] quantizedScratch = new int[fieldInfo.getVectorDimension()];
        float[] centroidScratch = new float[fieldInfo.getVectorDimension()];
        final byte[] quantized = new byte[fieldInfo.getVectorDimension()];
        // TODO do we want to store these distances as well for future use?
        // TODO: sort centroids by global centroid (was doing so previously here)
        // TODO: sorting tanks recall possibly because centroids ordinals no longer are aligned
        for (float[] centroid : centroids) {
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

        // write centroids
        writeCentroids(centroids, fieldInfo, globalCentroid, centroidOutput);

        if (logger.isDebugEnabled()) {
            logger.debug("calculate centroids and assign vectors time ms: {}", (System.nanoTime() - nanoTime) / 1000000.0);
            logger.debug("final centroid count: {}", centroids.length);
        }
        return buildCentroidAssignments(kMeansResult);
    }

    static CentroidAssignments buildCentroidAssignments(KMeansResult kMeansResult) {
        float[][] centroids = kMeansResult.centroids();
        int[] assignments = kMeansResult.assignments();
        int[] soarAssignments = kMeansResult.soarAssignments();
        return new CentroidAssignments(centroids, assignments, soarAssignments);
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

        OffHeapCentroidSupplier(IndexInput centroidsInput, int numCentroids, FieldInfo info) {
            this.centroidsInput = centroidsInput;
            this.numCentroids = numCentroids;
            this.dimension = info.getVectorDimension();
            this.scratch = new float[dimension];
            this.rawCentroidOffset = (dimension + 3 * Float.BYTES + Short.BYTES) * numCentroids;
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

    interface QuantizedVectorValues {
        int count();

        byte[] next() throws IOException;

        OptimizedScalarQuantizer.QuantizationResult getCorrections() throws IOException;
    }

    interface IntToBooleanFunction {
        boolean apply(int ord);
    }

    static class OnHeapQuantizedVectors implements QuantizedVectorValues {
        private final FloatVectorValues vectorValues;
        private final OptimizedScalarQuantizer quantizer;
        private final byte[] quantizedVector;
        private final int[] quantizedVectorScratch;
        private OptimizedScalarQuantizer.QuantizationResult corrections;
        private float[] currentCentroid;
        private IntToIntFunction ordTransformer = null;
        private int currOrd = -1;
        private int count;

        OnHeapQuantizedVectors(FloatVectorValues vectorValues, int dimension, OptimizedScalarQuantizer quantizer) {
            this.vectorValues = vectorValues;
            this.quantizer = quantizer;
            this.quantizedVector = new byte[BQVectorUtils.discretize(dimension, 64) / 8];
            this.quantizedVectorScratch = new int[dimension];
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
            corrections = quantizer.scalarQuantize(vector, quantizedVectorScratch, (byte) 1, currentCentroid);
            BQVectorUtils.packAsBinary(quantizedVectorScratch, quantizedVector);
            return quantizedVector;
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrections() throws IOException {
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
        private short bitSum;
        private int currOrd = -1;
        private int count;
        private IntToBooleanFunction isOverspill = null;
        private IntToIntFunction ordTransformer = null;

        OffHeapQuantizedVectors(IndexInput quantizedVectorsInput, int dimension) {
            this.quantizedVectorsInput = quantizedVectorsInput;
            this.binaryScratch = new byte[BQVectorUtils.discretize(dimension, 64) / 8];
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
        public OptimizedScalarQuantizer.QuantizationResult getCorrections() throws IOException {
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
            bitSum = quantizedVectorsInput.readShort();
        }
    }
}
