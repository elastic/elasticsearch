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
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.Arrays;

/**
 * Default implementation of {@link IVFVectorsWriter}. It uses {@link HierarchicalKMeans} algorithm to
 * partition the vector space, and then stores the centroids and posting list in a sequential
 * fashion.
 */
public class DefaultIVFVectorsWriter extends IVFVectorsWriter {
    private static final Logger logger = LogManager.getLogger(DefaultIVFVectorsWriter.class);

    private final int vectorPerCluster;
    private final int centroidsPerParentCluster;

    public DefaultIVFVectorsWriter(
        SegmentWriteState state,
        FlatVectorsWriter rawVectorDelegate,
        int vectorPerCluster,
        int centroidsPerParentCluster
    ) throws IOException {
        super(state, rawVectorDelegate);
        this.vectorPerCluster = vectorPerCluster;
        this.centroidsPerParentCluster = centroidsPerParentCluster;
    }

    @Override
    LongValues buildAndWritePostingsLists(
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
        final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        DocIdsWriter docIdsWriter = new DocIdsWriter();
        DiskBBQBulkWriter bulkWriter = new DiskBBQBulkWriter.OneBitDiskBBQBulkWriter(ES91OSQVectorsScorer.BULK_SIZE, postingsOutput);
        OnHeapQuantizedVectors onHeapQuantizedVectors = new OnHeapQuantizedVectors(
            floatVectorValues,
            fieldInfo.getVectorDimension(),
            new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction())
        );
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int c = 0; c < centroidSupplier.size(); c++) {
            float[] centroid = centroidSupplier.centroid(c);
            int[] cluster = assignmentsByCluster[c];
            offsets.add(postingsOutput.alignFilePointer(Float.BYTES));
            buffer.asFloatBuffer().put(centroid);
            // write raw centroid for quantizing the query vectors
            postingsOutput.writeBytes(buffer.array(), buffer.array().length);
            // write centroid dot product for quantizing the query vectors
            postingsOutput.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(centroid, centroid)));
            int size = cluster.length;
            // write docIds
            postingsOutput.writeVInt(size);
            onHeapQuantizedVectors.reset(centroid, size, ord -> cluster[ord]);
            // TODO we might want to consider putting the docIds in a separate file
            // to aid with only having to fetch vectors from slower storage when they are required
            // keeping them in the same file indicates we pull the entire file into cache
            docIdsWriter.writeDocIds(j -> floatVectorValues.ordToDoc(cluster[j]), size, postingsOutput);
            // write vectors
            bulkWriter.writeVectors(onHeapQuantizedVectors);
        }

        if (logger.isDebugEnabled()) {
            printClusterQualityStatistics(assignmentsByCluster);
        }

        return offsets.build();
    }

    @Override
    LongValues buildAndWritePostingsLists(
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
            final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
            OffHeapQuantizedVectors offHeapQuantizedVectors = new OffHeapQuantizedVectors(
                quantizedVectorsInput,
                fieldInfo.getVectorDimension()
            );
            DocIdsWriter docIdsWriter = new DocIdsWriter();
            DiskBBQBulkWriter bulkWriter = new DiskBBQBulkWriter.OneBitDiskBBQBulkWriter(ES91OSQVectorsScorer.BULK_SIZE, postingsOutput);
            final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            for (int c = 0; c < centroidSupplier.size(); c++) {
                float[] centroid = centroidSupplier.centroid(c);
                int[] cluster = assignmentsByCluster[c];
                boolean[] isOverspill = isOverspillByCluster[c];
                offsets.add(postingsOutput.alignFilePointer(Float.BYTES));
                // write raw centroid for quantizing the query vectors
                buffer.asFloatBuffer().put(centroid);
                postingsOutput.writeBytes(buffer.array(), buffer.array().length);
                // write centroid dot product for quantizing the query vectors
                postingsOutput.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(centroid, centroid)));
                // write docIds
                int size = cluster.length;
                postingsOutput.writeVInt(size);
                offHeapQuantizedVectors.reset(size, ord -> isOverspill[ord], ord -> cluster[ord]);
                // TODO we might want to consider putting the docIds in a separate file
                // to aid with only having to fetch vectors from slower storage when they are required
                // keeping them in the same file indicates we pull the entire file into cache
                docIdsWriter.writeDocIds(j -> floatVectorValues.ordToDoc(cluster[j]), size, postingsOutput);
                // write vectors
                bulkWriter.writeVectors(offHeapQuantizedVectors);
            }

            if (logger.isDebugEnabled()) {
                printClusterQualityStatistics(assignmentsByCluster);
            }
            return offsets.build();
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

    @Override
    void writeCentroids(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        LongValues offsets,
        IndexOutput centroidOutput
    ) throws IOException {
        // TODO do we want to store these distances as well for future use?
        // TODO: sort centroids by global centroid (was doing so previously here)
        // TODO: sorting tanks recall possibly because centroids ordinals no longer are aligned
        if (centroidSupplier.size() > centroidsPerParentCluster * centroidsPerParentCluster) {
            writeCentroidsWithParents(fieldInfo, centroidSupplier, globalCentroid, offsets, centroidOutput);
        } else {
            writeCentroidsWithoutParents(fieldInfo, centroidSupplier, globalCentroid, offsets, centroidOutput);
        }
    }

    private void writeCentroidsWithParents(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        LongValues offsets,
        IndexOutput centroidOutput
    ) throws IOException {
        DiskBBQBulkWriter.SevenBitDiskBBQBulkWriter bulkWriter = new DiskBBQBulkWriter.SevenBitDiskBBQBulkWriter(
            ES92Int7VectorsScorer.BULK_SIZE,
            centroidOutput
        );
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        final CentroidGroups centroidGroups = buildCentroidGroups(fieldInfo, centroidSupplier);
        centroidOutput.writeVInt(centroidGroups.centroids.length);
        centroidOutput.writeVInt(centroidGroups.maxVectorsPerCentroidLength);
        QuantizedCentroids parentQuantizeCentroid = new QuantizedCentroids(
            new OnHeapCentroidSupplier(centroidGroups.centroids),
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        bulkWriter.writeVectors(parentQuantizeCentroid);
        int offset = 0;
        for (int i = 0; i < centroidGroups.centroids().length; i++) {
            centroidOutput.writeInt(offset);
            centroidOutput.writeInt(centroidGroups.vectors()[i].length);
            offset += centroidGroups.vectors()[i].length;
        }

        QuantizedCentroids childrenQuantizeCentroid = new QuantizedCentroids(
            centroidSupplier,
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        for (int i = 0; i < centroidGroups.centroids().length; i++) {
            final int[] centroidAssignments = centroidGroups.vectors()[i];
            childrenQuantizeCentroid.reset(idx -> centroidAssignments[idx], centroidAssignments.length);
            bulkWriter.writeVectors(childrenQuantizeCentroid);
        }
        // write the centroid offsets at the end of the file
        for (int i = 0; i < centroidGroups.centroids().length; i++) {
            final int[] centroidAssignments = centroidGroups.vectors()[i];
            for (int assignment : centroidAssignments) {
                centroidOutput.writeLong(offsets.get(assignment));
            }
        }
    }

    private void writeCentroidsWithoutParents(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        LongValues offsets,
        IndexOutput centroidOutput
    ) throws IOException {
        centroidOutput.writeVInt(0);
        DiskBBQBulkWriter.SevenBitDiskBBQBulkWriter bulkWriter = new DiskBBQBulkWriter.SevenBitDiskBBQBulkWriter(
            ES92Int7VectorsScorer.BULK_SIZE,
            centroidOutput
        );
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        QuantizedCentroids quantizedCentroids = new QuantizedCentroids(
            centroidSupplier,
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        bulkWriter.writeVectors(quantizedCentroids);
        // write the centroid offsets at the end of the file
        for (int i = 0; i < centroidSupplier.size(); i++) {
            centroidOutput.writeLong(offsets.get(i));
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

    /**
     * Calculate the centroids for the given field.
     * We use the {@link HierarchicalKMeans} algorithm to partition the space of all vectors across merging segments
     *
     * @param fieldInfo merging field info
     * @param floatVectorValues the float vector values to merge
     * @param globalCentroid the global centroid, calculated by this method and used to quantize the centroids
     * @return the vector assignments, soar assignments, and if asked the centroids themselves that were computed
     * @throws IOException if an I/O error occurs
     */
    @Override
    CentroidAssignments calculateCentroids(FieldInfo fieldInfo, FloatVectorValues floatVectorValues, float[] globalCentroid)
        throws IOException {

        long nanoTime = System.nanoTime();

        // TODO: consider hinting / bootstrapping hierarchical kmeans with the prior segments centroids
        CentroidAssignments centroidAssignments = buildCentroidAssignments(floatVectorValues, vectorPerCluster);
        float[][] centroids = centroidAssignments.centroids();
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

        if (logger.isDebugEnabled()) {
            logger.debug("calculate centroids and assign vectors time ms: {}", (System.nanoTime() - nanoTime) / 1000000.0);
            logger.debug("final centroid count: {}", centroids.length);
        }
        return centroidAssignments;
    }

    static CentroidAssignments buildCentroidAssignments(FloatVectorValues floatVectorValues, int vectorPerCluster) throws IOException {
        KMeansResult kMeansResult = new HierarchicalKMeans(floatVectorValues.dimension()).cluster(floatVectorValues, vectorPerCluster);
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

    interface QuantizedVectorValues {
        int count();

        byte[] next() throws IOException;

        OptimizedScalarQuantizer.QuantizationResult getCorrections() throws IOException;
    }

    interface IntToBooleanFunction {
        boolean apply(int ord);
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
            // Its possible that the vectors are on-heap and we cannot mutate them as we may quantize twice
            // due to overspill, so we copy the vector to a scratch array
            System.arraycopy(vector, 0, floatVectorScratch, 0, vector.length);
            corrections = quantizer.scalarQuantize(floatVectorScratch, quantizedVectorScratch, (byte) 7, centroid);
            for (int i = 0; i < quantizedVectorScratch.length; i++) {
                quantizedVector[i] = (byte) quantizedVectorScratch[i];
            }
            return quantizedVector;
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrections() throws IOException {
            return corrections;
        }
    }

    static class OnHeapQuantizedVectors implements QuantizedVectorValues {
        private final FloatVectorValues vectorValues;
        private final OptimizedScalarQuantizer quantizer;
        private final byte[] quantizedVector;
        private final int[] quantizedVectorScratch;
        private final float[] floatVectorScratch;
        private OptimizedScalarQuantizer.QuantizationResult corrections;
        private float[] currentCentroid;
        private IntToIntFunction ordTransformer = null;
        private int currOrd = -1;
        private int count;

        OnHeapQuantizedVectors(FloatVectorValues vectorValues, int dimension, OptimizedScalarQuantizer quantizer) {
            this.vectorValues = vectorValues;
            this.quantizer = quantizer;
            this.quantizedVector = new byte[BQVectorUtils.discretize(dimension, 64) / 8];
            this.floatVectorScratch = new float[dimension];
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
            // Its possible that the vectors are on-heap and we cannot mutate them as we may quantize twice
            // due to overspill, so we copy the vector to a scratch array
            System.arraycopy(vector, 0, floatVectorScratch, 0, vector.length);
            corrections = quantizer.scalarQuantize(floatVectorScratch, quantizedVectorScratch, (byte) 1, currentCentroid);
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
