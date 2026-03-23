/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VectorScorerTestUtils {

    public static int[] generateFilteredOffsets(Random random, int numVectors, int filteredVectors) {
        var allOffsets = IntStream.range(0, numVectors).boxed().collect(Collectors.toList());
        for (int i = 0; i < filteredVectors; i++) {
            int allOffsetsLength = allOffsets.size();
            allOffsets.remove(random.nextInt(0, allOffsetsLength));
        }
        return allOffsets.stream().mapToInt(i -> i).toArray();
    }

    public record VectorData(
        byte[] vector,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        short quantizedComponentSum
    ) {}

    public record OSQVectorData(
        byte[] quantizedVector,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {}

    private VectorScorerTestUtils() {}

    public static VectorData createBinarizedIndexData(
        float[] values,
        float[] centroid,
        OptimizedScalarQuantizer binaryQuantizer,
        int dimension
    ) {
        int discretizedDimension = BQVectorUtils.discretize(dimension, 64);

        int[] quantizationScratch = new int[dimension];
        byte[] toIndex = new byte[discretizedDimension / 8];

        float[] scratch = new float[dimension];

        OptimizedScalarQuantizer.QuantizationResult r = binaryQuantizer.scalarQuantize(
            values,
            scratch,
            quantizationScratch,
            (byte) 1,
            centroid
        );
        // pack and store document bit vector
        ESVectorUtil.packAsBinary(quantizationScratch, toIndex);

        assert r.quantizedComponentSum() >= 0 && r.quantizedComponentSum() <= 0xffff;

        return new VectorData(toIndex, r.lowerInterval(), r.upperInterval(), r.additionalCorrection(), (short) r.quantizedComponentSum());
    }

    public static VectorData createBinarizedQueryData(
        float[] floatVectorValues,
        float[] centroid,
        OptimizedScalarQuantizer binaryQuantizer,
        int dimension
    ) {
        int discretizedDimension = BQVectorUtils.discretize(dimension, 64);

        int[] quantizationScratch = new int[dimension];
        byte[] toQuery = new byte[(discretizedDimension / 8) * ESVectorUtilSupport.B_QUERY];

        float[] scratch = new float[dimension];

        OptimizedScalarQuantizer.QuantizationResult r = binaryQuantizer.scalarQuantize(
            floatVectorValues,
            scratch,
            quantizationScratch,
            (byte) ESVectorUtilSupport.B_QUERY,
            centroid
        );

        // pack and store the 4bit query vector
        ESVectorUtil.transposeHalfByte(quantizationScratch, toQuery);
        assert r.quantizedComponentSum() >= 0 && r.quantizedComponentSum() <= 0xffff;

        return new VectorData(toQuery, r.lowerInterval(), r.upperInterval(), r.additionalCorrection(), (short) r.quantizedComponentSum());
    }

    public static void writeBinarizedVectorData(IndexOutput binarizedQueryData, VectorData vectorData) throws IOException {
        binarizedQueryData.writeBytes(vectorData.vector, vectorData.vector.length);
        binarizedQueryData.writeInt(Float.floatToIntBits(vectorData.lowerInterval()));
        binarizedQueryData.writeInt(Float.floatToIntBits(vectorData.upperInterval()));
        binarizedQueryData.writeInt(Float.floatToIntBits(vectorData.additionalCorrection()));
        binarizedQueryData.writeShort(vectorData.quantizedComponentSum());
    }

    public static OSQVectorData createOSQIndexData(
        float[] values,
        float[] centroid,
        OptimizedScalarQuantizer quantizer,
        int dimensions,
        byte indexBits,
        int vectorPackedLengthInBytes
    ) {

        final float[] residualScratch = new float[dimensions];
        final int[] scratch = new int[dimensions];
        final byte[] qVector = new byte[vectorPackedLengthInBytes];

        OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(
            values,
            residualScratch,
            scratch,
            indexBits,
            centroid
        );
        ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(indexBits).pack(scratch, qVector);

        return new OSQVectorData(
            qVector,
            result.lowerInterval(),
            result.upperInterval(),
            result.additionalCorrection(),
            result.quantizedComponentSum()
        );
    }

    public static OSQVectorData createOSQQueryData(
        float[] query,
        float[] centroid,
        OptimizedScalarQuantizer quantizer,
        int dimensions,
        byte queryBits,
        int queryVectorPackedLengthInBytes
    ) {
        final float[] residualScratch = new float[dimensions];
        final int[] scratch = new int[dimensions];

        OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
            query,
            residualScratch,
            scratch,
            queryBits,
            centroid
        );
        final byte[] quantizeQuery = new byte[queryVectorPackedLengthInBytes];
        ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(queryBits).packQuery(scratch, quantizeQuery);

        return new OSQVectorData(
            quantizeQuery,
            queryCorrections.lowerInterval(),
            queryCorrections.upperInterval(),
            queryCorrections.additionalCorrection(),
            queryCorrections.quantizedComponentSum()
        );
    }

    public static void writeSingleOSQVectorData(IndexOutput out, OSQVectorData vectorData) throws IOException {
        out.writeBytes(vectorData.quantizedVector(), 0, vectorData.quantizedVector().length);
        out.writeInt(Float.floatToIntBits(vectorData.lowerInterval()));
        out.writeInt(Float.floatToIntBits(vectorData.upperInterval()));
        out.writeInt(Float.floatToIntBits(vectorData.additionalCorrection()));
        out.writeInt(vectorData.quantizedComponentSum());
    }

    public static void writeBulkOSQVectorData(int bulkSize, IndexOutput out, VectorScorerTestUtils.OSQVectorData[] vectors)
        throws IOException {
        writeBulkOSQVectorData(bulkSize, out, vectors, 0);
    }

    public static void writeBulkOSQVectorData(int bulkSize, IndexOutput out, VectorScorerTestUtils.OSQVectorData[] vectors, int offset)
        throws IOException {
        for (int j = 0; j < bulkSize; j++) {
            out.writeBytes(vectors[offset + j].quantizedVector(), 0, vectors[offset + j].quantizedVector().length);
        }
        writeCorrections(vectors, offset, bulkSize, out);
    }

    private static void writeCorrections(VectorScorerTestUtils.OSQVectorData[] corrections, int offset, int count, IndexOutput out)
        throws IOException {
        for (int i = 0; i < count; i++) {
            out.writeInt(Float.floatToIntBits(corrections[offset + i].lowerInterval()));
        }
        for (int i = 0; i < count; i++) {
            out.writeInt(Float.floatToIntBits(corrections[offset + i].upperInterval()));
        }
        for (int i = 0; i < count; i++) {
            out.writeInt(corrections[offset + i].quantizedComponentSum());
        }
        for (int i = 0; i < count; i++) {
            out.writeInt(Float.floatToIntBits(corrections[offset + i].additionalCorrection()));
        }
    }

    public static void randomVector(Random random, float[] vector, VectorSimilarityFunction vectorSimilarityFunction) {
        for (int i = 0; i < vector.length; i++) {
            vector[i] = random.nextFloat();
        }
        if (vectorSimilarityFunction != VectorSimilarityFunction.EUCLIDEAN) {
            VectorUtil.l2normalize(vector);
        }
    }

    public static void randomInt4Bytes(Random random, byte[] bytes) {
        for (int i = 0, len = bytes.length; i < len;) {
            bytes[i++] = (byte) random.nextInt(0, 16);
        }
    }

    public static void writePackedVectorWithCorrection(
        IndexOutput out,
        byte[] packed,
        org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult correction
    ) throws IOException {
        out.writeBytes(packed, 0, packed.length);
        out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
        out.writeInt(Float.floatToIntBits(correction.upperInterval()));
        out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
        out.writeInt(correction.quantizedComponentSum());
    }

    /**
     * Creates a disk-backed {@link QuantizedByteVectorValues} for int4 (PACKED_NIBBLE) vectors.
     * The data must have been written via {@link #writePackedVectorWithCorrection}.
     */
    public static QuantizedByteVectorValues createDenseInt4VectorValues(
        int dims,
        int size,
        float[] centroid,
        float centroidDp,
        IndexInput in,
        VectorSimilarityFunction sim
    ) throws IOException {
        var slice = in.slice("values", 0, in.length());
        return new DenseOffHeapInt4VectorValues(dims, size, sim, slice, centroid, centroidDp);
    }

    @SuppressForbidden(reason = "require usage of OptimizedScalarQuantizer")
    private static org.apache.lucene.util.quantization.OptimizedScalarQuantizer luceneScalarQuantizer(VectorSimilarityFunction sim) {
        return new org.apache.lucene.util.quantization.OptimizedScalarQuantizer(sim);
    }

    private static class DenseOffHeapInt4VectorValues extends QuantizedByteVectorValues {
        final int dimension;
        final int size;
        final VectorSimilarityFunction similarityFunction;

        final IndexInput slice;
        final byte[] vectorValue;
        final ByteBuffer byteBuffer;
        final int byteSize;
        private int lastOrd = -1;
        final float[] correctiveValues;
        int quantizedComponentSum;
        final float[] centroid;
        final float centroidDp;

        DenseOffHeapInt4VectorValues(
            int dimension,
            int size,
            VectorSimilarityFunction similarityFunction,
            IndexInput slice,
            float[] centroid,
            float centroidDp
        ) {
            this.dimension = dimension;
            this.size = size;
            this.similarityFunction = similarityFunction;
            this.slice = slice;
            this.centroid = centroid;
            this.centroidDp = centroidDp;
            this.correctiveValues = new float[3];
            this.byteSize = dimension / 2 + (Float.BYTES * 3) + Integer.BYTES;
            this.byteBuffer = ByteBuffer.allocate(dimension / 2);
            this.vectorValue = byteBuffer.array();
        }

        @Override
        public IndexInput getSlice() {
            return slice;
        }

        @Override
        public org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int vectorOrd)
            throws IOException {
            if (lastOrd != vectorOrd) {
                slice.seek((long) vectorOrd * byteSize);
                slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), vectorValue.length);
                slice.readFloats(correctiveValues, 0, 3);
                quantizedComponentSum = slice.readInt();
                lastOrd = vectorOrd;
            }
            return new org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult(
                correctiveValues[0],
                correctiveValues[1],
                correctiveValues[2],
                quantizedComponentSum
            );
        }

        @Override
        public org.apache.lucene.util.quantization.OptimizedScalarQuantizer getQuantizer() {
            return luceneScalarQuantizer(similarityFunction);
        }

        @Override
        public Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding getScalarEncoding() {
            return Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.PACKED_NIBBLE;
        }

        @Override
        public float[] getCentroid() {
            return centroid;
        }

        @Override
        public float getCentroidDP() {
            return centroidDp;
        }

        @Override
        public VectorScorer scorer(float[] query) {
            assert false;
            return null;
        }

        @Override
        public byte[] vectorValue(int ord) throws IOException {
            if (lastOrd == ord) {
                return vectorValue;
            }
            slice.seek((long) ord * byteSize);
            slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), vectorValue.length);
            slice.readFloats(correctiveValues, 0, 3);
            quantizedComponentSum = slice.readInt();
            lastOrd = ord;
            return vectorValue;
        }

        @Override
        public int dimension() {
            return dimension;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public QuantizedByteVectorValues copy() throws IOException {
            return new DenseOffHeapInt4VectorValues(dimension, size, similarityFunction, slice.clone(), centroid, centroidDp);
        }
    }
}
