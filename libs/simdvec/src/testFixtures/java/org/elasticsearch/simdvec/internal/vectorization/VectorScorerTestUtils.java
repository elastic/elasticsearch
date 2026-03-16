/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Random;

public class VectorScorerTestUtils {

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
            bytes[i++] = (byte) random.nextInt(0, 0x10);
        }
    }

    /**
     * Packs unpacked int4 values (one value per byte) into Lucene nibble-packed format (two values per byte)
     * written by {@code Lucene104ScalarQuantizedVectorsWriter} (ScalarEncoding#PACKED_NIBBLE format).
     * <p>
     * The unpacked input comes from {@link OptimizedScalarQuantizer#scalarQuantize}, which quantizes a float
     * vector into one byte per element in natural order: unpacked = [v0, v1, v2, ..., v_{N-1}] where N = dims.
     * <p>
     * The packed format pairs elements that are packedLength ({@param unpacked} length / 2) apart. For example,
     * with dims=8, unpacked.length is 8 and packedLength is 4:
     *   - {@code packed[0] = (v0 << 4) | v4}
     *   - {@code packed[1] = (v1 << 4) | v5}
     *   - {@code packed[2] = (v2 << 4) | v6}
     *   - {@code packed[3] = (v3 << 4) | v7}
     * <p>
     * Or, visually,
     * UNPACKED (8 bytes, natural vector order, one 4-bit value per byte):
     *   index:   0     1     2     3     4     5     6     7
     *          [v0]  [v1]  [v2]  [v3]  [v4]  [v5]  [v6]  [v7]
     *   PACKED (4 bytes, on disk, two 4-bit values per byte):
     *   index:      0          1          2          3
     *          [v0  | v4]  [v1 | v5]  [v2 | v6]  [v3 | v7]
     *           hi    lo    hi   lo    hi   lo    hi   lo
     *          7..4  3..0  7..4 3..0  7..4 3..0  7..4 3..0
     */
    public static byte[] packNibbles(byte[] unpacked) {
        int packedLength = unpacked.length / 2;
        byte[] packed = new byte[packedLength];
        for (int i = 0; i < packedLength; i++) {
            packed[i] = (byte) ((unpacked[i] << 4) | (unpacked[i + packedLength] & 0x0F));
        }
        return packed;
    }

    /**
     * Unpacks "nibble-packed" int4 values (two values per byte) into a byte[] (one value per byte)
     */
    public static byte[] unpackNibbles(byte[] packed, int dims) {
        byte[] unpacked = new byte[dims];
        int packedLen = packed.length;
        for (int i = 0; i < packedLen; i++) {
            unpacked[i] = (byte) ((packed[i] & 0xFF) >> 4);
            unpacked[i + packedLen] = (byte) (packed[i] & 0x0F);
        }
        return unpacked;
    }
}
