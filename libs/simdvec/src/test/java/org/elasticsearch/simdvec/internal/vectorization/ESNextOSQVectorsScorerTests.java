/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

public class ESNextOSQVectorsScorerTests extends BaseVectorizationTests {

    private final DirectoryType directoryType;
    private final byte indexBits;
    private static final byte queryBits = 4;

    public enum DirectoryType {
        NIOFS,
        MMAP
    }

    public ESNextOSQVectorsScorerTests(DirectoryType directoryType, byte indexBits) {
        this.directoryType = directoryType;
        this.indexBits = indexBits;
    }

    public void testQuantizeScore() throws Exception {

        final int dimensions = random().nextInt(1, 2000);
        final int length2 = BQVectorUtils.discretize(dimensions, 64) / 8;

        final int length = switch (indexBits) {
            case 1 -> ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 2 -> ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 4 -> ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC.getDocPackedLength(dimensions);
            case 7 -> ESNextDiskBBQVectorsFormat.QuantEncoding.SEVEN_BIT_SYMMETRIC.getDocPackedLength(dimensions);
            default -> throw new IllegalArgumentException("Unsupported bits: " + indexBits);
        };

        final int numVectors = random().nextInt(1, 100);
        final byte[] vector = new byte[length];
        final int queryBytes = indexBits == 7 ? dimensions : length * (queryBits / indexBits);

        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    if (indexBits == 7) {
                        for (int j = 0; j < dimensions; j++) {
                            vector[j] = (byte) random().nextInt(128);
                        }
                    } else {
                        random().nextBytes(vector);
                    }
                    out.writeBytes(vector, 0, length);
                }
            }
            final byte[] query = new byte[queryBytes];
            if (indexBits == 7) {
                for (int j = 0; j < dimensions; j++) {
                    query[j] = (byte) random().nextInt(128);
                }
            } else {
                random().nextBytes(query);
            }
            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                final IndexInput slice = in.slice("test", 0, (long) length * numVectors);
                final ES92Int7VectorsScorer defaultInt7Scorer;
                final ES92Int7VectorsScorer panamaInt7Scorer;
                final ESNextOSQVectorsScorer defaultScorer;
                final ESNextOSQVectorsScorer panamaScorer;
                if (indexBits == 7) {
                    defaultInt7Scorer = defaultProvider().newES92Int7VectorsScorer(slice, dimensions, ESNextOSQVectorsScorer.BULK_SIZE);
                    panamaInt7Scorer = maybePanamaProvider().newES92Int7VectorsScorer(in, dimensions, ESNextOSQVectorsScorer.BULK_SIZE);
                    defaultScorer = null;
                    panamaScorer = null;
                } else {
                    defaultScorer = defaultProvider().newESNextOSQVectorsScorer(
                        slice,
                        queryBits,
                        indexBits,
                        dimensions,
                        length,
                        ESNextOSQVectorsScorer.BULK_SIZE
                    );
                    panamaScorer = maybePanamaProvider().newESNextOSQVectorsScorer(
                        in,
                        queryBits,
                        indexBits,
                        dimensions,
                        length,
                        ESNextOSQVectorsScorer.BULK_SIZE
                    );
                    defaultInt7Scorer = null;
                    panamaInt7Scorer = null;
                }
                for (int i = 0; i < numVectors; i++) {
                    if (indexBits == 7) {
                        assertEquals(defaultInt7Scorer.int7DotProduct(query), panamaInt7Scorer.int7DotProduct(query));
                    } else {
                        assertEquals(defaultScorer.quantizeScore(query), panamaScorer.quantizeScore(query));
                    }
                    assertEquals(in.getFilePointer(), slice.getFilePointer());
                }
                assertEquals((long) length * numVectors, slice.getFilePointer());
            }
        }
    }

    public void testScore() throws Exception {
        final int maxDims = random().nextInt(1, 1000) * 2;
        final int dimensions = random().nextInt(1, maxDims);

        final int length = switch (indexBits) {
            case 1 -> ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 2 -> ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 4 -> ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC.getDocPackedLength(dimensions);
            case 7 -> ESNextDiskBBQVectorsFormat.QuantEncoding.SEVEN_BIT_SYMMETRIC.getDocPackedLength(dimensions);
            default -> throw new IllegalArgumentException("Unsupported bits: " + indexBits);
        };

        final int queryBytes = indexBits == 7 ? dimensions : length * (queryBits / indexBits);

        final int numVectors = random().nextInt(10, 50);
        float[][] vectors = new float[numVectors][dimensions];
        final int[] scratch = new int[dimensions];
        final float[] residualScratch = new float[dimensions];
        final byte[] qVector = new byte[length];
        final float[] centroid = new float[dimensions];
        VectorSimilarityFunction similarityFunction = randomFrom(VectorSimilarityFunction.values());
        randomVector(centroid, similarityFunction);
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
        int padding = random().nextInt(100);
        byte[] paddingBytes = new byte[padding];
        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
                random().nextBytes(paddingBytes);
                out.writeBytes(paddingBytes, 0, padding);
                for (float[] vector : vectors) {
                    randomVector(vector, similarityFunction);
                    if (indexBits == 7) {
                        OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(
                            vector,
                            residualScratch,
                            scratch,
                            (byte) 7,
                            centroid
                        );
                        for (int j = 0; j < dimensions; j++) {
                            qVector[j] = (byte) scratch[j];
                        }
                        out.writeBytes(qVector, 0, dimensions);
                        out.writeInt(Float.floatToIntBits(result.lowerInterval()));
                        out.writeInt(Float.floatToIntBits(result.upperInterval()));
                        out.writeInt(Float.floatToIntBits(result.additionalCorrection()));
                        out.writeInt(result.quantizedComponentSum());
                    } else {
                        OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(
                            vector,
                            residualScratch,
                            scratch,
                            (byte) 1,
                            centroid
                        );
                        ESVectorUtil.packAsBinary(scratch, qVector);
                        out.writeBytes(qVector, 0, qVector.length);
                        out.writeInt(Float.floatToIntBits(result.lowerInterval()));
                        out.writeInt(Float.floatToIntBits(result.upperInterval()));
                        out.writeInt(Float.floatToIntBits(result.additionalCorrection()));
                        out.writeShort((short) result.quantizedComponentSum());
                    }
                }
            }
            final float[] query = new float[dimensions];
            randomVector(query, similarityFunction);
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
                query,
                residualScratch,
                scratch,
                indexBits == 7 ? (byte) 7 : queryBits,
                centroid
            );
            final byte[] quantizeQuery = new byte[queryBytes];
            if (indexBits == 7) {
                for (int i = 0; i < dimensions; i++) {
                    quantizeQuery[i] = (byte) scratch[i];
                }
            } else {
                ESVectorUtil.transposeHalfByte(scratch, quantizeQuery);
            }
            final float centroidDp = VectorUtil.dotProduct(centroid, centroid);
            final float[] floatScratch = new float[3];
            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                in.seek(padding);
                int correctionBytes = indexBits == 7 ? 16 : 14;
                assertEquals(in.length(), padding + (long) numVectors * (length + correctionBytes));
                final IndexInput slice = in.slice("test", in.getFilePointer(), (long) (length + correctionBytes) * numVectors);
                for (int i = 0; i < numVectors; i++) {
                    if (indexBits == 7) {
                        final var defaultScorer = defaultProvider().newES92Int7VectorsScorer(
                            in,
                            dimensions,
                            ESNextOSQVectorsScorer.BULK_SIZE
                        );
                        final var panamaScorer = maybePanamaProvider().newES92Int7VectorsScorer(
                            slice,
                            dimensions,
                            ESNextOSQVectorsScorer.BULK_SIZE
                        );
                        float defaultScore = defaultScorer.score(
                            quantizeQuery,
                            queryCorrections.lowerInterval(),
                            queryCorrections.upperInterval(),
                            queryCorrections.quantizedComponentSum(),
                            queryCorrections.additionalCorrection(),
                            similarityFunction,
                            centroidDp
                        );
                        float panamaScore = panamaScorer.score(
                            quantizeQuery,
                            queryCorrections.lowerInterval(),
                            queryCorrections.upperInterval(),
                            queryCorrections.quantizedComponentSum(),
                            queryCorrections.additionalCorrection(),
                            similarityFunction,
                            centroidDp
                        );
                        assertEquals(defaultScore, panamaScore, 1e-2f);
                    } else {
                        final var defaultScorer = defaultProvider().newESNextOSQVectorsScorer(
                            slice,
                            queryBits,
                            indexBits,
                            dimensions,
                            length,
                            ESNextOSQVectorsScorer.BULK_SIZE
                        );
                        final var panamaScorer = maybePanamaProvider().newESNextOSQVectorsScorer(
                            in,
                            queryBits,
                            indexBits,
                            dimensions,
                            length,
                            ESNextOSQVectorsScorer.BULK_SIZE
                        );
                        long qDist = defaultScorer.quantizeScore(quantizeQuery);
                        slice.readFloats(floatScratch, 0, 3);
                        int quantizedComponentSum = slice.readShort();
                        float defaulScore = defaultScorer.score(
                            queryCorrections.lowerInterval(),
                            queryCorrections.upperInterval(),
                            queryCorrections.quantizedComponentSum(),
                            queryCorrections.additionalCorrection(),
                            similarityFunction,
                            centroidDp,
                            floatScratch[0],
                            floatScratch[1],
                            quantizedComponentSum,
                            floatScratch[2],
                            qDist
                        );
                        qDist = panamaScorer.quantizeScore(quantizeQuery);
                        in.readFloats(floatScratch, 0, 3);
                        quantizedComponentSum = in.readShort();
                        float panamaScore = panamaScorer.score(
                            queryCorrections.lowerInterval(),
                            queryCorrections.upperInterval(),
                            queryCorrections.quantizedComponentSum(),
                            queryCorrections.additionalCorrection(),
                            similarityFunction,
                            centroidDp,
                            floatScratch[0],
                            floatScratch[1],
                            quantizedComponentSum,
                            floatScratch[2],
                            qDist
                        );
                        assertEquals(defaulScore, panamaScore, 1e-2f);
                    }
                    assertEquals(((long) (i + 1) * (length + correctionBytes)), slice.getFilePointer());
                    assertEquals(padding + ((long) (i + 1) * (length + correctionBytes)), in.getFilePointer());
                }
            }
        }
    }

    public void testScoreBulk() throws Exception {
        final int maxDims = random().nextInt(1, 1000) * 2;
        final int dimensions = random().nextInt(1, maxDims);

        final int length = switch (indexBits) {
            case 1 -> ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 2 -> ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 4 -> ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC.getDocPackedLength(dimensions);
            case 7 -> ESNextDiskBBQVectorsFormat.QuantEncoding.SEVEN_BIT_SYMMETRIC.getDocPackedLength(dimensions);
            default -> throw new IllegalArgumentException("Unsupported bits: " + indexBits);
        };
        final int queryBytes = indexBits == 7 ? dimensions : length * (queryBits / indexBits);

        final int numVectors = ESNextOSQVectorsScorer.BULK_SIZE * random().nextInt(1, 10);
        float[][] vectors = new float[numVectors][dimensions];
        final int[] scratch = new int[dimensions];
        final float[] residualScratch = new float[dimensions];
        final byte[] qVector = new byte[length];
        final float[] centroid = new float[dimensions];
        VectorSimilarityFunction similarityFunction = randomFrom(VectorSimilarityFunction.values());
        randomVector(centroid, similarityFunction);
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
        int padding = random().nextInt(100);
        byte[] paddingBytes = new byte[padding];
        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
                random().nextBytes(paddingBytes);
                out.writeBytes(paddingBytes, 0, padding);
                int limit = numVectors - ESNextOSQVectorsScorer.BULK_SIZE + 1;
                OptimizedScalarQuantizer.QuantizationResult[] results =
                    new OptimizedScalarQuantizer.QuantizationResult[ESNextOSQVectorsScorer.BULK_SIZE];
                for (int i = 0; i < limit; i += ESNextOSQVectorsScorer.BULK_SIZE) {
                    for (int j = 0; j < ESNextOSQVectorsScorer.BULK_SIZE; j++) {
                        randomVector(vectors[i + j], similarityFunction);
                        if (indexBits == 7) {
                            results[j] = quantizer.scalarQuantize(vectors[i + j], residualScratch, scratch, (byte) 7, centroid);
                            for (int k = 0; k < dimensions; k++) {
                                qVector[k] = (byte) scratch[k];
                            }
                            out.writeBytes(qVector, 0, dimensions);
                        } else {
                            results[j] = quantizer.scalarQuantize(vectors[i + j], residualScratch, scratch, (byte) 1, centroid);
                            ESVectorUtil.packAsBinary(scratch, qVector);
                            out.writeBytes(qVector, 0, qVector.length);
                        }
                    }
                    if (indexBits == 7) {
                        writeInt7Corrections(results, out);
                    } else {
                        writeCorrections(results, out);
                    }
                }
            }
            final float[] query = new float[dimensions];
            randomVector(query, similarityFunction);
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
                query,
                residualScratch,
                scratch,
                indexBits == 7 ? (byte) 7 : (byte) 4,
                centroid
            );
            final byte[] quantizeQuery = new byte[queryBytes];
            if (indexBits == 7) {
                for (int j = 0; j < dimensions; j++) {
                    quantizeQuery[j] = (byte) scratch[j];
                }
            } else {
                ESVectorUtil.transposeHalfByte(scratch, quantizeQuery);
            }
            final float centroidDp = VectorUtil.dotProduct(centroid, centroid);
            final float[] scoresDefault = new float[ESNextOSQVectorsScorer.BULK_SIZE];
            final float[] scoresPanama = new float[ESNextOSQVectorsScorer.BULK_SIZE];
            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                in.seek(padding);
                int correctionBytes = indexBits == 7 ? 16 : 14;
                assertEquals(in.length(), padding + (long) numVectors * (length + correctionBytes));
                for (int i = 0; i < numVectors; i += ESNextOSQVectorsScorer.BULK_SIZE) {
                    final IndexInput slice = in.slice(
                        "test",
                        in.getFilePointer(),
                        (long) (length + correctionBytes) * ESNextOSQVectorsScorer.BULK_SIZE
                    );
                    if (indexBits == 7) {
                        final var defaultScorer = defaultProvider().newES92Int7VectorsScorer(
                            in,
                            dimensions,
                            ESNextOSQVectorsScorer.BULK_SIZE
                        );
                        final var panamaScorer = maybePanamaProvider().newES92Int7VectorsScorer(
                            slice,
                            dimensions,
                            ESNextOSQVectorsScorer.BULK_SIZE
                        );
                        defaultScorer.scoreBulk(
                            quantizeQuery,
                            queryCorrections.lowerInterval(),
                            queryCorrections.upperInterval(),
                            queryCorrections.quantizedComponentSum(),
                            queryCorrections.additionalCorrection(),
                            similarityFunction,
                            centroidDp,
                            scoresDefault,
                            ESNextOSQVectorsScorer.BULK_SIZE
                        );
                        panamaScorer.scoreBulk(
                            quantizeQuery,
                            queryCorrections.lowerInterval(),
                            queryCorrections.upperInterval(),
                            queryCorrections.quantizedComponentSum(),
                            queryCorrections.additionalCorrection(),
                            similarityFunction,
                            centroidDp,
                            scoresPanama,
                            ESNextOSQVectorsScorer.BULK_SIZE
                        );
                    } else {
                        final var defaultScorer = defaultProvider().newESNextOSQVectorsScorer(
                            slice,
                            queryBits,
                            indexBits,
                            dimensions,
                            length,
                            ESNextOSQVectorsScorer.BULK_SIZE
                        );
                        final var panamaScorer = maybePanamaProvider().newESNextOSQVectorsScorer(
                            in,
                            queryBits,
                            indexBits,
                            dimensions,
                            length,
                            ESNextOSQVectorsScorer.BULK_SIZE
                        );
                        float defaultMaxScore = defaultScorer.scoreBulk(
                            quantizeQuery,
                            queryCorrections.lowerInterval(),
                            queryCorrections.upperInterval(),
                            queryCorrections.quantizedComponentSum(),
                            queryCorrections.additionalCorrection(),
                            similarityFunction,
                            centroidDp,
                            scoresDefault
                        );
                        float panamaMaxScore = panamaScorer.scoreBulk(
                            quantizeQuery,
                            queryCorrections.lowerInterval(),
                            queryCorrections.upperInterval(),
                            queryCorrections.quantizedComponentSum(),
                            queryCorrections.additionalCorrection(),
                            similarityFunction,
                            centroidDp,
                            scoresPanama
                        );
                        assertEquals(defaultMaxScore, panamaMaxScore, 1e-2f);
                    }
                    for (int j = 0; j < ESNextOSQVectorsScorer.BULK_SIZE; j++) {
                        assertEquals(scoresDefault[j], scoresPanama[j], 1e-2f);
                    }
                    assertEquals(((long) (ESNextOSQVectorsScorer.BULK_SIZE) * (length + correctionBytes)), slice.getFilePointer());
                    assertEquals(
                        padding + ((long) (i + ESNextOSQVectorsScorer.BULK_SIZE) * (length + correctionBytes)),
                        in.getFilePointer()
                    );
                }
            }
        }
    }

    private static void writeCorrections(OptimizedScalarQuantizer.QuantizationResult[] corrections, IndexOutput out) throws IOException {
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.upperInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            int targetComponentSum = correction.quantizedComponentSum();
            out.writeShort((short) targetComponentSum);
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
        }
    }

    private static void writeInt7Corrections(OptimizedScalarQuantizer.QuantizationResult[] corrections, IndexOutput out)
        throws IOException {
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.upperInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(correction.quantizedComponentSum());
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
        }
    }

    private void randomVector(float[] vector, VectorSimilarityFunction vectorSimilarityFunction) {
        for (int i = 0; i < vector.length; i++) {
            vector[i] = random().nextFloat();
        }
        if (vectorSimilarityFunction != VectorSimilarityFunction.EUCLIDEAN) {
            VectorUtil.l2normalize(vector);
        }
    }

    private Directory newParametrizedDirectory() throws IOException {
        return switch (directoryType) {
            case NIOFS -> new NIOFSDirectory(createTempDir());
            case MMAP -> new MMapDirectory(createTempDir());
        };
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        return () -> Stream.of((byte) 1, (byte) 2, (byte) 4, (byte) 7)
            .flatMap(i -> Arrays.stream(DirectoryType.values()).map(f -> new Object[] { f, i }))
            .iterator();
    }
}
