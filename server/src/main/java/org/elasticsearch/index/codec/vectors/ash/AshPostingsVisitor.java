/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.elasticsearch.index.codec.vectors.diskbbq.DocIdsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata;
import org.elasticsearch.search.vectors.BulkKnnCollector;
import org.elasticsearch.simdvec.AsymmetricHashingScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.function.IntFunction;

import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * PostingVisitor for ASH-encoded posting lists.
 * <p>
 * Reads bit-packed codes with float32 scale/offset per vector and scores them
 * asymmetrically using the precomputed query transform. The on-disk format per block is:
 * <pre>
 *   [docIds][packed_codes × blockSize][scales × blockSize][offsets × blockSize][docSums × blockSize]
 * </pre>
 * For EUCLIDEAN similarity, each block additionally contains:
 * <pre>
 *   [vecCentroidDots × blockSize][vecCentroidSqDists × blockSize]
 * </pre>
 * <p>
 * Two scoring paths are supported:
 * <ul>
 *   <li>Float path ({@code queryBitsPerDim == 0}): full-precision projected query × packed document codes</li>
 *   <li>Integer path ({@code queryBitsPerDim > 0}): query quantized to {@code queryBitsPerDim} bits,
 *       scored via AND+popcount with per-vector docSum correction</li>
 * </ul>
 * <p>
 * EUCLIDEAN scoring implements the ASH paper Appendix A (Eq. A.2):
 * <pre>
 *   ‖q - x‖² = ‖q - μ*‖² + ‖x - μ*‖² - 2·(⟨q,x⟩ - ⟨μ*,x⟩ - ⟨q,μ*⟩ + ‖μ*‖²)
 * </pre>
 */
public class AshPostingsVisitor implements IVFVectorsReader.PostingVisitor {

    /** Strategy for computing the approximate dot product from packed codes. */
    @FunctionalInterface
    private interface DotProductScorer {
        float score(byte[] packedCodes, int codeOffset, float[] docConstants);
    }

    private final IndexInput indexInput;
    private final Bits acceptDocs;
    private final int nDims;
    private final int bitsPerDim;
    private final int packedCodeBytes;
    private final int planeBytes;
    private final VectorSimilarityFunction similarityFunction;

    // Raw query vector — retained for exact centroid dot products at query time
    private final float[] query;

    // Centroid lookup: ordinal → float[] centroid vector (reads from centroid file)
    private final IntFunction<float[]> centroidReader;

    // Precomputed query transform: queryTransformed = query @ W (raw projection, not centered)
    private final float[] queryTransformed;

    // Scoring strategy: float or integer path, selected once at construction time
    private final DotProductScorer dotProductScorer;
    // Pre-allocated query constants array: [queryDotCentroid, invQScale, qOffset, constantCorrection]
    // queryDotCentroid is set per-cluster in resetPostingsScorer; the rest are set once in constructor.
    private final float[] queryConstants;
    // Pre-allocated per-vector constants scratch: [scale, offset, docSum]
    private final float[] docConstants;

    // Scratch buffers for bulk I/O
    private final DocIdsWriter idsWriter = new DocIdsWriter();
    private final int[] docIdsScratch = new int[BULK_SIZE];
    private final int[] offsetsScratch = new int[BULK_SIZE];
    private final float[] scores = new float[BULK_SIZE];
    private final byte[] bulkCodeBuf;
    private final float[] bulkScales = new float[BULK_SIZE];
    private final float[] bulkOffsets = new float[BULK_SIZE];
    private final int[] bulkDocSums = new int[BULK_SIZE];
    // EUCLIDEAN-only bulk buffers
    private final float[] bulkVecCentroidDots;
    private final float[] bulkVecCentroidSqDists;

    // Per-posting-list state
    private int vectors;
    private byte docEncoding;
    private int docBase;
    private float currentQueryDotCentroid;
    // EUCLIDEAN per-posting-list state (Appendix A, Eq. A.2)
    private float currentQueryCentroidSqDist;
    private float currentCentroidNormSq;

    /**
     * @param wT transposed projection matrix W^T in row-major order, shape (nDims, originalDim)
     * @param originalDim original vector dimensionality (number of columns in wT)
     * @param query the raw query vector
     * @param similarityFunction the vector similarity function for score conversion
     * @param indexInput input for reading posting list data
     * @param acceptDocs live docs filter
     * @param bitsPerDim bits per dimension for document codes
     * @param queryBitsPerDim bits per dimension for query quantization (0 for float path)
     * @param centroidReader function mapping centroid ordinal to float[] centroid vector
     */
    public AshPostingsVisitor(
        float[] wT,
        int originalDim,
        float[] query,
        VectorSimilarityFunction similarityFunction,
        IndexInput indexInput,
        Bits acceptDocs,
        int bitsPerDim,
        int queryBitsPerDim,
        IntFunction<float[]> centroidReader
    ) {
        this.indexInput = indexInput;
        this.acceptDocs = acceptDocs;
        this.nDims = wT.length / originalDim;
        this.bitsPerDim = bitsPerDim;
        this.planeBytes = (nDims + 7) >>> 3;
        this.packedCodeBytes = bitsPerDim * planeBytes;
        this.similarityFunction = similarityFunction;
        this.query = query;
        this.centroidReader = centroidReader;

        boolean isEuclidean = similarityFunction == VectorSimilarityFunction.EUCLIDEAN;
        this.bulkVecCentroidDots = isEuclidean ? new float[BULK_SIZE] : null;
        this.bulkVecCentroidSqDists = isEuclidean ? new float[BULK_SIZE] : null;

        // Precompute query projection: queryTransformed[j] = dot(query, wT[j*originalDim .. (j+1)*originalDim))
        this.queryTransformed = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            queryTransformed[j] = ESVectorUtil.dotProduct(query, 0, wT, j * originalDim, originalDim);
        }

        // Shared query/doc constants arrays used by both float and integer scoring paths
        this.queryConstants = new float[AsymmetricHashingScorer.QC_LENGTH];
        this.docConstants = new float[AsymmetricHashingScorer.DC_LENGTH];

        // Integer scoring setup: quantize projected query to queryBitsPerDim bits
        if (queryBitsPerDim > 0) {
            float qMin = Float.MAX_VALUE, qMax = -Float.MAX_VALUE;
            for (int j = 0; j < nDims; j++) {
                qMin = Math.min(qMin, queryTransformed[j]);
                qMax = Math.max(qMax, queryTransformed[j]);
            }
            float range = qMax - qMin;
            int numQueryLevels = 1 << queryBitsPerDim;
            float qScale = range > 0 ? (numQueryLevels - 1) / range : 1.0f;
            float invQScale = range > 0 ? range / (numQueryLevels - 1) : 0f;

            byte[] queryQuantized = new byte[queryBitsPerDim * planeBytes];
            int unsignedSum = 0;
            for (int j = 0; j < nDims; j++) {
                int level = Math.clamp(Math.round((queryTransformed[j] - qMin) * qScale), 0, numQueryLevels - 1);
                unsignedSum += level;
                int byteIdx = j >>> 3;
                int bitIdx = 7 - (j & 7);
                for (int p = 0; p < queryBitsPerDim; p++) {
                    if (((level >> p) & 1) != 0) {
                        queryQuantized[p * planeBytes + byteIdx] |= (byte) (1 << bitIdx);
                    }
                }
            }
            // constantCorrection accounts for the centering bias when using unsigned integer scoring:
            // dot(qt_float, centeredCode) = dot(qt_float, unsignedCode) - centerOffset * sum(qt_float)
            // The sum(qt_float) term ≈ invQScale * unsignedQuerySum + qOffset * nDims, precomputed here.
            float centerOffset = ((1 << bitsPerDim) - 1) / 2.0f;
            float constantCorrection = centerOffset * (unsignedSum * invQScale + qMin * nDims);
            // queryDotCentroid (index 0) is set per-cluster in resetPostingsScorer
            queryConstants[AsymmetricHashingScorer.QC_INV_Q_SCALE] = invQScale;
            queryConstants[AsymmetricHashingScorer.QC_Q_OFFSET] = qMin;
            queryConstants[AsymmetricHashingScorer.QC_CONSTANT_CORRECTION] = constantCorrection;
            final int qBits = queryBitsPerDim;
            this.dotProductScorer = (packedCodes, codeOffset, dc) -> AsymmetricHashingScorer.scoreInteger(
                queryQuantized,
                qBits,
                queryConstants,
                packedCodes,
                codeOffset,
                bitsPerDim,
                planeBytes,
                dc
            );
        } else {
            this.dotProductScorer = (packedCodes, codeOffset, dc) -> AsymmetricHashingScorer.score(
                queryTransformed,
                queryConstants,
                packedCodes,
                codeOffset,
                nDims,
                bitsPerDim,
                dc
            );
        }

        this.bulkCodeBuf = new byte[BULK_SIZE * packedCodeBytes];
    }

    @Override
    public int resetPostingsScorer(PostingMetadata metadata) throws IOException {
        indexInput.seek(metadata.offset());
        vectors = indexInput.readVInt();
        int centroidOrd = indexInput.readVInt();

        // Compute exact query·centroid dot product from the real centroid vector
        float[] centroid = centroidReader.apply(centroidOrd);
        currentQueryDotCentroid = ESVectorUtil.dotProduct(query, centroid);

        // EUCLIDEAN: read centroid norm squared from header and compute ‖q-μ*‖²
        if (similarityFunction == VectorSimilarityFunction.EUCLIDEAN) {
            currentCentroidNormSq = Float.intBitsToFloat(indexInput.readInt());
            currentQueryCentroidSqDist = ESVectorUtil.squareDistance(query, centroid);
        }

        docEncoding = indexInput.readByte();
        docBase = 0;

        queryConstants[AsymmetricHashingScorer.QC_QUERY_DOT_CENTROID] = currentQueryDotCentroid;

        return vectors;
    }

    @Override
    public int visit(KnnCollector knnCollector) throws IOException {
        int scoredDocs = 0;

        int limit = vectors - BULK_SIZE + 1;
        int i = 0;
        for (; i < limit; i += BULK_SIZE) {
            scoredDocs += processBlock(knnCollector, BULK_SIZE);
        }
        // Tail
        if (i < vectors) {
            int tailSize = vectors - i;
            scoredDocs += processBlock(knnCollector, tailSize);
        }
        if (scoredDocs > 0) {
            knnCollector.incVisitedCount(scoredDocs);
        }
        return scoredDocs;
    }

    private int processBlock(KnnCollector knnCollector, int blockSize) throws IOException {
        readDocIds(blockSize);
        int docsToScore = filterAcceptedDocs(blockSize);
        boolean isEuclidean = similarityFunction == VectorSimilarityFunction.EUCLIDEAN;
        if (docsToScore == 0) {
            // Skip the entire block: codes + scales + offsets + docSums (+ EUCLIDEAN fields)
            long bytesToSkip = (long) blockSize * packedCodeBytes + (long) blockSize * Float.BYTES * 2 + (long) blockSize * Integer.BYTES;
            if (isEuclidean) {
                bytesToSkip += (long) blockSize * Float.BYTES * 2;
            }
            indexInput.skipBytes(bytesToSkip);
            return 0;
        }

        // Read structure-of-arrays: codes, scales, offsets, docSums
        indexInput.readBytes(bulkCodeBuf, 0, blockSize * packedCodeBytes);
        indexInput.readFloats(bulkScales, 0, blockSize);
        indexInput.readFloats(bulkOffsets, 0, blockSize);
        indexInput.readInts(bulkDocSums, 0, blockSize);
        // EUCLIDEAN: read ⟨μ*,x⟩ and ‖x-μ*‖² per vector (float32)
        if (isEuclidean) {
            indexInput.readFloats(bulkVecCentroidDots, 0, blockSize);
            indexInput.readFloats(bulkVecCentroidSqDists, 0, blockSize);
        }

        // Score each vector
        float maxScore = Float.NEGATIVE_INFINITY;
        for (int j = 0; j < blockSize; j++) {
            if (docIdsScratch[j] != -1) {
                float scale = bulkScales[j];
                float offset = bulkOffsets[j];
                // Compute approximate ⟨q,x⟩ via ASH (same for all similarity functions)
                docConstants[AsymmetricHashingScorer.DC_SCALE] = scale;
                docConstants[AsymmetricHashingScorer.DC_OFFSET] = offset;
                docConstants[AsymmetricHashingScorer.DC_DOC_SUM] = bulkDocSums[j];
                float approxDotProduct = dotProductScorer.score(bulkCodeBuf, j * packedCodeBytes, docConstants);
                // Convert raw dot product to similarity score
                if (isEuclidean) {
                    // Appendix A, Eq. A.2:
                    // ‖q-x‖² = ‖q-μ*‖² + ‖x-μ*‖² - 2·(⟨q,x⟩ - ⟨μ*,x⟩ - ⟨q,μ*⟩ + ‖μ*‖²)
                    float vecCentroidDot = bulkVecCentroidDots[j];
                    float vecCentroidSqDist = bulkVecCentroidSqDists[j];
                    float sqDist = currentQueryCentroidSqDist + vecCentroidSqDist - 2 * (approxDotProduct - vecCentroidDot
                        - currentQueryDotCentroid + currentCentroidNormSq);
                    // Clamp to non-negative (floating point rounding can produce small negatives)
                    scores[j] = 1 / (1 + Math.max(0, sqDist));
                } else {
                    scores[j] = convertScore(approxDotProduct);
                }
                if (scores[j] > maxScore) {
                    maxScore = scores[j];
                }
            }
        }

        if (knnCollector.minCompetitiveSimilarity() < maxScore) {
            collectBulk(knnCollector, blockSize, docsToScore, maxScore);
        }
        return docsToScore;
    }

    private float convertScore(float rawDotProduct) {
        return switch (similarityFunction) {
            case EUCLIDEAN -> throw new IllegalStateException("EUCLIDEAN handled inline in processBlock");
            case COSINE, DOT_PRODUCT -> (1 + rawDotProduct) / 2;
            case MAXIMUM_INNER_PRODUCT -> rawDotProduct >= 0 ? rawDotProduct + 1 : 1 / (1 - rawDotProduct);
        };
    }

    private void readDocIds(int count) throws IOException {
        idsWriter.readInts(indexInput, count, docEncoding, docIdsScratch);
        for (int j = 0; j < count; j++) {
            docBase += docIdsScratch[j];
            docIdsScratch[j] = docBase;
        }
    }

    private int filterAcceptedDocs(int bulkSize) {
        if (acceptDocs == null) {
            return bulkSize;
        }
        int docToScore = 0;
        for (int ii = 0; ii < bulkSize; ii++) {
            if (docIdsScratch[ii] == -1 || acceptDocs.get(docIdsScratch[ii]) == false) {
                docIdsScratch[ii] = -1;
            } else {
                offsetsScratch[docToScore] = ii;
                docToScore++;
            }
        }
        return docToScore;
    }

    private void collectBulk(KnnCollector knnCollector, int bulkSize, int docsToScore, float maxScore) {
        if (knnCollector instanceof BulkKnnCollector bulkCollector) {
            if (docsToScore == bulkSize) {
                bulkCollector.bulkCollect(docIdsScratch, scores, bulkSize, maxScore);
                return;
            }
            for (int ii = 0; ii < docsToScore; ii++) {
                int offset = offsetsScratch[ii];
                docIdsScratch[ii] = docIdsScratch[offset];
                scores[ii] = scores[offset];
            }
            bulkCollector.bulkCollect(docIdsScratch, scores, docsToScore, maxScore);
            return;
        }
        for (int ii = 0; ii < bulkSize; ii++) {
            final int doc = docIdsScratch[ii];
            if (doc != -1) {
                knnCollector.collect(doc, scores[ii]);
            }
        }
    }
}
