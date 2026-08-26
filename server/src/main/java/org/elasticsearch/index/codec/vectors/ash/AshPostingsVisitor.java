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
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.index.codec.vectors.diskbbq.DocIdsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata;
import org.elasticsearch.search.vectors.BulkKnnCollector;
import org.elasticsearch.simdvec.AsymmetricHashingVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * PostingVisitor for ASH-encoded posting lists.
 * <p>
 * Reads bit-packed codes with float32 scale/offset per vector and scores them
 * asymmetrically using the precomputed query transform. The on-disk format per block is:
 * <pre>
 *   [docIds][packed_codes × blockSize][corrections × blockSize]
 * </pre>
 * Corrections use AoS layout: [scale, offset, docSum, vecCentroidDot, vecCentroidSqDist] per vector.
 * <p>
 * Two scoring paths are supported:
 * <ul>
 *   <li>Float path ({@code queryBitsPerDim == 0}): full-precision projected query × packed document codes</li>
 *   <li>Integer path ({@code queryBitsPerDim > 0}): query quantized to {@code queryBitsPerDim} bits,
 *       scored via AND+popcount with per-vector docSum correction</li>
 * </ul>
 * <p>
 * Scoring of packed codes is delegated to an {@link AsymmetricHashingVectorsScorer} which
 * reads directly from the {@link IndexInput}, avoiding heap copies when backed by mmap.
 * <p>
 * EUCLIDEAN scoring implements the ASH paper Appendix A (Eq. A.2):
 * <pre>
 *   ‖q - x‖² = ‖q - μ*‖² + ‖x - μ*‖² - 2·(⟨q,x⟩ - ⟨μ*,x⟩ - ⟨q,μ*⟩ + ‖μ*‖²)
 * </pre>
 */
public class AshPostingsVisitor implements IVFVectorsReader.PostingVisitor {

    // --- Per-vector correction layout (AoS: all fields interleaved per vector) ---
    /** Byte offset of scale (float32) within a correction entry. */
    public static final int CORR_SCALE = 0;
    /** Byte offset of offset (float32) within a correction entry. */
    public static final int CORR_OFFSET = Float.BYTES;
    /** Byte offset of docSum (int32) within a correction entry. */
    public static final int CORR_DOC_SUM = 2 * Float.BYTES;
    /** Byte offset of the vector-centroid dot product (float32) within a correction entry (EUCLIDEAN; 0 otherwise). */
    public static final int CORR_VEC_CENTROID_DOT = 3 * Float.BYTES;
    /** Byte offset of the vector-centroid squared distance (float32) within a correction entry (EUCLIDEAN; 0 otherwise). */
    public static final int CORR_VEC_CENTROID_SQ_DIST = 4 * Float.BYTES;
    /** Total bytes per correction entry. */
    public static final int CORRECTION_BYTES = 5 * Float.BYTES;

    private static final int QC_QUERY_DOT_CENTROID = 0;
    private static final int QC_INV_Q_SCALE = 1;
    private static final int QC_Q_OFFSET = 2;
    private static final int QC_CONSTANT_CORRECTION = 3;
    private static final int QC_LENGTH = 4;

    /** Strategy for converting a raw dot product into a Lucene similarity score. */
    @FunctionalInterface
    private interface SimilarityConverter {
        float convert(float approxDotProduct, byte[] corrections, int correctionOffset);
    }

    private final IndexInput indexInput;
    private final Bits acceptDocs;
    private final int nDims;
    private final int bitsPerDim;
    private final int packedCodeBytes;
    private final int queryBitsPerDim;
    private final VectorSimilarityFunction similarityFunction;

    // Raw query vector — retained for exact centroid dot products at query time
    private final float[] query;

    // Centroid lookup: ordinal → float[] centroid vector (reads from centroid file)
    private final CheckedIntFunction<float[], IOException> centroidReader;

    // Precomputed query transform: queryTransformed = query @ W (raw projection, not centered)
    private final float[] queryTransformed;

    // The scorer that reads packed codes from the IndexInput and produces raw dot products.
    // May be scalar or SIMD-accelerated depending on IndexInput type and bit combination.
    private final AsymmetricHashingVectorsScorer scorer;

    // Similarity conversion strategy: selected once at construction time
    private final SimilarityConverter similarityConverter;
    // Pre-allocated query constants array: [queryDotCentroid, invQScale, qOffset, constantCorrection]
    // queryDotCentroid is set per-cluster in resetPostingsScorer; the rest are set once in constructor.
    private final float[] queryConstants;

    // For integer path: quantized query in bit-plane format
    private final byte[] queryQuantized;

    // Scratch buffers for bulk I/O
    private final DocIdsWriter idsWriter = new DocIdsWriter();
    private final int[] docIdsScratch = new int[BULK_SIZE];
    private final int[] offsetsScratch = new int[BULK_SIZE];
    private final float[] scores = new float[BULK_SIZE];
    // Per-vector corrections in AoS layout: [scale, offset, docSum, vecCentroidDot, vecCentroidSqDist] × blockSize
    private final byte[] bulkCorrectionsBuf = new byte[BULK_SIZE * CORRECTION_BYTES];

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
     * @param scorer the scorer to use for reading and scoring packed codes
     * @param indexInput input for reading posting list data (doc IDs, corrections)
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
        AsymmetricHashingVectorsScorer scorer,
        IndexInput indexInput,
        Bits acceptDocs,
        int bitsPerDim,
        int queryBitsPerDim,
        CheckedIntFunction<float[], IOException> centroidReader
    ) {
        this.indexInput = indexInput;
        this.acceptDocs = acceptDocs;
        this.nDims = wT.length / originalDim;
        this.bitsPerDim = bitsPerDim;
        this.packedCodeBytes = bitsPerDim * ((nDims + 7) >>> 3);
        this.queryBitsPerDim = queryBitsPerDim;
        this.similarityFunction = similarityFunction;
        this.query = query;
        this.centroidReader = centroidReader;
        this.scorer = scorer;

        int planeBytes = (nDims + 7) >>> 3;

        // Precompute query projection: queryTransformed[j] = dot(query, wT[j*originalDim .. (j+1)*originalDim))
        this.queryTransformed = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            queryTransformed[j] = ESVectorUtil.dotProduct(query, 0, wT, j * originalDim, originalDim);
        }

        // Shared query constants array used by both float and integer scoring paths
        this.queryConstants = new float[QC_LENGTH];

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

            this.queryQuantized = new byte[queryBitsPerDim * planeBytes];
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
            float centerOffset = ((1 << bitsPerDim) - 1) / 2.0f;
            float constantCorrection = centerOffset * (unsignedSum * invQScale + qMin * nDims);
            queryConstants[QC_INV_Q_SCALE] = invQScale;
            queryConstants[QC_Q_OFFSET] = qMin;
            queryConstants[QC_CONSTANT_CORRECTION] = constantCorrection;
        } else {
            this.queryQuantized = null;
        }

        // Similarity conversion strategy
        this.similarityConverter = switch (similarityFunction) {
            case EUCLIDEAN -> (dot, corr, corrOff) -> {
                float vecCentroidDot = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(corr, corrOff + CORR_VEC_CENTROID_DOT));
                float vecCentroidSqDist = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(corr, corrOff + CORR_VEC_CENTROID_SQ_DIST));
                float sqDist = currentQueryCentroidSqDist + vecCentroidSqDist - 2 * (dot - vecCentroidDot - currentQueryDotCentroid
                    + currentCentroidNormSq);
                return 1 / (1 + Math.max(0, sqDist));
            };
            case COSINE, DOT_PRODUCT -> (dot, corr, corrOff) -> VectorUtil.normalizeToUnitInterval(dot);
            case MAXIMUM_INNER_PRODUCT -> (dot, corr, corrOff) -> VectorUtil.scaleMaxInnerProductScore(dot);
        };
    }

    @Override
    public int resetPostingsScorer(PostingMetadata metadata) throws IOException {
        indexInput.seek(metadata.offset());
        vectors = indexInput.readVInt();
        int centroidOrd = indexInput.readVInt();

        // Compute exact query·centroid dot product from the real centroid vector
        float[] centroid = centroidReader.apply(centroidOrd);
        currentQueryDotCentroid = ESVectorUtil.dotProduct(query, centroid);

        // Read centroid norm squared from header; compute ‖q-μ*‖² for EUCLIDEAN
        currentCentroidNormSq = Float.intBitsToFloat(indexInput.readInt());
        if (similarityFunction == VectorSimilarityFunction.EUCLIDEAN) {
            currentQueryCentroidSqDist = ESVectorUtil.squareDistance(query, centroid);
        }

        docEncoding = indexInput.readByte();
        docBase = 0;

        queryConstants[QC_QUERY_DOT_CENTROID] = currentQueryDotCentroid;

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
        if (docsToScore == 0) {
            // Skip the entire block: codes + corrections
            long bytesToSkip = (long) blockSize * packedCodeBytes + (long) blockSize * CORRECTION_BYTES;
            indexInput.skipBytes(bytesToSkip);
            return 0;
        }

        // Step 1: Read packed codes via the scorer (produces raw dot products).
        // The scorer reads from the same IndexInput and advances past all code bytes.
        if (queryBitsPerDim > 0) {
            scorer.scoreIntegerBulk(queryQuantized, queryBitsPerDim, scores, blockSize);
        } else {
            scorer.scoreFloatBulk(queryTransformed, scores, blockSize);
        }

        // Step 2: Read corrections (IndexInput is now past the codes, at the corrections)
        indexInput.readBytes(bulkCorrectionsBuf, 0, blockSize * CORRECTION_BYTES);

        // Step 3: Apply per-vector corrections and similarity conversion
        float maxScore = Float.NEGATIVE_INFINITY;
        for (int j = 0; j < blockSize; j++) {
            if (docIdsScratch[j] != -1) {
                int corrOff = j * CORRECTION_BYTES;
                float approxDotProduct = applyCorrections(scores[j], corrOff);
                scores[j] = similarityConverter.convert(approxDotProduct, bulkCorrectionsBuf, corrOff);
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

    /**
     * Applies per-vector corrections to a raw dot product to produce the approximate dot product.
     * <p>
     * For the float path: {@code approxDot = rawDot * scale + queryDotCentroid + offset}
     * For the integer path: {@code approxDot = (invQScale * rawDot + qOffset * docSum - constantCorrection) * scale + qdc + offset}
     */
    private float applyCorrections(float rawScore, int corrOff) {
        float scale = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(bulkCorrectionsBuf, corrOff + CORR_SCALE));
        float offset = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(bulkCorrectionsBuf, corrOff + CORR_OFFSET));

        if (queryBitsPerDim > 0) {
            // Integer path: rawScore is the raw integer dot product
            float invQScale = queryConstants[QC_INV_Q_SCALE];
            float qOffset = queryConstants[QC_Q_OFFSET];
            float constantCorrection = queryConstants[QC_CONSTANT_CORRECTION];
            float docSum = (int) BitUtil.VH_LE_INT.get(bulkCorrectionsBuf, corrOff + CORR_DOC_SUM);
            float floatDot = Math.fma(invQScale, rawScore, Math.fma(qOffset, docSum, -constantCorrection));
            return Math.fma(floatDot, scale, queryConstants[QC_QUERY_DOT_CENTROID] + offset);
        } else {
            // Float path: rawScore is the weighted bit-plane dot product (already centered)
            return Math.fma(rawScore, scale, queryConstants[QC_QUERY_DOT_CENTROID] + offset);
        }
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
