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
import org.elasticsearch.simdvec.AshScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * PostingVisitor for ASH-encoded posting lists.
 * <p>
 * Parameterized on {@code T}, the query type passed to the {@link AshScorer}:
 * {@code float[]} for the float-query path or {@code byte[]} for the integer-query path.
 * <p>
 * The on-disk format per block is:
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
 * Scoring of packed codes is delegated to an {@link AshScorer} which
 * reads directly from the {@link IndexInput}, avoiding heap copies when backed by mmap.
 * <p>
 * EUCLIDEAN scoring implements the ASH paper Appendix A (Eq. A.2):
 * <pre>
 *   ‖q - x‖² = ‖q - μ*‖² + ‖x - μ*‖² - 2·(⟨q,x⟩ - ⟨μ*,x⟩ - ⟨q,μ*⟩ + ‖μ*‖²)
 * </pre>
 */
public class AshPostingsVisitor<T> implements IVFVectorsReader.PostingVisitor {

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

    /**
     * Result of quantizing a projected query for the integer scoring path.
     *
     * @param queryQuantized quantized query in bit-plane format
     * @param invQScale inverse of the query quantization scale
     * @param qOffset the query minimum value (used as offset in correction)
     * @param constantCorrection precomputed centering correction term
     */
    private record QuantizedQuery(byte[] queryQuantized, float invQScale, float qOffset, float constantCorrection) {}

    /**
     * Quantizes a projected query vector for the integer scoring path.
     *
     * @param queryTransformed the projected query (query @ W)
     * @param nDims number of projected dimensions
     * @param queryBitsPerDim bits per dimension for query quantization
     * @param bitsPerDim bits per dimension for document codes
     * @return the quantized query and correction parameters
     */
    private static QuantizedQuery quantizeQuery(float[] queryTransformed, int nDims, int queryBitsPerDim, int bitsPerDim) {
        // TODO: combine with ESVectorUtil.ashPack & panamize somehow
        int planeBytes = (nDims + 7) >>> 3;
        int numQueryLevels = 1 << queryBitsPerDim;

        float qMin = Float.MAX_VALUE, qMax = -Float.MAX_VALUE;
        for (int j = 0; j < nDims; j++) {
            qMin = Math.min(qMin, queryTransformed[j]);
            qMax = Math.max(qMax, queryTransformed[j]);
        }
        float range = qMax - qMin;
        float qScale = range > 0 ? (numQueryLevels - 1) / range : 1.0f;
        float invQScale = range > 0 ? range / (numQueryLevels - 1) : 0f;

        int[] rounded = new int[nDims];
        int unsignedSum = 0;
        for (int j = 0; j < nDims; j++) {
            rounded[j] = Math.clamp(Math.round((queryTransformed[j] - qMin) * qScale), 0, numQueryLevels - 1);
            unsignedSum += rounded[j];
        }

        byte[] queryQuantized = new byte[queryBitsPerDim * planeBytes];
        switch (queryBitsPerDim) {
            case 1 -> ESVectorUtil.pack1BitValues(rounded, queryQuantized);
            case 2 -> ESVectorUtil.stride2BitValues(rounded, queryQuantized);
            case 4 -> ESVectorUtil.stride4BitValues(rounded, queryQuantized);
            case 3, 8 -> {
                for (int j = 0; j < nDims; j++) {
                    int byteIdx = j >>> 3;
                    int bitIdx = 7 - (j & 7);
                    for (int p = 0; p < queryBitsPerDim; p++) {
                        if ((rounded[j] & (1 << p)) != 0) {
                            queryQuantized[p * planeBytes + byteIdx] |= (byte) (1 << bitIdx);
                        }
                    }
                }
            }
            default -> throw new IllegalArgumentException("Unsupported bitsPerDim: " + queryBitsPerDim);
        }

        float centerOffset = ((1 << bitsPerDim) - 1) / 2.0f;
        float constantCorrection = centerOffset * (unsignedSum * invQScale + qMin * nDims);
        return new QuantizedQuery(queryQuantized, invQScale, qMin, constantCorrection);
    }

    /**
     * Factory method that creates a fully-configured {@link AshPostingsVisitor} for the
     * appropriate query path (float or integer), encapsulating query projection, optional
     * quantization, and scorer selection.
     *
     * @param wT transposed projection matrix W^T in row-major order, shape (nDims, originalDim)
     * @param originalDim original vector dimensionality (number of columns in wT)
     * @param query the raw query vector
     * @param similarityFunction the vector similarity function for score conversion
     * @param indexInput input for reading posting list data (must be unwrapped for MemorySegment access)
     * @param acceptDocs live docs filter
     * @param bitsPerDim bits per dimension for document codes
     * @param queryBitsPerDim bits per dimension for query quantization (0 for float path)
     * @param centroidReader function mapping centroid ordinal to float[] centroid vector
     * @return a configured visitor for the appropriate scoring path
     */
    public static AshPostingsVisitor<?> create(
        float[] wT,
        int originalDim,
        float[] query,
        VectorSimilarityFunction similarityFunction,
        IndexInput indexInput,
        Bits acceptDocs,
        int bitsPerDim,
        int queryBitsPerDim,
        CheckedIntFunction<float[], IOException> centroidReader
    ) throws IOException {
        int nDims = wT.length / originalDim;

        // Precompute query projection: queryTransformed[j] = dot(query, wT[j*originalDim .. (j+1)*originalDim))
        float[] queryTransformed = SvdUtil.matrixVectorMultiply(wT, nDims, originalDim, query);

        if (queryBitsPerDim > 0) {
            QuantizedQuery qq = quantizeQuery(queryTransformed, nDims, queryBitsPerDim, bitsPerDim);
            AshScorer<byte[]> scorer = ESVectorUtil.getAshIntegerVectorsScorer(indexInput, nDims, bitsPerDim, queryBitsPerDim);
            return new AshPostingsVisitor<>(
                wT,
                originalDim,
                query,
                similarityFunction,
                scorer,
                qq.queryQuantized(),
                indexInput,
                acceptDocs,
                bitsPerDim,
                qq,
                centroidReader
            );
        } else {
            AshScorer<float[]> scorer = ESVectorUtil.getAshFloatVectorsScorer(indexInput, nDims, bitsPerDim);
            return new AshPostingsVisitor<>(
                wT,
                originalDim,
                query,
                similarityFunction,
                scorer,
                queryTransformed,
                indexInput,
                acceptDocs,
                bitsPerDim,
                null,
                centroidReader
            );
        }
    }

    /** Strategy for transforming a score using per-vector correction data. */
    @FunctionalInterface
    private interface ScoreTransform {
        float apply(float score, byte[] corrections, int correctionOffset);
    }

    private final IndexInput indexInput;
    private final Bits acceptDocs;
    private final int packedCodeBytes;
    private final VectorSimilarityFunction similarityFunction;

    // Raw query vector — retained for exact centroid dot products at query time
    private final float[] query;

    // Centroid lookup: ordinal → float[] centroid vector (reads from centroid file)
    private final CheckedIntFunction<float[], IOException> centroidReader;

    // The scorer and its query, both typed on T
    private final AshScorer<T> scorer;
    private final T scorerQuery;

    // Correction and similarity strategies, selected once at construction time
    private final ScoreTransform correctionApplier;
    private final ScoreTransform similarityConverter;

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
     * @param scorerQuery the query to pass to the scorer (float[] or byte[])
     * @param indexInput input for reading posting list data (doc IDs, corrections)
     * @param acceptDocs live docs filter
     * @param bitsPerDim bits per dimension for document codes
     * @param quantizedQuery quantized query parameters (null for float path)
     * @param centroidReader function mapping centroid ordinal to float[] centroid vector
     */
    private AshPostingsVisitor(
        float[] wT,
        int originalDim,
        float[] query,
        VectorSimilarityFunction similarityFunction,
        AshScorer<T> scorer,
        T scorerQuery,
        IndexInput indexInput,
        Bits acceptDocs,
        int bitsPerDim,
        QuantizedQuery quantizedQuery,
        CheckedIntFunction<float[], IOException> centroidReader
    ) {
        int nDims = wT.length / originalDim;
        this.indexInput = indexInput;
        this.acceptDocs = acceptDocs;
        this.packedCodeBytes = bitsPerDim * ((nDims + 7) >>> 3);
        this.similarityFunction = similarityFunction;
        this.query = query;
        this.centroidReader = centroidReader;
        this.scorer = scorer;
        this.scorerQuery = scorerQuery;

        // Correction applier: captures quantization params for integer path,
        // or uses the simple float formula. Reads currentQueryDotCentroid at call time.
        if (quantizedQuery != null) {
            float invQScale = quantizedQuery.invQScale();
            float qOffset = quantizedQuery.qOffset();
            float constantCorrection = quantizedQuery.constantCorrection();
            this.correctionApplier = (rawScore, corr, corrOff) -> {
                float scale = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(corr, corrOff + CORR_SCALE));
                float offset = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(corr, corrOff + CORR_OFFSET));
                float docSum = (int) BitUtil.VH_LE_INT.get(corr, corrOff + CORR_DOC_SUM);
                float floatDot = Math.fma(invQScale, rawScore, Math.fma(qOffset, docSum, -constantCorrection));
                return Math.fma(floatDot, scale, currentQueryDotCentroid + offset);
            };
        } else {
            this.correctionApplier = (rawScore, corr, corrOff) -> {
                float scale = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(corr, corrOff + CORR_SCALE));
                float offset = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(corr, corrOff + CORR_OFFSET));
                return Math.fma(rawScore, scale, currentQueryDotCentroid + offset);
            };
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
        scorer.scoreBulk(scorerQuery, blockSize, scores);

        // Step 2: Read corrections (IndexInput is now past the codes, at the corrections)
        indexInput.readBytes(bulkCorrectionsBuf, 0, blockSize * CORRECTION_BYTES);

        // Step 3: Apply per-vector corrections and similarity conversion
        float maxScore = Float.NEGATIVE_INFINITY;
        for (int j = 0; j < blockSize; j++) {
            if (docIdsScratch[j] != -1) {
                int corrOff = j * CORRECTION_BYTES;
                float approxDotProduct = correctionApplier.apply(scores[j], bulkCorrectionsBuf, corrOff);
                scores[j] = similarityConverter.apply(approxDotProduct, bulkCorrectionsBuf, corrOff);
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
