/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.index.FieldInfo;
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

import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * PostingVisitor for ASH-encoded posting lists.
 * <p>
 * Reads bit-packed 2-bit codes with float16 scale/offset per vector and scores them
 * asymmetrically using the precomputed query transform. The on-disk format per block is:
 * <pre>
 *   [docIds][packed_codes × blockSize][scales × blockSize][offsets × blockSize][docSums × blockSize]
 * </pre>
 * <p>
 * This initial implementation uses scalar scoring via {@link AsymmetricHashingScorer#score}.
 * SIMD-accelerated bulk scoring (ipFloatBit, D2Q4) will be added in a follow-up.
 */
public class AshPostingsVisitor implements IVFVectorsReader.PostingVisitor {

    private final FieldInfo fieldInfo;
    private final IndexInput indexInput;
    private final Bits acceptDocs;
    private final int nDims;
    private final int bitsPerDim;
    private final int packedCodeBytes;
    private final VectorSimilarityFunction similarityFunction;

    // Precomputed query transform: queryTransformed = query @ W (raw projection, not centered)
    private final float[] queryTransformed;

    // Scratch buffers for bulk I/O
    private final DocIdsWriter idsWriter = new DocIdsWriter();
    private final int[] docIdsScratch = new int[BULK_SIZE];
    private final int[] offsetsScratch = new int[BULK_SIZE];
    private final float[] scores = new float[BULK_SIZE];
    private final byte[] bulkCodeBuf;
    private final short[] bulkScalesF16 = new short[BULK_SIZE];
    private final short[] bulkOffsetsF16 = new short[BULK_SIZE];
    // docSums are read for future D2Q4 integer scoring (PR 2); unused in the current scalar path
    private final short[] bulkDocSums = new short[BULK_SIZE];

    // Per-posting-list state
    private int vectors;
    private byte docEncoding;
    private int docBase;
    private float currentQueryDotCentroid;

    public AshPostingsVisitor(float[][] wT, float[] query, FieldInfo fieldInfo, IndexInput indexInput, Bits acceptDocs, int bitsPerDim) {
        this.fieldInfo = fieldInfo;
        this.indexInput = indexInput;
        this.acceptDocs = acceptDocs;
        this.nDims = wT.length;
        this.bitsPerDim = bitsPerDim;
        this.packedCodeBytes = AsymmetricHashingScorer.packedLength(nDims, bitsPerDim);
        this.similarityFunction = fieldInfo.getVectorSimilarityFunction();

        // Precompute query projection: queryTransformed[j] = dot(query, wT[j])
        this.queryTransformed = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            queryTransformed[j] = ESVectorUtil.dotProduct(query, wT[j]);
        }

        this.bulkCodeBuf = new byte[BULK_SIZE * packedCodeBytes];
    }

    @Override
    public int resetPostingsScorer(PostingMetadata metadata) throws IOException {
        float score = metadata.documentCentroidScore();
        indexInput.seek(metadata.offset());
        float centroidToParentSqDist = Float.intBitsToFloat(indexInput.readInt());
        vectors = indexInput.readVInt();
        docEncoding = indexInput.readByte();
        docBase = 0;

        // Approximate query·centroid derived from the quantized centroid scoring.
        // The centroid score is an OSQ-quantized similarity; we invert the similarity transform
        // to recover an approximate raw dot product. This avoids reading centroid float vectors
        // per posting list. A future improvement could compute the exact dot product in the
        // centroid iterator and pass it via PostingMetadata.
        currentQueryDotCentroid = switch (similarityFunction) {
            case EUCLIDEAN -> ((1 / score) - 1) - centroidToParentSqDist;
            case COSINE, DOT_PRODUCT -> 2 * score - 1;
            case MAXIMUM_INNER_PRODUCT -> score - 1;
        };

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
            // Skip the entire block
            indexInput.skipBytes((long) blockSize * packedCodeBytes + (long) blockSize * Short.BYTES * 3);
            return 0;
        }

        // Read structure-of-arrays: codes, scales, offsets, docSums
        indexInput.readBytes(bulkCodeBuf, 0, blockSize * packedCodeBytes);
        for (int j = 0; j < blockSize; j++) {
            bulkScalesF16[j] = indexInput.readShort();
        }
        for (int j = 0; j < blockSize; j++) {
            bulkOffsetsF16[j] = indexInput.readShort();
        }
        for (int j = 0; j < blockSize; j++) {
            bulkDocSums[j] = indexInput.readShort();
        }

        // Score each vector using scalar multi-bit scorer
        float maxScore = Float.NEGATIVE_INFINITY;
        for (int j = 0; j < blockSize; j++) {
            if (docIdsScratch[j] != -1) {
                float scale = Float.float16ToFloat(bulkScalesF16[j]);
                float offset = Float.float16ToFloat(bulkOffsetsF16[j]);
                float rawScore = AsymmetricHashingScorer.score(
                    queryTransformed,
                    currentQueryDotCentroid,
                    bulkCodeBuf,
                    j * packedCodeBytes,
                    nDims,
                    bitsPerDim,
                    scale,
                    offset
                );
                scores[j] = convertScore(rawScore);
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
            case EUCLIDEAN -> 1 / (1 + rawDotProduct);
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
