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
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidSupplier;
import org.elasticsearch.index.codec.vectors.diskbbq.ClusterAssignmentBuilder;
import org.elasticsearch.index.codec.vectors.diskbbq.DocIdsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IntSorter;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.codec.vectors.diskbbq.OverspillAssignments;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.AsymmetricHashingScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * Builds and writes ASH-encoded posting lists for IVF segments.
 * <p>
 * This class encapsulates the full ASH write pipeline:
 * <ol>
 *   <li>Collect vectors from the segment</li>
 *   <li>Train the projection matrix W via the ASH optimization procedure</li>
 *   <li>Encode all vectors (project, center, scalar-quantize)</li>
 *   <li>Write posting lists grouped by IVF cluster assignment</li>
 * </ol>
 * <p>
 * The trained {@link AshProjectionMatrix} is retained after writing so the caller can
 * serialize it in the preconditioner slot of the segment file.
 */
public class AshPostingsListWriter {

    private static final Logger logger = LogManager.getLogger(AshPostingsListWriter.class);

    private AshProjectionMatrix ashProjectionMatrix;

    /**
     * Returns the projection matrix trained during the most recent
     * {@link #buildAndWrite} call, or null if not yet called.
     */
    public AshProjectionMatrix getAshProjectionMatrix() {
        return ashProjectionMatrix;
    }

    /**
     * Result of writing posting lists: per-cluster offsets and lengths into the postings file.
     */
    public record PostingsOffsetAndLength(PackedLongValues offsets, PackedLongValues lengths) {}

    /**
     * Trains ASH, encodes vectors, and writes posting lists to the given output.
     */
    public PostingsOffsetAndLength buildAndWrite(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        IvfSegmentConfig.AshConfig ashConfig,
        VectorSimilarityFunction similarityFunction
    ) throws IOException {
        int nVectors = assignments.length;
        int originalDim = fieldInfo.getVectorDimension();
        int nClusters = centroidSupplier.size();

        // Collect all vectors into arrays for ASH training and per-write re-encoding.
        // ClusteringFloatVectorValues (KMeansFloatVectorValues) supports random-access vectorValue(ord)
        // without requiring iterator advance — the same pattern used in ESNextDiskBBQVectorsWriter.
        float[][] vectors = new float[nVectors][];
        for (int i = 0; i < nVectors; i++) {
            vectors[i] = floatVectorValues.vectorValue(i).clone();
        }

        // Create and train the ASH quantizer
        // TODO: consider whether using AsymmetricHashingQuantizer.Method.RANDOM is sufficient
        AsymmetricHashingQuantizer ashQuantizer = new AsymmetricHashingQuantizer(
            ashConfig.projectedDimsFraction(),
            ashConfig.bitsPerDim(),
            AsymmetricHashingQuantizer.Method.LEARNED,
            ashConfig.trainingIterations(),
            ashConfig.trainingFactor(),
            42L
        );

        CheckedIntFunction<float[], IOException> centroidGetter = i -> centroidSupplier.centroid(assignments[i]);

        // Train W using primary assignments only.
        float[] w = ashQuantizer.train(vectors, centroidGetter);

        // Transpose W once for SIMD-friendly dot products during encoding
        int nDims = ashQuantizer.nDims(originalDim);
        float[] wT = ESVectorUtil.transposeMatrix(w, originalDim, nDims);

        // Store the projection matrix for later serialization
        this.ashProjectionMatrix = new AshProjectionMatrix(w, originalDim, nDims);

        // Build cluster-to-vector mappings, counting primary + SOAR overspill assignments
        ClusterAssignmentBuilder clusterAssignments = ClusterAssignmentBuilder.build(assignments, overspillAssignments, nClusters);
        int[][] assignmentsByCluster = clusterAssignments.assignmentsByCluster();
        int maxPostingListSize = clusterAssignments.maxPostingListSize();

        // Write posting lists, re-encoding each vector against its posting list's centroid
        final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        final PackedLongValues.Builder lengths = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        final int bitsPerDim = ashConfig.bitsPerDim();
        final int packedCodeBytes = AsymmetricHashingScorer.packedLength(nDims, bitsPerDim);
        final float centerOffset = ((1 << bitsPerDim) - 1) / 2.0f;
        final int[] docIds = new int[maxPostingListSize];
        final int[] docDeltas = new int[maxPostingListSize];
        final int[] clusterOrds = new int[maxPostingListSize];
        // Pre-allocated bulk block buffers (reused across all clusters)
        final byte[] blockCodesBuf = new byte[BULK_SIZE * packedCodeBytes];
        final short[] blockScales = new short[BULK_SIZE];
        final short[] blockOffsets = new short[BULK_SIZE];
        final int[] blockDocSums = new int[BULK_SIZE];
        final boolean wideDocSum = bitsPerDim > 4;
        // EUCLIDEAN-only per-vector fields (Appendix A, Eq. A.2)
        final boolean isEuclidean = similarityFunction == VectorSimilarityFunction.EUCLIDEAN;
        final float[] blockVecCentroidDots = isEuclidean ? new float[BULK_SIZE] : null;
        final float[] blockVecCentroidSqDists = isEuclidean ? new float[BULK_SIZE] : null;
        DocIdsWriter idsWriter = new DocIdsWriter();

        for (int c = 0; c < nClusters; c++) {
            float[] centroid = centroidSupplier.centroid(c);
            // Precompute centroid projection + norm once per posting list
            AsymmetricHashingQuantizer.PrecomputedCentroid precomputed = AsymmetricHashingQuantizer.precomputeCentroid(centroid, wT);
            int[] cluster = assignmentsByCluster[c];
            long offset = postingsOutput.alignFilePointer(Float.BYTES) - fileOffset;
            offsets.add(offset);
            // Header: size, centroid ordinal
            int size = cluster.length;
            postingsOutput.writeVInt(size);
            postingsOutput.writeVInt(c);
            // For EUCLIDEAN: write ‖μ*‖² (centroid squared norm) — needed for Eq. A.2
            if (isEuclidean) {
                postingsOutput.writeInt(Float.floatToIntBits(ESVectorUtil.dotProduct(centroid, centroid)));
            }

            // Sort by docId
            for (int j = 0; j < size; j++) {
                docIds[j] = floatVectorValues.ordToDoc(cluster[j]);
                clusterOrds[j] = j;
            }
            new IntSorter(clusterOrds, i -> docIds[i]).sort(0, size);
            for (int j = 0; j < size; j++) {
                docDeltas[j] = j == 0 ? docIds[clusterOrds[j]] : docIds[clusterOrds[j]] - docIds[clusterOrds[j - 1]];
            }

            byte encoding = idsWriter.calculateBlockEncoding(i -> docDeltas[i], size, BULK_SIZE);
            postingsOutput.writeByte(encoding);

            // Write vectors in bulk blocks using structure-of-arrays layout:
            // [docIds][all packed_codes][all scales][all offsets][all docSums]
            // EUCLIDEAN additionally writes: [all vecCentroidDots][all vecCentroidSqDists]
            int written = 0;
            while (written < size) {
                int blockSize = Math.min(BULK_SIZE, size - written);
                final int blockStart = written;
                idsWriter.writeDocIds(d -> docDeltas[blockStart + d], blockSize, encoding, postingsOutput);

                // Encode all vectors in this block into pre-allocated buffers
                for (int j = 0; j < blockSize; j++) {
                    int vectorOrd = cluster[clusterOrds[written + j]];
                    AsymmetricHashingQuantizer.EncodedVector enc = ashQuantizer.encode(vectors[vectorOrd], centroid, wT, precomputed);
                    byte[] vectorPacked = AsymmetricHashingScorer.pack(enc.xEnc(), bitsPerDim);
                    System.arraycopy(vectorPacked, 0, blockCodesBuf, j * packedCodeBytes, packedCodeBytes);
                    blockScales[j] = Float.floatToFloat16(enc.scale());
                    blockOffsets[j] = Float.floatToFloat16(enc.offset());
                    // Compute docSum: sum of unsigned code values directly from the centered float codes
                    int docSum = 0;
                    float[] xEnc = enc.xEnc();
                    for (int d = 0; d < nDims; d++) {
                        docSum += Math.round(xEnc[d] + centerOffset);
                    }
                    blockDocSums[j] = docSum;
                    // EUCLIDEAN: compute ⟨μ*,x⟩ and ‖x-μ*‖² from the original float vectors
                    if (isEuclidean) {
                        float[] vec = vectors[vectorOrd];
                        blockVecCentroidDots[j] = ESVectorUtil.dotProduct(centroid, vec);
                        blockVecCentroidSqDists[j] = ESVectorUtil.squareDistance(vec, centroid);
                    }
                }
                // Write all packed codes contiguously
                postingsOutput.writeBytes(blockCodesBuf, 0, blockSize * packedCodeBytes);
                // Write all scales
                for (int j = 0; j < blockSize; j++) {
                    postingsOutput.writeShort(blockScales[j]);
                }
                // Write all offsets
                for (int j = 0; j < blockSize; j++) {
                    postingsOutput.writeShort(blockOffsets[j]);
                }
                // Write all docSums (sum of unsigned code values, for D2Q4 correction)
                for (int j = 0; j < blockSize; j++) {
                    if (wideDocSum) {
                        postingsOutput.writeInt(blockDocSums[j]);
                    } else {
                        postingsOutput.writeShort((short) blockDocSums[j]);
                    }
                }
                // EUCLIDEAN: write ⟨μ*,x⟩ and ‖x-μ*‖² per vector (Appendix A, Eq. A.2)
                if (isEuclidean) {
                    for (int j = 0; j < blockSize; j++) {
                        postingsOutput.writeInt(Float.floatToIntBits(blockVecCentroidDots[j]));
                    }
                    for (int j = 0; j < blockSize; j++) {
                        postingsOutput.writeInt(Float.floatToIntBits(blockVecCentroidSqDists[j]));
                    }
                }
                written += blockSize;
            }
            lengths.add(postingsOutput.getFilePointer() - fileOffset - offset);
        }

        if (logger.isDebugEnabled()) {
            printClusterQualityStatistics(assignmentsByCluster);
        }

        return new PostingsOffsetAndLength(offsets.build(), lengths.build());
    }

    private static void printClusterQualityStatistics(int[][] clusters) {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        float mean = 0;
        float m2 = 0;
        int count = 0;
        for (int[] cluster : clusters) {
            count += 1;
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
}
