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
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BitUtil;
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

    /**
     * Most multiplications by the second-moment matrix to run when warm-starting W from an inherited
     * projection matrix at merge time. Far fewer than learning W from a random starting basis may need,
     * since the inherited matrix is already a good starting subspace.
     */
    private static final int WARM_START_REFINE_ITERATIONS = 2;
    /**
     * Training benefits from a relatively large bulk size, due to the training being represented as a matrix
     * Performing a single large multiplication at once is generally better than several smaller multiplications,
     * especially if the matrices are large - then the data needs to be read from RAM less often
     */
    private static final int ENCODE_SIZE = BULK_SIZE * 4;

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
     *
     * @param skipDocIds when {@code true}, per-block doc IDs are not written into the posting lists.
     *        Used for sliced flush segments where vectors are in ordinal order and the reader
     *        translates ordinals to doc IDs via {@code KnnVectorValues.ordToDoc()}.
     * @param pretrainedWT an already-learned transposed projection matrix W^T (row-major, shape
     *        {@code (nDims, originalDim)}) to reuse instead of training a new one, or {@code null}
     *        to train W from the supplied vectors. Used at merge time to seed the merged segment's
     *        projection matrix from an existing input segment. When supplied, its length must equal
     *        {@code originalDim * nDims}; a mismatch is a programming error and throws.
     * @param useMergeTrainingFactor selects the training-sample factor, the sample size as a multiple of the vector
     *        dimension: {@code true} uses the configuration's {@code trainingFactor} (merge), {@code false}
     *        its {@code flushTrainingFactor}. Both currently have the same default.
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
        TaskExecutor quantizerExec,
        VectorSimilarityFunction similarityFunction,
        boolean skipDocIds,
        float[] pretrainedWT,
        boolean useMergeTrainingFactor
    ) throws IOException {
        int nVectors = assignments.length;
        int originalDim = fieldInfo.getVectorDimension();
        int nClusters = centroidSupplier.size();

        // Vector access for training and encoding. Read directly from the supplied vectors by
        // ordinal rather than cloning the whole corpus on-heap:
        // - flush: the vectors are already resident on-heap in the flat-vector writer's buffer.
        // - merge: the vectors are backed by an off-heap temp file (streamed via seek+readFloats).
        // The encode loop and training both fetch each vector once and consume it before requesting
        // the next ordinal, so a provider that returns a shared/reused buffer (the merge case) is
        // safe. This mirrors the OSQ (BBQ) writer, which also encodes directly from the off-heap
        // values at merge without cloning the corpus.
        final CheckedIntFunction<float[], IOException> vectors = floatVectorValues::vectorValue;

        // Select the projection-matrix training profile: the size of the training sample, as a multiple
        // of the vector dimension.
        final int effectiveTrainingFactor = useMergeTrainingFactor ? ashConfig.trainingFactor() : ashConfig.flushTrainingFactor();
        AsymmetricHashingQuantizer ashQuantizer = new AsymmetricHashingQuantizer(
            ashConfig.projectedDimsFraction(),
            ashConfig.bitsPerDim(),
            AsymmetricHashingQuantizer.Method.LEARNED,
            ashConfig.trainingIterations(),
            effectiveTrainingFactor,
            42L,
            quantizerExec
        );

        int nDims = ashQuantizer.nDims(originalDim);

        // Projection matrix selection:
        // - merge with an inheritable W (pretrainedWT != null): warm-start from it, running a couple of steps of
        // subspace iteration against the merged set's second-moment matrix, then applying a new rotation.
        // - otherwise: train W from scratch: the PCA subspace of the training sample, then a rotation (random, unless
        // Procrustes iterations are configured).
        CheckedIntFunction<float[], IOException> centroidGetter = i -> centroidSupplier.centroid(assignments[i]);
        final AsymmetricHashingQuantizer.TrainedProjection trained;
        if (pretrainedWT != null) {
            if (pretrainedWT.length != originalDim * nDims) {
                throw new IllegalArgumentException(
                    "inherited projection matrix has length ["
                        + pretrainedWT.length
                        + "] but expected ["
                        + (originalDim * nDims)
                        + "] for originalDim ["
                        + originalDim
                        + "] and nDims ["
                        + nDims
                        + "]"
                );
            }
            trained = ashQuantizer.trainWarmStart(
                vectors,
                nVectors,
                originalDim,
                centroidGetter,
                pretrainedWT,
                WARM_START_REFINE_ITERATIONS
            );
        } else {
            // LEARNED learns the PCA subspace from the training sample and rotates it.
            trained = ashQuantizer.train(vectors, nVectors, originalDim, centroidGetter);
        }

        // Store the projection matrix for later serialization. The quantizer reports whether W was learned from
        // enough vectors to be inherited/warm-started at merge time. A random orthonormal fallback, or a
        // projection learned from fewer than twice as many vectors as projected dimensions (a small segment), is
        // marked learned=false so that merge does not inherit it.
        this.ashProjectionMatrix = new AshProjectionMatrix(trained.wT(), originalDim, nDims, trained.learned());

        // Build cluster-to-vector mappings, counting primary + SOAR overspill assignments
        ClusterAssignmentBuilder clusterAssignments = ClusterAssignmentBuilder.build(assignments, overspillAssignments, nClusters);

        return writePostingLists(
            vectors,
            ashQuantizer,
            trained.w(),
            trained.wT(),
            originalDim,
            centroidSupplier,
            floatVectorValues,
            clusterAssignments.assignmentsByCluster(),
            clusterAssignments.maxPostingListSize(),
            postingsOutput,
            fileOffset,
            ashConfig,
            similarityFunction,
            skipDocIds
        );
    }

    /**
     * Writes ASH-encoded posting lists for all clusters.
     */
    private PostingsOffsetAndLength writePostingLists(
        CheckedIntFunction<float[], IOException> vectors,
        AsymmetricHashingQuantizer ashQuantizer,
        float[] w,
        float[] wT,
        int originalDim,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        int[][] assignmentsByCluster,
        int maxPostingListSize,
        IndexOutput postingsOutput,
        long fileOffset,
        IvfSegmentConfig.AshConfig ashConfig,
        VectorSimilarityFunction similarityFunction,
        boolean skipDocIds
    ) throws IOException {
        int nClusters = assignmentsByCluster.length;
        int nDims = w.length / originalDim;
        final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        final PackedLongValues.Builder lengths = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        final int bitsPerDim = ashConfig.bitsPerDim();
        final int packedCodeBytes = bitsPerDim * ((nDims + 7) >>> 3);
        final float centerOffset = ((1 << bitsPerDim) - 1) / 2.0f;
        final int[] docIds = new int[maxPostingListSize];
        final int[] docDeltas = new int[maxPostingListSize];
        final int[] clusterOrds = new int[maxPostingListSize];
        // Pre-allocated bulk block buffers.
        // Codes are written contiguously, then corrections in SoA layout per field.
        final byte[] blockCodesBuf = new byte[BULK_SIZE * packedCodeBytes];
        final byte[] blockCorrectionsBuf = new byte[BULK_SIZE * AshPostingsVisitor.CORRECTION_BYTES];
        final boolean isEuclidean = similarityFunction == VectorSimilarityFunction.EUCLIDEAN;
        // Encodes a whole block in one matrix multiply, so W is read once per block rather than once
        // per vector. Created here and reused for every posting list, since W does not change.
        final AsymmetricHashingQuantizer.BlockEncoder encoder = ashQuantizer.newBlockEncoder(w, originalDim, ENCODE_SIZE);
        final float[] blockVecCentroidSqDists = new float[ENCODE_SIZE];
        DocIdsWriter idsWriter = new DocIdsWriter();

        for (int c = 0; c < nClusters; c++) {
            float[] centroid = centroidSupplier.centroid(c);
            // Precompute centroid projection + norm once per posting list
            AsymmetricHashingQuantizer.VectorAndNorm precomputed = AsymmetricHashingQuantizer.precomputeCentroid(centroid, wT);
            int[] cluster = assignmentsByCluster[c];
            long offset = postingsOutput.alignFilePointer(Float.BYTES) - fileOffset;
            offsets.add(offset);
            // Header: size, centroid ordinal, centroid norm squared
            int size = cluster.length;
            postingsOutput.writeVInt(size);
            postingsOutput.writeVInt(c);
            float centroidNormSq = isEuclidean ? ESVectorUtil.dotProduct(centroid, centroid) : 0f;
            postingsOutput.writeInt(Float.floatToIntBits(centroidNormSq));

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
            // The encoding byte is always written for header consistency, even when skipDocIds is true.
            // In the sliced flush path the reader consumes it in resetPostingsScorer but never uses it.
            postingsOutput.writeByte(encoding);

            // Write vectors in bulk blocks:
            // [docIds][packed_codes × blockSize]
            // [scales × blockSize][offsets × blockSize][docSums × blockSize]
            // [vecCentroidDots × blockSize][vecCentroidSqDists × blockSize]
            // When skipDocIds is true (sliced flush), doc IDs are omitted -- the reader uses ordToDoc().
            int written = 0;
            int encodeOffset = ENCODE_SIZE;
            while (written < size) {

                if (encodeOffset == ENCODE_SIZE) {
                    // encode the next encoder block, then project and quantize it in one go.
                    // Providers may return a shared/live buffer, so everything that needs the original vector is read here,
                    // before the next ordinal is requested
                    encoder.reset();
                    for (int j = 0; j < Math.min(ENCODE_SIZE, size - written); j++) {
                        int vectorOrd = cluster[clusterOrds[written + j]];
                        float[] vec = vectors.apply(vectorOrd);
                        encoder.add(vec, centroid);
                        if (isEuclidean) {
                            blockVecCentroidSqDists[j] = ESVectorUtil.squareDistance(vec, centroid);
                        }
                    }
                    encoder.encode(precomputed);
                    encodeOffset = 0;
                }

                int blockSize = Math.min(BULK_SIZE, size - written);
                if (skipDocIds == false) {
                    int blockStart = written;
                    idsWriter.writeDocIds(d -> docDeltas[blockStart + d], blockSize, encoding, postingsOutput);
                }

                // Encode all vectors in this block into pre-allocated buffers.
                // Corrections are packed in SoA order: [scales][offsets][docSums][vecCentroidDots][vecCentroidSqDists]
                int scaleBase = 0;
                int offsetBase = blockSize * Float.BYTES;
                int docSumBase = 2 * blockSize * Float.BYTES;
                int vcdBase = 3 * blockSize * Float.BYTES;
                int vcsdBase = 4 * blockSize * Float.BYTES;

                for (int j = 0; j < blockSize; j++) {
                    float[] code = encoder.code(encodeOffset + j);
                    byte[] vectorPacked = ESVectorUtil.ashPack(code, bitsPerDim);
                    System.arraycopy(vectorPacked, 0, blockCodesBuf, j * packedCodeBytes, packedCodeBytes);
                    int jOff = j * Float.BYTES;
                    BitUtil.VH_BE_INT.set(blockCorrectionsBuf, scaleBase + jOff, Float.floatToIntBits(encoder.scale(encodeOffset + j)));
                    BitUtil.VH_BE_INT.set(blockCorrectionsBuf, offsetBase + jOff, Float.floatToIntBits(encoder.offset(encodeOffset + j)));
                    // Compute docSum: sum of unsigned code values directly from the centered float codes
                    int docSum = 0;
                    for (int d = 0; d < nDims; d++) {
                        docSum += Math.round(code[d] + centerOffset);
                    }
                    BitUtil.VH_BE_INT.set(blockCorrectionsBuf, docSumBase + jOff, docSum);
                    // EUCLIDEAN: ⟨μ*,x⟩ and ‖x-μ*‖² from the original float vectors; 0 otherwise
                    if (isEuclidean) {
                        BitUtil.VH_BE_INT.set(
                            blockCorrectionsBuf,
                            vcdBase + jOff,
                            Float.floatToIntBits(encoder.vecCentroidDot(encodeOffset + j))
                        );
                        BitUtil.VH_BE_INT.set(
                            blockCorrectionsBuf,
                            vcsdBase + jOff,
                            Float.floatToIntBits(blockVecCentroidSqDists[encodeOffset + j])
                        );
                    } else {
                        BitUtil.VH_BE_INT.set(blockCorrectionsBuf, vcdBase + jOff, 0);
                        BitUtil.VH_BE_INT.set(blockCorrectionsBuf, vcsdBase + jOff, 0);
                    }
                }
                // Write packed codes, then all corrections in one call
                postingsOutput.writeBytes(blockCodesBuf, 0, blockSize * packedCodeBytes);
                postingsOutput.writeBytes(blockCorrectionsBuf, 0, blockSize * AshPostingsVisitor.CORRECTION_BYTES);
                written += blockSize;
                encodeOffset += blockSize;
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
