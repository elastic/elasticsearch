/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Panamized scorer for 7-bit quantized vectors stored as an {@link IndexInput}. **/
public final class MemorySegmentES92Int7VectorsScorer extends MemorySegmentES92FallBackInt7VectorsScorer {

    public MemorySegmentES92Int7VectorsScorer(IndexInput in, int dimensions, MemorySegment memorySegment) {
        super(in, dimensions, memorySegment);
    }

    @Override
    public boolean hasNativeAccess() {
        return false; // This class does not support native access
    }

    @Override
    public long int7DotProduct(byte[] q) throws IOException {
        return fallbackInt7DotProduct(q);
    }

    @Override
    public void int7DotProductBulk(byte[] q, int count, float[] scores) throws IOException {
        fallbackInt7DotProductBulk(q, count, scores);
    }

    @Override
    public void scoreBulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores
    ) throws IOException {
        int7DotProductBulk(q, BULK_SIZE, scores);
        applyCorrectionsBulk(
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            scores
        );
    }

    private void applyCorrectionsBulk(
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores
    ) throws IOException {
        int limit = FLOAT_SPECIES.loopBound(BULK_SIZE);
        int i = 0;
        long offset = in.getFilePointer();
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * SEVEN_BIT_SCALE;
        float y1 = queryComponentSum;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            var ax = FloatVector.fromMemorySegment(FLOAT_SPECIES, memorySegment, offset + i * Float.BYTES, ByteOrder.LITTLE_ENDIAN);
            var lx = FloatVector.fromMemorySegment(
                FLOAT_SPECIES,
                memorySegment,
                offset + 4 * BULK_SIZE + i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).sub(ax).mul(SEVEN_BIT_SCALE);
            var targetComponentSums = IntVector.fromMemorySegment(
                INT_SPECIES,
                memorySegment,
                offset + 8 * BULK_SIZE + i * Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).convert(VectorOperators.I2F, 0);
            var additionalCorrections = FloatVector.fromMemorySegment(
                FLOAT_SPECIES,
                memorySegment,
                offset + 12 * BULK_SIZE + i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN
            );
            var qcDist = FloatVector.fromArray(FLOAT_SPECIES, scores, i);
            // ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly *
            // qcDist;
            var res1 = ax.mul(ay).mul(dimensions);
            var res2 = lx.mul(ay).mul(targetComponentSums);
            var res3 = ax.mul(ly).mul(y1);
            var res4 = lx.mul(ly).mul(qcDist);
            var res = res1.add(res2).add(res3).add(res4);
            // For euclidean, we need to invert the score and apply the additional correction, which is
            // assumed to be the squared l2norm of the centroid centered vectors.
            if (similarityFunction == EUCLIDEAN) {
                res = res.mul(-2).add(additionalCorrections).add(queryAdditionalCorrection).add(1f);
                res = FloatVector.broadcast(FLOAT_SPECIES, 1).div(res).max(0);
                res.intoArray(scores, i);
            } else {
                // For cosine and max inner product, we need to apply the additional correction, which is
                // assumed to be the non-centered dot-product between the vector and the centroid
                res = res.add(queryAdditionalCorrection).add(additionalCorrections).sub(centroidDp);
                if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                    res.intoArray(scores, i);
                    // not sure how to do it better
                    for (int j = 0; j < FLOAT_SPECIES.length(); j++) {
                        scores[i + j] = VectorUtil.scaleMaxInnerProductScore(scores[i + j]);
                    }
                } else {
                    res = res.add(1f).mul(0.5f).max(0);
                    res.intoArray(scores, i);
                }
            }
        }
        in.seek(offset + 16L * BULK_SIZE);
    }
}
