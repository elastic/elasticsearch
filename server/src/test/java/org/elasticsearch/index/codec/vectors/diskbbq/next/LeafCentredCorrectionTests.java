/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.test.ESTestCase;

public class LeafCentredCorrectionTests extends ESTestCase {

    private static float[] randomVector(int dims, float scale) {
        float[] v = new float[dims];
        for (int i = 0; i < dims; i++) {
            v[i] = (float) randomGaussianValue() * scale;
        }
        return v;
    }

    private static double randomGaussianValue() {
        return random().nextGaussian();
    }

    public void testMatchesScalarReference() {
        for (QuantEncoding encoding : new QuantEncoding[] {
            QuantEncoding.ONE_BIT_4BIT_QUERY,
            QuantEncoding.TWO_BIT_4BIT_QUERY,
            QuantEncoding.FOUR_BIT_SYMMETRIC,
            QuantEncoding.SEVEN_BIT_SYMMETRIC }) {
            // include dimensions that are not a multiple of 8 so that discretization padding is exercised
            int dims = randomIntBetween(1, 200);
            int bits = encoding.bits();
            OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(VectorSimilarityFunction.DOT_PRODUCT);
            LeafCentredCorrection correction = new LeafCentredCorrection(dims);
            for (int iter = 0; iter < 20; iter++) {
                float[] x = randomVector(dims, 1f);
                float[] c = randomVector(dims, 1f);
                float[] p = randomVector(dims, 1f);
                float[] q = randomVector(dims, 1f);
                float[] residual = new float[dims];
                int[] quantized = new int[encoding.discretizedDimensions(dims)];
                OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(x, residual, quantized, encoding.bits(), c);

                correction.prepare(c, p);
                float actual = correction.compute(result, quantized, bits);

                double a = result.lowerInterval();
                double step = (result.upperInterval() - a) / ((1 << bits) - 1);
                double cr = 0, shift = 0, qMinusPRhat = 0, qMinusCRhat = 0, qc = 0, qx = 0;
                for (int i = 0; i < dims; i++) {
                    double rhat = a + step * quantized[i];
                    cr += (double) c[i] * residual[i];
                    shift += (double) (c[i] - p[i]) * rhat;
                    qMinusPRhat += (double) (q[i] - p[i]) * rhat;
                    qMinusCRhat += (double) (q[i] - c[i]) * rhat;
                    qc += (double) q[i] * c[i];
                    qx += (double) q[i] * x[i];
                }
                double expected = cr - shift;
                assertEquals(expected, actual, 1e-3 * Math.max(1, Math.abs(expected)));

                // the integer sum now estimates <q - c, r^>: both decompositions of the score must agree
                double viaParent = qMinusPRhat + actual + qc;
                double viaLeaf = qMinusCRhat + cr + qc;
                assertEquals(viaLeaf, viaParent, 1e-3 * Math.max(1, Math.abs(viaLeaf)));
                // and approximate the true dot product, with more bits meaning a tighter bound
                if (bits == 7) {
                    assertEquals(qx, viaParent, 0.05 * Math.sqrt(dims) + 0.05);
                }
            }
        }
    }
}
