/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Random;

public class VectorTestUtils {

    private VectorTestUtils() {}

    public static float[] randomFloatVector(int dims) {
        return randomFloatVector(ESTestCase.random(), dims);
    }

    public static float[] randomFloatVector(Random random, int dims) {
        float[] vec = new float[dims];
        randomFloatVector(random, vec);
        return vec;
    }

    public static void randomFloatVector(Random random, float[] vec) {
        assert vec.length > 0;
        generateRandomFloatVector(random, vec);
    }

    public static float[] randomNormalizedFloatVector(int dims) {
        return randomNormalizedFloatVector(ESTestCase.random(), dims);
    }

    public static float[] randomNormalizedFloatVector(Random random, int dims) {
        float[] vec = new float[dims];
        randomNormalizedFloatVector(random, vec);
        return vec;
    }

    public static void randomNormalizedFloatVector(Random random, float[] vec) {
        double squareSum = generateRandomFloatVector(random, vec);

        double norm = Math.sqrt(squareSum);
        for (int i = 0; i < vec.length; i++) {
            vec[i] = (float) (vec[i] / norm);
        }
    }

    private static double generateRandomFloatVector(Random random, float[] vec) {
        assert vec.length > 0;
        // we don't want a zero-length vector
        double squareSum;
        do {
            squareSum = 0;
            for (int i = 0; i < vec.length; i++) {
                // from -1 to +1
                vec[i] = random.nextFloat() * 2f - 1f;
                squareSum += vec[i] * vec[i];
            }
        } while (squareSum == 0d);

        return squareSum;
    }

    public static byte[] randomByteVector(int dims) {
        return randomByteVector(ESTestCase.random(), dims);
    }

    public static byte[] randomByteVector(Random random, int dims) {
        assert dims > 0;
        // we don't want a zero-length vector
        byte[] vec = new byte[dims];
        do {
            random.nextBytes(vec);
        } while (Arrays.equals(vec, new byte[dims]));

        return vec;
    }
}
