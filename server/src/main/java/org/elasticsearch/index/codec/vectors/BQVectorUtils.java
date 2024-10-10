/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;

/** Utility class for vector quantization calculations */
public class BQVectorUtils {
    private static final float EPSILON = 1e-4f;

    public static double sqrtNewtonRaphson(double x, double curr, double prev) {
        return (curr == prev) ? curr : sqrtNewtonRaphson(x, 0.5 * (curr + x / curr), curr);
    }

    public static double constSqrt(double x) {
        return x >= 0 && Double.isInfinite(x) == false ? sqrtNewtonRaphson(x, x, 0) : Double.NaN;
    }

    public static boolean isUnitVector(float[] v) {
        double l1norm = VectorUtil.dotProduct(v, v);
        return Math.abs(l1norm - 1.0d) <= EPSILON;
    }

    public static int discretize(int value, int bucket) {
        return ((value + (bucket - 1)) / bucket) * bucket;
    }

    public static float[] pad(float[] vector, int dimensions) {
        if (vector.length >= dimensions) {
            return vector;
        }
        return ArrayUtil.growExact(vector, dimensions);
    }

    public static byte[] pad(byte[] vector, int dimensions) {
        if (vector.length >= dimensions) {
            return vector;
        }
        return ArrayUtil.growExact(vector, dimensions);
    }

    /**
     * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
     * @param d the byte array to count the number of set bits in
     * @return count of flipped bits in the byte array
     */
    public static int popcount(byte[] d) {
        int r = 0;
        int cnt = 0;
        for (final int upperBound = d.length & -Integer.BYTES; r < upperBound; r += Integer.BYTES) {
            cnt += Integer.bitCount((int) BitUtil.VH_NATIVE_INT.get(d, r));
        }
        for (; r < d.length; r++) {
            cnt += Integer.bitCount(d[r] & 0xFF);
        }
        return cnt;
    }

    // TODO: move to VectorUtil & vectorize?
    public static void divideInPlace(float[] a, float b) {
        for (int j = 0; j < a.length; j++) {
            a[j] /= b;
        }
    }

    public static float[] subtract(float[] a, float[] b) {
        float[] result = new float[a.length];
        subtract(a, b, result);
        return result;
    }

    public static void subtractInPlace(float[] target, float[] other) {
        subtract(target, other, target);
    }

    private static void subtract(float[] a, float[] b, float[] result) {
        for (int j = 0; j < a.length; j++) {
            result[j] = a[j] - b[j];
        }
    }

    public static float norm(float[] vector) {
        float magnitude = VectorUtil.dotProduct(vector, vector);
        return (float) Math.sqrt(magnitude);
    }
}
