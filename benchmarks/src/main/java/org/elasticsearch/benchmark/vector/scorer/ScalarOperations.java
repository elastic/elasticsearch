/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

/**
 * Basic scalar implementations of similarity operations.
 * <p>
 * It is tricky to get specifically the scalar implementations from Lucene,
 * as it tries to push into Panama implementations. So just re-implement them here.
 */
class ScalarOperations {

    static float dotProduct(float[] a, float[] b) {
        float res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    static int dotProduct(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    static float squareDistance(float[] a, float[] b) {
        float res = 0;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            res += diff * diff;
        }
        return res;
    }

    static int squareDistance(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            int diff = a[i] - b[i];
            res += diff * diff;
        }
        return res;
    }
}
