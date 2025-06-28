/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;

public class JDKVectorInt7uBenchmarkTests extends ESTestCase {

    final double delta = 1e-3;
    final int size;

    public JDKVectorInt7uBenchmarkTests(int size) {
        this.size = size;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    public void testDotProduct() {
        for (int i = 0; i < 100; i++) {
            var bench = new JDKVectorInt7uBenchmark();
            bench.size = size;
            bench.init();
            try {
                float expected = dotProductScalar(bench.byteArrayA, bench.byteArrayB);
                assertEquals(expected, bench.dotProductLucene(), delta);
                assertEquals(expected, bench.dotProductNativeWithHeapSeg(), delta);
                assertEquals(expected, bench.dotProductNativeWithNativeSeg(), delta);
            } finally {
                bench.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            var params = JDKVectorInt7uBenchmark.class.getField("size").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(params).map(Integer::parseInt).map(i -> new Object[] { i }).iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    /** Computes the dot product of the given vectors a and b. */
    static int dotProductScalar(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }
}
