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

public class Float32ScorerBenchmarkTests extends ESTestCase {

    final double delta = 1e-3;
    final int dims;

    public Float32ScorerBenchmarkTests(int dims) {
        this.dims = dims;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    public void testDotProduct() throws Exception {
        for (int i = 0; i < 100; i++) {
            var bench = new Float32ScorerBenchmark();
            bench.dims = dims;
            bench.setup();
            try {
                float expected = bench.dotProductScalar();
                assertEquals(expected, bench.dotProductLucene(), delta);
                assertEquals(expected, bench.dotProductNative(), delta);

                expected = bench.dotProductLuceneQuery();
                assertEquals(expected, bench.dotProductNativeQuery(), delta);
            } finally {
                bench.teardown();
            }
        }
    }

    public void testSquareDistance() throws Exception {
        for (int i = 0; i < 100; i++) {
            var bench = new Float32ScorerBenchmark();
            bench.dims = dims;
            bench.setup();
            try {
                float expected = bench.squareDistanceScalar();
                assertEquals(expected, bench.squareDistanceLucene(), delta);
                assertEquals(expected, bench.squareDistanceNative(), delta);

                expected = bench.squareDistanceLuceneQuery();
                assertEquals(expected, bench.squareDistanceNativeQuery(), delta);
            } finally {
                bench.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            var params = Float32ScorerBenchmark.class.getField("dims").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(params).map(Integer::parseInt).map(i -> new Object[] { i }).iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
