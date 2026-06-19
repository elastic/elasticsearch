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

import org.elasticsearch.benchmark.vector.scorer.BenchmarkTest;
import org.elasticsearch.test.ESTestCase;

import java.util.Random;

public class ESVectorUtilFloatOperationBenchmarkTests extends ESTestCase {

    private final int size;

    public ESVectorUtilFloatOperationBenchmarkTests(int size) {
        this.size = size;
    }

    public void testDotProduct() {
        long seed = randomLong();
        var bench = new ESVectorUtilFloatOperationBenchmark();
        bench.size = size;
        bench.implementation = VectorImplementation.SCALAR;
        bench.setup(new Random(seed));
        float expected = bench.dotProduct();

        bench = new ESVectorUtilFloatOperationBenchmark();
        bench.size = size;
        bench.implementation = VectorImplementation.PANAMA;
        bench.setup(new Random(seed));
        float panama = bench.dotProduct();

        assertEquals(expected, panama, 1e-3f * size);
    }

    public void testL2Normalize() {
        long seed = randomLong();

        var bench = new ESVectorUtilFloatOperationBenchmark();
        bench.size = size;
        bench.implementation = VectorImplementation.SCALAR;
        bench.setup(new Random(seed));
        float expected = bench.l2Normalize();

        bench = new ESVectorUtilFloatOperationBenchmark();
        bench.size = size;
        bench.implementation = VectorImplementation.PANAMA;
        bench.setup(new Random(seed));
        float panama = bench.l2Normalize();

        assertEquals(expected, panama, 1e-5f);
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        return BenchmarkTest.generateParameters(ESVectorUtilFloatOperationBenchmark.class.getField("size"));
    }
}
