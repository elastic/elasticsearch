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

import org.elasticsearch.benchmark.vector.ESVectorUtilFloatOperationBenchmark.Operation;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

public class ESVectorUtilFloatOperationBenchmarkTests extends ESTestCase {

    private final Operation operation;
    private final int size;

    public ESVectorUtilFloatOperationBenchmarkTests(Operation operation, int size) {
        this.operation = operation;
        this.size = size;
    }

    public void testBenchmarkRuns() {
        var bench = new ESVectorUtilFloatOperationBenchmark();
        bench.operation = operation;
        bench.size = size;
        bench.setup();
        assertFalse(Float.isNaN(bench.scalar()));
        assertFalse(Float.isNaN(bench.panamaSimd()));
    }

    public void testPanamaMatchesScalar() {
        assumeTrue("jdk.incubator.vector module required", Runtime.version().feature() >= 21);
        assumeTrue("jdk.incubator.vector module required", ModuleLayer.boot().findModule("jdk.incubator.vector").isPresent());

        var bench = new ESVectorUtilFloatOperationBenchmark();
        bench.operation = operation;
        bench.size = size;
        bench.setup();

        switch (operation) {
            case DOT_PRODUCT -> assertEquals(
                ESVectorUtilFloatOperationBenchmark.scalarDotProduct(bench.a, bench.b, 0, size),
                ESVectorUtil.dotProduct(bench.a, bench.b, size),
                1e-3f * size
            );
            case L2_NORMALIZE -> {
                float[] expected = bench.normalizeSource.clone();
                float[] actual = bench.normalizeSource.clone();
                ESVectorUtilFloatOperationBenchmark.scalarL2Normalize(expected, 0, size);
                ESVectorUtil.l2Normalize(actual, size);
                assertArrayEquals(expected, actual, 1e-5f);
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        var operationField = ESVectorUtilFloatOperationBenchmark.class.getField("operation");
        var sizeField = ESVectorUtilFloatOperationBenchmark.class.getField("size");
        Operation[] operations = (Operation[]) operationField.getType().getEnumConstants();
        String[] sizes = sizeField.getAnnotation(org.openjdk.jmh.annotations.Param.class).value();
        Object[][] params = new Object[operations.length * sizes.length][];
        int i = 0;
        for (Operation operation : operations) {
            for (String size : sizes) {
                params[i++] = new Object[] { operation, Integer.parseInt(size) };
            }
        }
        return java.util.Arrays.asList(params);
    }
}
