/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;

public class VectorScorerInt4OperationBenchmarkTests extends ESTestCase {

    private final int size;

    public VectorScorerInt4OperationBenchmarkTests(int size) {
        this.size = size;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    public void test() {
        for (int i = 0; i < 100; i++) {
            var bench = new VectorScorerInt4OperationBenchmark();
            bench.size = size;
            bench.init();

            int expected = bench.scalar();
            assertEquals(expected, bench.lucene());
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            String[] sizes = VectorScorerInt4OperationBenchmark.class.getField("size").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(sizes).map(Integer::parseInt).map(s -> new Object[] { s }).iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
