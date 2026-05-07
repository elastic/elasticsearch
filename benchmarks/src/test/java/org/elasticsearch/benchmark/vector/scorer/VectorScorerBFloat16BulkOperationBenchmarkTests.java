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
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.BFloat16QueryType;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.util.Arrays;

public class VectorScorerBFloat16BulkOperationBenchmarkTests extends ESTestCase {

    private final VectorSimilarityType function;
    private final BFloat16QueryType queryType;
    private final int dims;

    public VectorScorerBFloat16BulkOperationBenchmarkTests(VectorSimilarityType function, BFloat16QueryType queryType, int dims) {
        this.function = function;
        this.queryType = queryType;
        this.dims = dims;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    private VectorScorerBFloat16BulkOperationBenchmark newBench() {
        var vectorData = VectorScorerBFloat16BulkOperationBenchmark.VectorData.create(dims, 1000, 200, random());
        var bench = new VectorScorerBFloat16BulkOperationBenchmark();
        bench.function = function;
        bench.queryType = queryType;
        bench.dims = dims;
        bench.numVectors = 1000;
        bench.bulkSize = 200;
        bench.setup(vectorData);
        return bench;
    }

    public void testBulk() {
        for (int i = 0; i < 100; i++) {
            var bench = newBench();
            try {
                assertArrayEquals(function + "/" + queryType, bench.scoreSequential(), bench.scoreBulk(), 1e-3f);
            } finally {
                bench.teardown();
            }
        }
    }

    public void testBulkOffsets() {
        for (int i = 0; i < 100; i++) {
            var bench = newBench();
            try {
                assertArrayEquals(function + "/" + queryType, bench.scoreRandom(), bench.scoreBulkOffsets(), 1e-3f);
            } finally {
                bench.teardown();
            }
        }
    }

    public void testBulkSparse() {
        for (int i = 0; i < 100; i++) {
            var bench = newBench();
            try {
                assertArrayEquals(function + "/" + queryType, bench.scoreRandom(), bench.scoreBulkSparse(), 1e-3f);
            } finally {
                bench.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        var dims = Utils.possibleValues(VectorScorerBFloat16BulkOperationBenchmark.class, "dims");
        var functions = Utils.possibleValues(VectorScorerBFloat16BulkOperationBenchmark.class, "function");
        return () -> dims.stream()
            .map(Integer::parseInt)
            .flatMap(
                d -> functions.stream()
                    .map(VectorSimilarityType::valueOf)
                    .flatMap(f -> Arrays.stream(BFloat16QueryType.values()).map(qt -> new Object[] { f, qt, d }))
            )
            .iterator();
    }
}
