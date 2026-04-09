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
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.jdk.ScalarOperations;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.elasticsearch.test.ESTestCase;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;

import java.nio.ShortBuffer;
import java.util.Arrays;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;

public class VectorScorerBFloat16OperationBenchmarkTests extends ESTestCase {

    private final VectorSimilarityType function;
    private final double delta;
    private final int size;
    private final VectorSimilarityFunctions.BFloat16QueryType queryType;

    public VectorScorerBFloat16OperationBenchmarkTests(
        VectorSimilarityType function,
        int size,
        VectorSimilarityFunctions.BFloat16QueryType queryType
    ) {
        this.function = function;
        this.size = size;
        this.queryType = queryType;
        delta = 1e-3 * size;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    public void test() {
        for (int i = 0; i < 5; i++) {
            var bench = new VectorScorerBFloat16OperationBenchmark();
            bench.function = function;
            bench.size = size;
            bench.queryType = queryType;
            bench.init();
            try {
                BFloat16.bFloat16ToFloat(ShortBuffer.wrap(bench.bFloatsA), bench.scratchA);
                float[] floatsB = switch (queryType) {
                    case BFLOAT16 -> {
                        BFloat16.bFloat16ToFloat(ShortBuffer.wrap(bench.bFloatsB), bench.scratchB);
                        yield bench.scratchB;
                    }
                    case FLOAT32 -> bench.floatsB;
                };

                float expected = switch (function) {
                    case DOT_PRODUCT -> ScalarOperations.dotProduct(bench.scratchA, floatsB);
                    case EUCLIDEAN -> ScalarOperations.squareDistance(bench.scratchA, floatsB);
                    default -> throw new AssumptionViolatedException("Not tested");
                };
                assertEquals(expected, bench.lucene(), delta);
                assertEquals(expected, bench.nativeWithNativeSeg(), delta);
                if (supportsHeapSegments()) {
                    assertEquals(expected, bench.nativeWithHeapSeg(), delta);
                }
            } finally {
                bench.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        String[] size = Utils.possibleValues(VectorScorerBFloat16OperationBenchmark.class, "size").toArray(new String[0]);
        String[] functions = Utils.possibleValues(VectorScorerBFloat16OperationBenchmark.class, "function").toArray(new String[0]);
        VectorSimilarityFunctions.BFloat16QueryType[] queryTypes = VectorSimilarityFunctions.BFloat16QueryType.values();
        return () -> Arrays.stream(size)
            .map(Integer::parseInt)
            .flatMap(i -> Arrays.stream(functions).map(VectorSimilarityType::valueOf).map(f -> new Object[] { f, i }))
            .flatMap(o -> Arrays.stream(queryTypes).map(qt -> CollectionUtils.appendToCopy(Arrays.asList(o), qt).toArray()))
            .iterator();
    }
}
