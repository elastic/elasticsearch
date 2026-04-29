/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.openjdk.jmh.annotations.Param;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

public class BenchmarkTest extends ESTestCase {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static Iterable<Object[]> generateParameters(Field... paramsFields) {
        List<Object[]> params = Arrays.stream(paramsFields).map(f -> {
            String[] values = f.getAnnotation(Param.class).value();
            return Arrays.copyOf(values, values.length, Object[].class);
        }).collect(Collectors.toCollection(ArrayList::new));

        for (int p = 0; p < params.size(); p++) {
            Field f = paramsFields[p];
            Object[] ps = params.get(p);
            UnaryOperator<Object> op;
            if (f.getType() == int.class) {
                op = o -> Integer.parseInt(o.toString());
            } else if (f.getType() == byte.class) {
                op = o -> Byte.parseByte(o.toString());
            } else if (f.getType().isEnum()) {
                if (ps[0].equals(Param.BLANK_ARGS)) {
                    // use all the enum constants
                    params.set(p, f.getType().getEnumConstants());
                    continue;
                } else {
                    op = o -> Enum.valueOf((Class<Enum>) f.getType(), o.toString());
                }
            } else {
                throw new IllegalArgumentException("Unknown type " + f.getType());
            }

            for (int i = 0; i < ps.length; i++) {
                ps[i] = op.apply(ps[i]);
            }
        }

        // produce all the permutations of the field values
        List<Object[]> permutations = new ArrayList<>();
        generatePermutations(params, 0, new Object[params.size()], permutations);
        return permutations;
    }

    private static void generatePermutations(List<Object[]> params, int index, Object[] current, List<Object[]> permutations) {
        if (index == params.size()) {
            permutations.add(current.clone());
            return;
        }

        for (Object value : params.get(index)) {
            current[index] = value;
            generatePermutations(params, index + 1, current, permutations);
        }
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    @BeforeClass
    public static void requiresHeapSegments() {
        assumeTrue("Native scorers only supported on JDK 22+", SUPPORTS_HEAP_SEGMENTS);
    }

    public <V> void testSequential(
        Supplier<V> vectorData,
        CheckedBiFunction<V, VectorImplementation, VectorScorerBulkBenchmark, IOException> createBenchmark,
        float delta
    ) throws Exception {
        for (int i = 0; i < 100; i++) {
            V data = vectorData.get();
            assertResultsEqual(
                List.of(VectorImplementation.values()),
                impl -> createBenchmark.apply(data, impl),
                List.of(VectorScorerBulkBenchmark::scoreMultipleSequential, VectorScorerBulkBenchmark::scoreMultipleSequentialBulk),
                delta
            );
        }
    }

    public <V> void testRandom(
        Supplier<V> vectorData,
        CheckedBiFunction<V, VectorImplementation, VectorScorerBulkBenchmark, IOException> createBenchmark,
        float delta
    ) throws IOException {
        for (int i = 0; i < 100; i++) {
            V data = vectorData.get();
            assertResultsEqual(
                List.of(VectorImplementation.values()),
                impl -> createBenchmark.apply(data, impl),
                List.of(VectorScorerBulkBenchmark::scoreMultipleRandom, VectorScorerBulkBenchmark::scoreMultipleRandomBulk),
                delta
            );
        }
    }

    public <V> void testQueryRandom(
        Supplier<V> vectorData,
        CheckedBiFunction<V, VectorImplementation, VectorScorerBulkBenchmark, IOException> createBenchmark,
        float delta
    ) throws IOException {
        assumeTrue("Only test with heap segments", supportsHeapSegments());
        for (int i = 0; i < 100; i++) {
            V data = vectorData.get();
            assertResultsEqual(
                List.of(VectorImplementation.LUCENE, VectorImplementation.NATIVE),
                impl -> createBenchmark.apply(data, impl),
                List.of(VectorScorerBulkBenchmark::scoreQueryMultipleRandom, VectorScorerBulkBenchmark::scoreQueryMultipleRandomBulk),
                delta
            );
        }
    }

    static <B extends VectorScorerBulkBenchmark> void assertResultsEqual(
        List<VectorImplementation> implementations,
        IOFunction<VectorImplementation, B> getBenchmark,
        List<IOFunction<B, float[]>> scores,
        float delta
    ) throws IOException {
        float[] expected = null;
        for (var impl : implementations) {
            B bench = getBenchmark.apply(impl);

            try {
                for (IOFunction<B, float[]> score : scores) {
                    float[] result = score.apply(bench);
                    // just check against the first one - they should all be identical to each other
                    if (expected == null) {
                        expected = result;
                        continue;
                    }

                    assertArrayEquals(impl.toString(), expected, result, delta);
                }
            } finally {
                bench.teardown();
            }
        }
    }
}
