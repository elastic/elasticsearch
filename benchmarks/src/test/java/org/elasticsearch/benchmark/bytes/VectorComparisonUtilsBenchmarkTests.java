/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.bytes;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

public class VectorComparisonUtilsBenchmarkTests extends ESTestCase {

    final int size;

    public VectorComparisonUtilsBenchmarkTests(int size) {
        this.size = size;
    }

    public void testByte() {
        var bench = new VectorComparisonUtilsByteBenchmark();
        bench.size = size;
        bench.setup();
        final boolean[] expected = expected(bench.data, bench.target);

        var consumer = new TestConsumer(size);
        bench.byteScalarImpl(consumer);
        assertArrayEquals(expected, consumer.mask);

        consumer = new TestConsumer(size);
        bench.byteDefaultImpl(consumer);
        assertArrayEquals(expected, consumer.mask);

        consumer = new TestConsumer(size);
        bench.bytePanamaImpl(consumer);
        assertArrayEquals(expected, consumer.mask);
        /*
        consumer = new TestConsumer(size);
        bench.bytePanamaDirectImpl(consumer);
        assertArrayEquals(expected, consumer.mask);

        consumer = new TestConsumer(size);
        bench.bytePanamaDirectImpltoLong(consumer);
        assertArrayEquals(expected, consumer.mask);
         */
    }

    public void testLong() {
        var bench = new VectorComparisonUtilsLongBenchmark();
        bench.size = size;
        bench.setup();
        final boolean[] expected = expected(bench.data, bench.target);

        var consumer = new TestConsumer(size);
        bench.scalarImpl(consumer);
        assertArrayEquals(expected, consumer.mask);

        consumer = new TestConsumer(size);
        bench.defaultImpl(consumer);
        assertArrayEquals(expected, consumer.mask);

        consumer = new TestConsumer(size);
        bench.panamaImpl(consumer);
        assertArrayEquals(expected, consumer.mask);
        /*
        consumer = new TestConsumer(size);
        bench.panamaDirectImpl(consumer);
        assertArrayEquals(expected, consumer.mask);

        consumer = new TestConsumer(size);
        bench.panamaDirectImpltoLong(consumer);
        assertArrayEquals(expected, consumer.mask);
        */
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            var params = VectorComparisonUtilsByteBenchmark.class.getField("size").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(params).map(Integer::parseInt).map(i -> new Object[] { i }).iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    // Trivial summer, so allow for assertion checking
    static class TestConsumer implements IntConsumer {
        final boolean[] mask;

        TestConsumer(int length) {
            mask = new boolean[length];
        }

        @Override
        public void accept(int idx) {
            assert mask[idx] == false;
            mask[idx] = true;
        }
    }

    static boolean[] expected(byte[] data, byte target) {
        boolean[] expected = new boolean[data.length];
        IntStream.range(0, data.length).forEach(i -> expected[i] = data[i] == target);
        return expected;
    }

    static boolean[] expected(long[] data, long target) {
        boolean[] expected = new boolean[data.length];
        IntStream.range(0, data.length).forEach(i -> expected[i] = data[i] == target);
        return expected;
    }
}
