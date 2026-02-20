/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;
import org.openjdk.jmh.annotations.Param;

import java.io.IOException;
import java.util.Arrays;

public class Base64VectorBenchmarkTests extends ESTestCase {

    final int dims;

    public Base64VectorBenchmarkTests(int dims) {
        this.dims = dims;
    }

    public void testBasic() throws IOException {
        var bench = new Base64VectorBenchmark();
        bench.dims = dims;
        bench.setup();

        FloatArrayConsumerAccess consumer1 = new FloatArrayConsumerAccess(bench.numVectors);
        FloatArrayConsumerAccess consumer2 = new FloatArrayConsumerAccess(bench.numVectors);
        FloatArrayConsumerAccess consumer3 = new FloatArrayConsumerAccess(bench.numVectors);

        bench.parserVectorFloatsImpl(consumer1);
        bench.parserVectorBase64Impl(consumer2);
        bench.parserVectorBase64NoCopyImpl(consumer3);

        float[][] floats1 = consumer1.get();
        float[][] floats2 = consumer2.get();
        float[][] floats3 = consumer3.get();

        assertArrayEquals(floats1, floats2);
        assertArrayEquals(floats1, floats3);
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            var params = Base64VectorBenchmark.class.getField("dims").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(params).map(Integer::parseInt).map(i -> new Object[] { i }).iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    static class FloatArrayConsumerAccess implements Base64VectorBenchmark.FloatArrayConsumer {

        float[][] floats;
        int count = 0;

        FloatArrayConsumerAccess(int size) {
            floats = new float[size][];
        }

        float[][] get() {
            return floats;
        }

        @Override
        public void consume(float[] value) {
            floats[count] = value;
            count++;
        }
    }
}
