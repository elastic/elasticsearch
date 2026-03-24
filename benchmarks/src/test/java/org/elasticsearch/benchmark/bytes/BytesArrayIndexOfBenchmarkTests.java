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

public class BytesArrayIndexOfBenchmarkTests extends ESTestCase {

    final int size;

    public BytesArrayIndexOfBenchmarkTests(int size) {
        this.size = size;
    }

    public void testIndexOf() {
        var bench = new BytesArrayIndexOfBenchmark();
        bench.size = size;
        bench.setup();
        assertEquals(size - 1, bench.indexOf());
        assertEquals(size - 1, bench.indexOfPanama());
        assertEquals(size - 1, bench.withOffsetIndexOf());
        assertEquals(size - 1, bench.withOffsetIndexPanama());
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            var params = BytesArrayIndexOfBenchmark.class.getField("size").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(params).map(Integer::parseInt).map(i -> new Object[] { i }).iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
