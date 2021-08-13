/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.stream.DoubleStream;

import static org.hamcrest.Matchers.arrayContaining;

public class DoubleArrayTests extends ESTestCase {

    public void testCumulativeSum() {
        assertThat(
            boxed(DoubleArray.cumulativeSum(DoubleStream.generate(() -> 0.0).limit(10).toArray())),
            arrayContaining(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        );
        assertThat(
            boxed(DoubleArray.cumulativeSum(DoubleStream.generate(() -> 1.0).limit(10).toArray())),
            arrayContaining(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)
        );
    }

    public void testDivMut() {
        double[] zeros = DoubleStream.generate(() -> 0.0).limit(10).toArray();
        DoubleArray.divMut(zeros, randomDouble());
        assertThat(
            boxed(zeros),
            arrayContaining(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        );

        double[] ones = DoubleStream.generate(() -> 1.0).limit(10).toArray();
        DoubleArray.divMut(ones, 2.0);
        assertThat(
            boxed(ones),
            arrayContaining(0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5)
        );

    }

    public void testDivMut_validation() {
        expectThrows(
            IllegalArgumentException.class,
            () -> DoubleArray.divMut(
                DoubleStream.generate(ESTestCase::randomDouble).limit(10).toArray(),
                0.0
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> DoubleArray.divMut(
                DoubleStream.generate(ESTestCase::randomDouble).limit(10).toArray(),
                randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
            )
        );
    }

    private static Double[] boxed(double[] arr) {
        return Arrays.stream(arr).boxed().toArray(Double[]::new);
    }

}
