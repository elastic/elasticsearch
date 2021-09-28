/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.utils;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;

public class StatisticsTests extends ESTestCase {

    public void testSoftMax() {
        double[] values = new double[] {Double.NEGATIVE_INFINITY, 1.0, -0.5, Double.NaN, Double.NaN, Double.POSITIVE_INFINITY, 1.0, 5.0};
        double[] softMax = Statistics.softMax(values);

        double[] expected = new double[] {0.0, 0.017599040, 0.003926876, 0.0, 0.0, 0.0, 0.017599040, 0.960875042};

        for(int i = 0; i < expected.length; i++) {
            assertThat(softMax[i], closeTo(expected[i], 0.000001));
        }
    }

    public void testSoftMaxWithNoValidValues() {
        double[] values = new double[] {Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
        expectThrows(IllegalArgumentException.class, () -> Statistics.softMax(values));
    }

    public void testSigmoid() {
        double eps = 0.000001;
        List<Tuple<Double, Double>> paramsAndExpectedReturns = Arrays.asList(
            Tuple.tuple(0.0, 0.5),
            Tuple.tuple(0.5, 0.62245933),
            Tuple.tuple(1.0, 0.73105857),
            Tuple.tuple(10000.0, 1.0),
            Tuple.tuple(-0.5, 0.3775406),
            Tuple.tuple(-1.0, 0.2689414),
            Tuple.tuple(-10000.0, 0.0)
        );
        for (Tuple<Double, Double> expectation : paramsAndExpectedReturns) {
            assertThat(Statistics.sigmoid(expectation.v1()), closeTo(expectation.v2(), eps));
        }
    }

}
