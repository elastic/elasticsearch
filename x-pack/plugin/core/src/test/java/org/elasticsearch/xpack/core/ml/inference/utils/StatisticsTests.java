/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.utils;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;

public class StatisticsTests extends ESTestCase {

    public void testSoftMax() {
        List<Double> values = Arrays.asList(Double.NEGATIVE_INFINITY, 1.0, -0.5, null, Double.NaN, Double.POSITIVE_INFINITY, 1.0, 5.0);
        List<Double> softMax = Statistics.softMax(values);

        List<Double> expected = Arrays.asList(0.0, 0.017599040, 0.003926876, 0.0, 0.0, 0.0, 0.017599040, 0.960875042);

        for(int i = 0; i < expected.size(); i++) {
            assertThat(softMax.get(i), closeTo(expected.get(i), 0.000001));
        }
    }

    public void testSoftMaxWithNoValidValues() {
        List<Double> values = Arrays.asList(Double.NEGATIVE_INFINITY, null, Double.NaN, Double.POSITIVE_INFINITY);
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
