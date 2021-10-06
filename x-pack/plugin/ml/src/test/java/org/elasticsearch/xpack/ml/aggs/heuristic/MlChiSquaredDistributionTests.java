/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.heuristic;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;

public class MlChiSquaredDistributionTests extends ESTestCase {

    public void testSurvivalFunction() {
        double[] inputs = new double[] {0.210212602629, 0.554298076728, 0.831211613487, 1.14547622606, 1.61030798696,
            20.5150056524, 15.0862724694, 12.8325019940, 11.0704976935, 9.23635689978, 0.0, -1.0};
        double[] results = new double[] {0.001, 0.01, 0.025, 0.05, 0.1, 0.999, 0.990, 0.975, 0.950, 0.900, 0.0, 0.0};

        MlChiSquaredDistribution mlChiSquaredDistribution = new MlChiSquaredDistribution(5.0);

        for (int j = 0; j < inputs.length; j++) {
            assertThat(mlChiSquaredDistribution.survivalFunction(inputs[j]), closeTo(1 - results[j], 1e-9));
        }
    }

}
