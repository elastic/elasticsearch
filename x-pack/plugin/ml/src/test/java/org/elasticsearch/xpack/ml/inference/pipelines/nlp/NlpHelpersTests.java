/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.search.aggregations.pipeline.MovingFunctions;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class NlpHelpersTests extends ESTestCase {

    public void testConvertToProbabilitiesBySoftMax_GivenConcreteExample() {
        double[][] scores = {
            { 0.1, 0.2, 3},
            { 6, 0.2, 0.1}
        };

        double[][] probabilities = NlpHelpers.convertToProbabilitesBySoftMax(scores);

        assertThat(probabilities[0][0], closeTo(0.04931133, 0.00000001));
        assertThat(probabilities[0][1], closeTo(0.05449744, 0.00000001));
        assertThat(probabilities[0][2], closeTo(0.89619123, 0.00000001));
        assertThat(probabilities[1][0], closeTo(0.99426607, 0.00000001));
        assertThat(probabilities[1][1], closeTo(0.00301019, 0.00000001));
        assertThat(probabilities[1][2], closeTo(0.00272374, 0.00000001));
    }

    public void testConvertToProbabilitiesBySoftMax_GivenRandom() {
        double[][] scores = new double[100][100];
        for (int i = 0; i < scores.length; i++) {
            for (int j = 0; j < scores[i].length; j++) {
                scores[i][j] = randomDoubleBetween(-10, 10, true);
            }
        }

        double[][] probabilities = NlpHelpers.convertToProbabilitesBySoftMax(scores);

        // Assert invariants that
        //   1. each row sums to 1
        //   2. all values are in [0-1]
        assertThat(probabilities.length, equalTo(scores.length));
        for (int i = 0; i < probabilities.length; i++) {
            assertThat(probabilities[i].length, equalTo(scores[i].length));
            double rowSum = MovingFunctions.sum(probabilities[i]);
            assertThat(rowSum, closeTo(1.0, 0.01));
            for (int j = 0; j < probabilities[i].length; j++) {
                assertThat(probabilities[i][j], greaterThanOrEqualTo(0.0));
                assertThat(probabilities[i][j], lessThanOrEqualTo(1.0));
            }
        }
    }
}
