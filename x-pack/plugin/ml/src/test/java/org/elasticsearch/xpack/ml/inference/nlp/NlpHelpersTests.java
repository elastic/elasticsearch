/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.search.aggregations.pipeline.MovingFunctions;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

        double[][] probabilities = NlpHelpers.convertToProbabilitiesBySoftMax(scores);

        assertThat(probabilities[0][0], closeTo(0.04931133, 0.00000001));
        assertThat(probabilities[0][1], closeTo(0.05449744, 0.00000001));
        assertThat(probabilities[0][2], closeTo(0.89619123, 0.00000001));
        assertThat(probabilities[1][0], closeTo(0.99426607, 0.00000001));
        assertThat(probabilities[1][1], closeTo(0.00301019, 0.00000001));
        assertThat(probabilities[1][2], closeTo(0.00272374, 0.00000001));
    }

    public void testConvertToProbabilitiesBySoftMax_OneDimension() {
        double[] scores = { 0.1, 0.2, 3};
        double[] probabilities = NlpHelpers.convertToProbabilitiesBySoftMax(scores);

        assertThat(probabilities[0], closeTo(0.04931133, 0.00000001));
        assertThat(probabilities[1], closeTo(0.05449744, 0.00000001));
        assertThat(probabilities[2], closeTo(0.89619123, 0.00000001));
    }

    public void testConvertToProbabilitiesBySoftMax_GivenRandom() {
        double[][] scores = new double[100][100];
        for (int i = 0; i < scores.length; i++) {
            for (int j = 0; j < scores[i].length; j++) {
                scores[i][j] = randomDoubleBetween(-10, 10, true);
            }
        }

        double[][] probabilities = NlpHelpers.convertToProbabilitiesBySoftMax(scores);

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

    public void testTopK() {
        {
            int k = 3;
            double[] data = new double[]{1.0, 0.0, 2.0, 8.0, 9.0, 4.2, 4.2, 3.0};
            int[] topKIndices = NlpHelpers.topK(k, data);

            assertArrayEquals(new int[]{4, 3, 5}, topKIndices);
        }
        {
            int k = 5;
            double[] data = new double[]{10.0, 0.0, 2.0, 6.0, 9.0, 4.2, 7.6, 6.0, 0.2, 4.2, 3.0, 0.1, 4.0};
            int[] topKIndices = NlpHelpers.topK(k, data);

            assertArrayEquals(new int[]{0, 4, 6, 7, 3}, topKIndices);
        }
        {
            // in this case use the standard java libraries to sort the
            // doubles and track the starting index of each value
            class ValueAndStartIndex {
                final double value;
                final int originalIndex;
                ValueAndStartIndex(double value, int index) {
                    this.value = value;
                    this.originalIndex = index;
                }
            }

            int size = randomIntBetween(50, 100);
            int k = randomIntBetween(1, 10);
            double[] data = new double[size];
            for (int i=0; i<data.length; i++) {
                data[i] = randomDouble();
            }

            AtomicInteger index = new AtomicInteger(0);
            List<ValueAndStartIndex> sortedByValue =
                Stream.generate(() -> new ValueAndStartIndex(data[index.get()], index.getAndIncrement()))
                .limit(size)
                .sorted((o1, o2) -> Double.compare(o2.value, o1.value))
                .collect(Collectors.toList());

            int[] topKIndices = NlpHelpers.topK(k, data);

            // now compare the starting indices in the sorted list
            // to the top k.
            for (int i=0; i<topKIndices.length; i++) {
                assertEquals(sortedByValue.get(i).originalIndex, topKIndices[i]);
            }
        }
    }
}
