/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.search.aggregations.pipeline.MovingFunctions;

public final class NlpHelpers {

    private NlpHelpers() {}

    static double[][] convertToProbabilitesBySoftMax(double[][] scores) {
        double[][] probabilities = new double[scores.length][];
        double[] sum = new double[scores.length];
        for (int i = 0; i < scores.length; i++) {
            probabilities[i] = new double[scores[i].length];
            double maxScore = MovingFunctions.max(scores[i]);
            for (int j = 0; j < scores[i].length; j++) {
                probabilities[i][j] = Math.exp(scores[i][j] - maxScore);
                sum[i] += probabilities[i][j];
            }
        }
        for (int i = 0; i < scores.length; i++) {
            for (int j = 0; j < scores[i].length; j++) {
                probabilities[i][j] /= sum[i];
            }
        }
        return probabilities;
    }

    static int argmax(double[] arr) {
        int maxIndex = 0;
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] > arr[maxIndex]) {
                maxIndex = i;
            }
        }
        return maxIndex;
    }
}
