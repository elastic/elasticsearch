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

    static double[][] convertToProbabilitiesBySoftMax(double[][] scores) {
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

    static double[] convertToProbabilitiesBySoftMax(double[] scores) {
        double[] probabilities = new double[scores.length];
        double sum = 0.0;
        double maxScore = MovingFunctions.max(scores);
        for (int i = 0; i < scores.length; i++) {
            probabilities[i] = Math.exp(scores[i] - maxScore);
            sum += probabilities[i];
        }
        for (int i = 0; i < scores.length; i++) {
            probabilities[i] /= sum;
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

    static int[] topK(int k, double[] arr) {
        int[] topK = new int[k];
        for (int i=0; i<k; i++) {
            topK[i] = i;
        }

        int min = indexOfSmallestValue(topK, arr);
        for (int i = k; i < arr.length; i++) {
            if (arr[i] > arr[topK[min]]) {
                topK[min] = i;
                min = indexOfSmallestValue(topK, arr);
            }
        }

        // Sort the result so the largest values are at the beginning
        insertionSort(topK, arr);
        return topK;
    }

    // modifies indices
    private static void insertionSort(int [] indices, double [] data) {
        for (int i=1; i< indices.length; i++) {
            int j = i;
            while (j > 0 && data[indices[j-1]] < data[indices[j]]) {
                int tmp = indices[j-1];
                indices[j-1] = indices[j];
                indices[j] = tmp;
                j--;
            }
        }
    }

    private static int indexOfSmallestValue(int [] indices, double [] data) {
        int minIndex = 0;
        for (int i=1; i<indices.length; i++) {
            if (data[indices[i]] < data[indices[minIndex]]) {
                minIndex = i;
            }
        }
        return minIndex;
    }
}
