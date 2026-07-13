/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.search.aggregations.pipeline.MovingFunctions;

import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;

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

    /**
     * Find the index of the highest value in {@code arr}
     * @param arr Array to search
     * @return Index of highest value
     */
    static int argmax(double[] arr) {
        int maxIndex = 0;
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] > arr[maxIndex]) {
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    /**
     * Find the top K highest values in {@code arr} and their
     * index positions. Similar to {@link #argmax(double[])}
     * but generalised to k instead of just 1. If {@code arr.length < k}
     * then {@code arr.length} items are returned.
     *
     * The function uses a PriorityQueue of size {@code k} to
     * track the highest values
     *
     * @param k Number of values to track
     * @param arr Array to search
     * @return Index positions and values of the top k elements.
     */
    static ScoreAndIndex[] topK(int k, double[] arr) {
        if (k > arr.length) {
            k = arr.length;
        }

        PriorityQueue<ScoreAndIndex> minHeap = new PriorityQueue<>(k, Comparator.comparingDouble(o -> o.score));
        // initialise with the first k values
        for (int i = 0; i < k; i++) {
            minHeap.add(new ScoreAndIndex(arr[i], i));
        }

        double minValue = minHeap.peek().score;
        for (int i = k; i < arr.length; i++) {
            if (arr[i] > minValue) {
                minHeap.poll();
                minHeap.add(new ScoreAndIndex(arr[i], i));
                minValue = minHeap.peek().score;
            }
        }

        ScoreAndIndex[] result = new ScoreAndIndex[k];
        // The result should be ordered highest score first
        // so reverse the min heap order
        for (int i = k - 1; i >= 0; i--) {
            result[i] = minHeap.poll();
        }
        return result;
    }

    public static class ScoreAndIndex {
        final double score;
        final int index;

        ScoreAndIndex(double value, int index) {
            this.score = value;
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ScoreAndIndex that = (ScoreAndIndex) o;
            return Double.compare(that.score, score) == 0 && index == that.index;
        }

        @Override
        public int hashCode() {
            return Objects.hash(score, index);
        }
    }
}
