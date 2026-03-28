/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import java.util.Locale;

/**
 * Score normalisation strategies applied after re-scoring on the coordinating node.
 * <ul>
 *   <li><b>NONE</b> — return raw scores (default)</li>
 *   <li><b>MIN_MAX</b> — linear scaling to [0, 1] based on observed min/max</li>
 *   <li><b>Z_SCORE</b> — (score - mean) / stddev; unbounded but comparable across methods</li>
 * </ul>
 */
public enum ScoreNormalization {

    NONE,
    MIN_MAX,
    Z_SCORE;

    public static ScoreNormalization fromString(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Normalise an array of scores in-place according to this strategy.
     *
     * @param scores the scores to normalise
     */
    public void apply(double[] scores) {
        if (scores.length == 0) {
            return;
        }
        switch (this) {
            case NONE -> {}
            case MIN_MAX -> applyMinMax(scores);
            case Z_SCORE -> applyZScore(scores);
        }
    }

    private static void applyMinMax(double[] scores) {
        double min = scores[0];
        double max = scores[0];
        for (double s : scores) {
            min = Math.min(min, s);
            max = Math.max(max, s);
        }
        double range = max - min;
        if (range <= 0) {
            // All scores identical — set to 0
            for (int i = 0; i < scores.length; i++) {
                scores[i] = 0.0;
            }
            return;
        }
        for (int i = 0; i < scores.length; i++) {
            scores[i] = (scores[i] - min) / range;
        }
    }

    private static void applyZScore(double[] scores) {
        double sum = 0;
        for (double s : scores) {
            sum += s;
        }
        double mean = sum / scores.length;

        double varSum = 0;
        for (double s : scores) {
            double diff = s - mean;
            varSum += diff * diff;
        }
        double stddev = Math.sqrt(varSum / scores.length);

        if (stddev <= 0) {
            for (int i = 0; i < scores.length; i++) {
                scores[i] = 0.0;
            }
            return;
        }
        for (int i = 0; i < scores.length; i++) {
            scores[i] = (scores[i] - mean) / stddev;
        }
    }
}
