/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.elasticsearch.test.ESTestCase;

public class ScoreNormalizationTests extends ESTestCase {

    public void testNoneDoesNotModifyScores() {
        double[] scores = { 1.0, 5.0, 10.0 };
        ScoreNormalization.NONE.apply(scores);
        assertEquals(1.0, scores[0], 0.001);
        assertEquals(5.0, scores[1], 0.001);
        assertEquals(10.0, scores[2], 0.001);
    }

    public void testMinMaxScalesToZeroOne() {
        double[] scores = { 2.0, 5.0, 8.0 };
        ScoreNormalization.MIN_MAX.apply(scores);
        assertEquals(0.0, scores[0], 0.001);
        assertEquals(0.5, scores[1], 0.001);
        assertEquals(1.0, scores[2], 0.001);
    }

    public void testMinMaxIdenticalScores() {
        double[] scores = { 5.0, 5.0, 5.0 };
        ScoreNormalization.MIN_MAX.apply(scores);
        assertEquals(0.0, scores[0], 0.001);
        assertEquals(0.0, scores[1], 0.001);
        assertEquals(0.0, scores[2], 0.001);
    }

    public void testZScoreZeroMeanUnitVar() {
        double[] scores = { 1.0, 2.0, 3.0, 4.0, 5.0 };
        ScoreNormalization.Z_SCORE.apply(scores);

        // Mean should be 0
        double sum = 0;
        for (double s : scores) {
            sum += s;
        }
        assertEquals(0.0, sum / scores.length, 0.001);

        // Stddev should be ~1
        double varSum = 0;
        for (double s : scores) {
            varSum += s * s;
        }
        double stddev = Math.sqrt(varSum / scores.length);
        assertEquals(1.0, stddev, 0.001);
    }

    public void testZScoreIdenticalScores() {
        double[] scores = { 3.0, 3.0, 3.0 };
        ScoreNormalization.Z_SCORE.apply(scores);
        assertEquals(0.0, scores[0], 0.001);
        assertEquals(0.0, scores[1], 0.001);
        assertEquals(0.0, scores[2], 0.001);
    }

    public void testEmptyArray() {
        double[] scores = {};
        ScoreNormalization.MIN_MAX.apply(scores);
        ScoreNormalization.Z_SCORE.apply(scores);
        assertEquals(0, scores.length);
    }

    public void testFromString() {
        assertEquals(ScoreNormalization.NONE, ScoreNormalization.fromString("none"));
        assertEquals(ScoreNormalization.MIN_MAX, ScoreNormalization.fromString("min_max"));
        assertEquals(ScoreNormalization.Z_SCORE, ScoreNormalization.fromString("z_score"));
        assertEquals(ScoreNormalization.MIN_MAX, ScoreNormalization.fromString("MIN_MAX"));
    }

    public void testMinMaxPreservesOrdering() {
        double[] scores = { 10.0, 1.0, 50.0, 25.0 };
        ScoreNormalization.MIN_MAX.apply(scores);
        assertTrue(scores[2] > scores[3]);
        assertTrue(scores[3] > scores[0]);
        assertTrue(scores[0] > scores[1]);
    }
}
