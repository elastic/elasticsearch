/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.normalize;

import org.elasticsearch.test.ESTestCase;

import java.util.function.DoubleUnaryOperator;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class NormalizePipelineMethodsTests extends ESTestCase {

    private static final double[] DATA = new double[] { 1, 50, Double.NaN, 8, 10, 4, 3, 0, 10, -10, -4 };
    private static final int COUNT = 10;
    private static final double MIN = -10;
    private static final double MAX = 50;
    private static final double SUM = 72;
    private static final double MEAN = SUM / COUNT;

    public void testRescaleZeroToOne() {
        NormalizePipelineMethods.RescaleZeroToOne normalizer = new NormalizePipelineMethods.RescaleZeroToOne(DATA);
        assertSinglePassStatistics(normalizer);
        double[] normalized = new double[] {
            0.18333333333333332,
            1.0,
            Double.NaN,
            0.3,
            0.3333333333333333,
            0.23333333333333334,
            0.21666666666666667,
            0.16666666666666666,
            0.3333333333333333,
            0.0,
            0.1 };
        assertNormalized(normalizer, normalized);
    }

    public void testRescaleZeroToOneHundred() {
        NormalizePipelineMethods.RescaleZeroToOneHundred normalizer = new NormalizePipelineMethods.RescaleZeroToOneHundred(DATA);
        assertSinglePassStatistics(normalizer);
        double[] normalized = new double[] {
            18.333333333333332,
            100.0,
            Double.NaN,
            30.0,
            33.333333333333336,
            23.333333333333332,
            21.666666666666668,
            16.666666666666668,
            33.333333333333336,
            0.0,
            10.0 };
        assertNormalized(normalizer, normalized);
    }

    public void testMean() {
        NormalizePipelineMethods.Mean normalizer = new NormalizePipelineMethods.Mean(DATA);
        assertSinglePassStatistics(normalizer);
        double[] normalized = new double[] {
            -0.10333333333333333,
            0.7133333333333333,
            Double.NaN,
            0.01333333333333333,
            0.04666666666666666,
            -0.05333333333333334,
            -0.07,
            -0.12000000000000001,
            0.04666666666666666,
            -0.2866666666666667,
            -0.18666666666666665 };
        assertNormalized(normalizer, normalized);
    }

    public void testZScore() {
        NormalizePipelineMethods.ZScore normalizer = new NormalizePipelineMethods.ZScore(DATA);
        assertSinglePassStatistics(normalizer);
        double[] normalized = new double[] {
            -0.4012461740749068,
            2.7698929436138724,
            Double.NaN,
            0.05177369988063312,
            0.18120794958221595,
            -0.20709479952253254,
            -0.27181192437332397,
            -0.4659632989256982,
            0.18120794958221595,
            -1.1131345474336123,
            -0.7248317983288638 };
        assertNormalized(normalizer, normalized);
    }

    public void testSoftmax() {
        NormalizePipelineMethods.Softmax normalizer = new NormalizePipelineMethods.Softmax(DATA);
        double[] normalized = new double[] {
            5.242885663363464E-22,
            1.0,
            Double.NaN,
            5.74952226429356E-19,
            4.24835425529159E-18,
            1.0530617357553813E-20,
            3.8739976286871875E-21,
            1.928749847963918E-22,
            4.24835425529159E-18,
            8.756510762696521E-27,
            3.532628572200807E-24 };

        assertNormalized(normalizer, normalized);
    }

    private void assertSinglePassStatistics(NormalizePipelineMethods.SinglePassSimpleStatisticsMethod normalizer) {
        assertThat(normalizer.min, equalTo(MIN));
        assertThat(normalizer.max, equalTo(MAX));
        assertThat(normalizer.count, equalTo(COUNT));
        assertThat(normalizer.sum, equalTo(SUM));
        assertThat(normalizer.mean, equalTo(MEAN));
    }

    private void assertNormalized(DoubleUnaryOperator op, double[] normalizedData) {
        assertThat(normalizedData.length, equalTo(DATA.length));
        for (int i = 0; i < DATA.length; i++) {
            if (Double.isNaN(DATA[i])) {
                assertTrue(Double.isNaN(normalizedData[i]));
            } else {
                assertThat(op.applyAsDouble(DATA[i]), closeTo(normalizedData[i], 0.00000000001));
            }
        }
    }
}
