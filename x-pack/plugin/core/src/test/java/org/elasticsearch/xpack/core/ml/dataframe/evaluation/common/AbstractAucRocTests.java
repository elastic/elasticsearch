/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.common;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.AucRoc;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.common.AbstractAucRoc.AucRocPoint;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AbstractAucRocTests extends ESTestCase {

    public void testCalculateAucScore_GivenZeroPercentiles() {
        double[] tpPercentiles = zeroPercentiles();
        double[] fpPercentiles = zeroPercentiles();

        List<AucRocPoint> curve = AucRoc.buildAucRocCurve(tpPercentiles, fpPercentiles);
        double aucRocScore = AucRoc.calculateAucScore(curve);

        assertThat(aucRocScore, closeTo(0.5, 0.01));
    }

    public void testCalculateAucScore_GivenRandomTpPercentilesAndZeroFpPercentiles() {
        double[] tpPercentiles = randomPercentiles();
        double[] fpPercentiles = zeroPercentiles();

        List<AucRocPoint> curve = AucRoc.buildAucRocCurve(tpPercentiles, fpPercentiles);
        double aucRocScore = AucRoc.calculateAucScore(curve);

        assertThat(aucRocScore, closeTo(1.0, 0.1));
    }

    public void testCalculateAucScore_GivenZeroTpPercentilesAndRandomFpPercentiles() {
        double[] tpPercentiles = zeroPercentiles();
        double[] fpPercentiles = randomPercentiles();

        List<AucRocPoint> curve = AucRoc.buildAucRocCurve(tpPercentiles, fpPercentiles);
        double aucRocScore = AucRoc.calculateAucScore(curve);

        assertThat(aucRocScore, closeTo(0.0, 0.1));
    }

    public void testCalculateAucScore_GivenRandomPercentiles() {
        for (int i = 0; i < 20; i++) {
            double[] tpPercentiles = randomPercentiles();
            double[] fpPercentiles = randomPercentiles();

            List<AucRocPoint> curve = AucRoc.buildAucRocCurve(tpPercentiles, fpPercentiles);
            double aucRocScore = AucRoc.calculateAucScore(curve);

            List<AucRocPoint> inverseCurve = AucRoc.buildAucRocCurve(fpPercentiles, tpPercentiles);
            double inverseAucRocScore = AucRoc.calculateAucScore(inverseCurve);

            assertThat(aucRocScore, greaterThanOrEqualTo(0.0));
            assertThat(aucRocScore, lessThanOrEqualTo(1.0));
            assertThat(inverseAucRocScore, greaterThanOrEqualTo(0.0));
            assertThat(inverseAucRocScore, lessThanOrEqualTo(1.0));
            assertThat(aucRocScore + inverseAucRocScore, closeTo(1.0, 0.05));
        }
    }

    public void testCalculateAucScore_GivenPrecalculated() {
        double[] tpPercentiles = new double[99];
        double[] fpPercentiles = new double[99];

        double[] tpSimplified = new double[] { 0.3, 0.6, 0.5, 0.8 };
        double[] fpSimplified = new double[] { 0.1, 0.3, 0.5, 0.5 };

        for (int i = 0; i < tpPercentiles.length; i++) {
            int simplifiedIndex = i / 25;
            tpPercentiles[i] = tpSimplified[simplifiedIndex];
            fpPercentiles[i] = fpSimplified[simplifiedIndex];
        }

        List<AucRocPoint> curve = AucRoc.buildAucRocCurve(tpPercentiles, fpPercentiles);
        double aucRocScore = AucRoc.calculateAucScore(curve);

        List<AucRocPoint> inverseCurve = AucRoc.buildAucRocCurve(fpPercentiles, tpPercentiles);
        double inverseAucRocScore = AucRoc.calculateAucScore(inverseCurve);

        assertThat(aucRocScore, closeTo(0.8, 0.05));
        assertThat(inverseAucRocScore, closeTo(0.2, 0.05));
    }

    public void testCollapseEqualThresholdPoints_GivenEmpty() {
        assertThat(AbstractAucRoc.collapseEqualThresholdPoints(Collections.emptyList()), is(empty()));
    }

    public void testCollapseEqualThresholdPoints() {
        List<AucRocPoint> curve = Arrays.asList(
            new AucRocPoint(0.0, 0.0, 1.0),
            new AucRocPoint(0.1, 0.9, 0.1),
            new AucRocPoint(0.2, 0.8, 0.2),
            new AucRocPoint(0.1, 0.9, 0.2),
            new AucRocPoint(0.3, 0.6, 0.3),
            new AucRocPoint(0.5, 0.5, 0.4),
            new AucRocPoint(0.4, 0.6, 0.4),
            new AucRocPoint(0.9, 0.1, 0.4),
            new AucRocPoint(1.0, 1.0, 0.0)
        );

        List<AucRocPoint> collapsed = AbstractAucRoc.collapseEqualThresholdPoints(curve);

        assertThat(collapsed.size(), equalTo(6));
        assertThat(collapsed.get(0), equalTo(curve.get(0)));
        assertThat(collapsed.get(1), equalTo(curve.get(1)));
        assertPointCloseTo(collapsed.get(2), 0.15, 0.85, 0.2);
        assertThat(collapsed.get(3), equalTo(curve.get(4)));
        assertPointCloseTo(collapsed.get(4), 0.6, 0.4, 0.4);
        assertThat(collapsed.get(5), equalTo(curve.get(8)));
    }

    public static double[] zeroPercentiles() {
        double[] percentiles = new double[99];
        Arrays.fill(percentiles, 0.0);
        return percentiles;
    }

    public static double[] randomPercentiles() {
        double[] percentiles = new double[99];
        for (int i = 0; i < percentiles.length; i++) {
            percentiles[i] = randomDouble();
        }
        Arrays.sort(percentiles);
        return percentiles;
    }

    private static void assertPointCloseTo(AucRocPoint point, double expectedTpr, double expectedFpr, double expectedThreshold) {
        assertThat(point.tpr, closeTo(expectedTpr, 0.00001));
        assertThat(point.fpr, closeTo(expectedFpr, 0.00001));
        assertThat(point.threshold, closeTo(expectedThreshold, 0.00001));
    }
}
