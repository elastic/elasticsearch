/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AucRocTests extends AbstractSerializingTestCase<AucRoc> {

    @Override
    protected AucRoc doParseInstance(XContentParser parser) throws IOException {
        return AucRoc.PARSER.apply(parser, null);
    }

    @Override
    protected AucRoc createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<AucRoc> instanceReader() {
        return AucRoc::new;
    }

    public static AucRoc createRandom() {
        return new AucRoc(randomBoolean() ? randomBoolean() : null);
    }

    public void testCalculateAucScore_GivenZeroPercentiles() {
        double[] tpPercentiles = zeroPercentiles();
        double[] fpPercentiles = zeroPercentiles();

        List<AucRoc.AucRocPoint> curve = AucRoc.buildAucRocCurve(tpPercentiles, fpPercentiles);
        double aucRocScore = AucRoc.calculateAucScore(curve);

        assertThat(aucRocScore, closeTo(0.5, 0.01));
    }

    public void testCalculateAucScore_GivenRandomTpPercentilesAndZeroFpPercentiles() {
        double[] tpPercentiles = randomPercentiles();
        double[] fpPercentiles = zeroPercentiles();

        List<AucRoc.AucRocPoint> curve = AucRoc.buildAucRocCurve(tpPercentiles, fpPercentiles);
        double aucRocScore = AucRoc.calculateAucScore(curve);

        assertThat(aucRocScore, closeTo(1.0, 0.1));
    }

    public void testCalculateAucScore_GivenZeroTpPercentilesAndRandomFpPercentiles() {
        double[] tpPercentiles = zeroPercentiles();
        double[] fpPercentiles = randomPercentiles();

        List<AucRoc.AucRocPoint> curve = AucRoc.buildAucRocCurve(tpPercentiles, fpPercentiles);
        double aucRocScore = AucRoc.calculateAucScore(curve);

        assertThat(aucRocScore, closeTo(0.0, 0.1));
    }

    public void testCalculateAucScore_GivenRandomPercentiles() {
        for (int i = 0; i < 20; i++) {
            double[] tpPercentiles = randomPercentiles();
            double[] fpPercentiles = randomPercentiles();

            List<AucRoc.AucRocPoint> curve = AucRoc.buildAucRocCurve(tpPercentiles, fpPercentiles);
            double aucRocScore = AucRoc.calculateAucScore(curve);

            List<AucRoc.AucRocPoint> inverseCurve = AucRoc.buildAucRocCurve(fpPercentiles, tpPercentiles);
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

        double[] tpSimplified = new double[] { 0.3, 0.6, 0.5 , 0.8 };
        double[] fpSimplified = new double[] { 0.1, 0.3, 0.5 , 0.5 };

        for (int i = 0; i < tpPercentiles.length; i++) {
            int simplifiedIndex = i / 25;
            tpPercentiles[i] = tpSimplified[simplifiedIndex];
            fpPercentiles[i] = fpSimplified[simplifiedIndex];
        }

        List<AucRoc.AucRocPoint> curve = AucRoc.buildAucRocCurve(tpPercentiles, fpPercentiles);
        double aucRocScore = AucRoc.calculateAucScore(curve);

        List<AucRoc.AucRocPoint> inverseCurve = AucRoc.buildAucRocCurve(fpPercentiles, tpPercentiles);
        double inverseAucRocScore = AucRoc.calculateAucScore(inverseCurve);

        assertThat(aucRocScore, closeTo(0.8, 0.05));
        assertThat(inverseAucRocScore, closeTo(0.2, 0.05));
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
}
