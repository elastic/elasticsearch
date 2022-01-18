/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.heuristic;

import org.apache.commons.math3.util.FastMath;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.bucket.AbstractNXYSignificanceHeuristicTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class PValueScoreTests extends AbstractNXYSignificanceHeuristicTestCase {

    private static final double eps = 1e-9;

    @Override
    protected Version randomVersion() {
        return randomFrom(Version.V_8_0_0, Version.V_7_16_0);
    }

    @Override
    protected SignificanceHeuristic getHeuristic() {
        return new PValueScore(randomBoolean(), randomBoolean() ? null : randomLongBetween(1, 10000000L));
    }

    @Override
    protected SignificanceHeuristic getHeuristic(boolean includeNegatives, boolean backgroundIsSuperset) {
        return new PValueScore(backgroundIsSuperset, randomBoolean() ? null : randomLongBetween(1, 10000000L));
    }

    @Override
    public void testAssertions() {
        testBackgroundAssertions(
            new PValueScore(true, randomBoolean() ? null : randomLongBetween(1, 10000000L)),
            new PValueScore(false, randomBoolean() ? null : randomLongBetween(1, 10000000L))
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, List.of(new MachineLearning(Settings.EMPTY))).getNamedXContents()
        );
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(
            new SearchModule(Settings.EMPTY, List.of(new MachineLearning(Settings.EMPTY))).getNamedWriteables()
        );
    }

    public void testPValueScore_WhenAllDocsContainTerm() {
        PValueScore pValueScore = new PValueScore(randomBoolean(), null);
        long supersetCount = randomNonNegativeLong();
        long subsetCount = randomLongBetween(0L, supersetCount);
        assertThat(pValueScore.getScore(subsetCount, subsetCount, supersetCount, supersetCount), equalTo(0.0));
    }

    public void testHighPValueScore() {
        boolean backgroundIsSuperset = randomBoolean();
        // supersetFreqCount needs to at less than 20% ratio
        long supersetCount = randomLongBetween(0L, Long.MAX_VALUE / 2);
        long supersetFreqCount = randomLongBetween(0L, (long) (supersetCount / 5.0));
        // subsetFreqCount needs to be at least 25% ratio
        long subsetCount = randomLongBetween((long) (supersetCount / 4.0), supersetCount);
        long subsetFreqCount = randomLongBetween((long) (subsetCount / 4.0), subsetCount);
        if (backgroundIsSuperset) {
            supersetCount += subsetCount;
            supersetFreqCount += subsetFreqCount;
        }

        PValueScore pValueScore = new PValueScore(backgroundIsSuperset, null);
        assertThat(pValueScore.getScore(subsetFreqCount, subsetCount, supersetFreqCount, supersetCount), greaterThanOrEqualTo(700.0));
    }

    public void testLowPValueScore() {
        boolean backgroundIsSuperset = randomBoolean();
        // supersetFreqCount needs to at least be 20% ratio
        long supersetCount = randomLongBetween(0L, Long.MAX_VALUE / 2);
        long supersetFreqCount = randomLongBetween((long) (supersetCount / 5.0), supersetCount);
        // subsetFreqCount needs to be less than 16% ratio
        long subsetCount = randomLongBetween((long) (supersetCount / 5.0), supersetCount);
        long subsetFreqCount = randomLongBetween(0L, (long) (subsetCount / 6.0));
        if (backgroundIsSuperset) {
            supersetCount += subsetCount;
            supersetFreqCount += subsetFreqCount;
        }

        PValueScore pValueScore = new PValueScore(backgroundIsSuperset, null);
        assertThat(
            pValueScore.getScore(subsetFreqCount, subsetCount, supersetFreqCount, supersetCount),
            allOf(lessThanOrEqualTo(5.0), greaterThanOrEqualTo(0.0))
        );
    }

    public void testPValueScore() {
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(10, 100, 100, 1000)), closeTo(1.0, eps));
        assertThat(FastMath.exp(-new PValueScore(false, 200L).getScore(10, 100, 100, 1000)), closeTo(1.0, eps));
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(10, 100, 10, 1000)), closeTo(0.003972388976814195, eps));
        assertThat(FastMath.exp(-new PValueScore(false, 200L).getScore(10, 100, 10, 1000)), closeTo(0.020890782016496683, eps));
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(10, 100, 200, 1000)), closeTo(1.0, eps));
        assertThat(FastMath.exp(-new PValueScore(false, 200L).getScore(10, 100, 200, 1000)), closeTo(1.0, eps));
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(20, 10000, 5, 10000)), closeTo(1.0, eps));
        assertThat(FastMath.exp(-new PValueScore(false, 200L).getScore(20, 10000, 5, 10000)), closeTo(1.0, eps));
    }

    public void testSmallChanges() {
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(1, 4205, 0, 821496)), closeTo(0.9999037287868853, eps));

        // Same(ish) ratios
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(10, 4205, 195, 82149)), closeTo(0.9995943820612134, eps));
        assertThat(FastMath.exp(-new PValueScore(false, 100L).getScore(10, 4205, 195, 82149)), closeTo(0.9876284079864467, eps));

        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(10, 4205, 1950, 821496)), closeTo(0.9999942565428899, eps));
        assertThat(FastMath.exp(-new PValueScore(false, 100L).getScore(10, 4205, 1950, 821496)), closeTo(1.0, eps));

        // 4% vs 0%
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(168, 4205, 0, 821496)), closeTo(1.2680918648731284e-26, eps));
        assertThat(FastMath.exp(-new PValueScore(false, 100L).getScore(168, 4205, 0, 821496)), closeTo(0.3882951183744724, eps));
        // 4% vs 2%
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(168, 4205, 16429, 821496)), closeTo(8.542608559219833e-5, eps));
        assertThat(FastMath.exp(-new PValueScore(false, 100L).getScore(168, 4205, 16429, 821496)), closeTo(0.579463586350363, eps));
        // 4% vs 3.5%
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(168, 4205, 28752, 821496)), closeTo(0.8833950526957098, eps));
        assertThat(FastMath.exp(-new PValueScore(false, 100L).getScore(168, 4205, 28752, 821496)), closeTo(1.0, eps));
    }

    public void testLargerValues() {
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(101000, 1000000, 500000, 5000000)), closeTo(1.0, eps));
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(102000, 1000000, 500000, 5000000)), closeTo(1.0, eps));
        assertThat(FastMath.exp(-new PValueScore(false, null).getScore(103000, 1000000, 500000, 5000000)), closeTo(1.0, eps));
    }

    public void testScoreIsZero() {
        for (int j = 0; j < 10; j++) {
            assertThat(new PValueScore(false, null).getScore((j + 1) * 5, (j + 10) * 100, (j + 1) * 10, (j + 10) * 100), equalTo(0.0));
        }
    }

    public void testIncreasedSubsetIncreasedScore() {
        final Function<Long, Double> getScore = (subsetFreq) -> new PValueScore(false, null).getScore(subsetFreq, 5000, 5, 5000);
        double priorScore = getScore.apply(5L);
        assertThat(priorScore, greaterThanOrEqualTo(0.0));
        for (int j = 1; j < 11; j++) {
            double nextScore = getScore.apply(j * 10L);
            assertThat(nextScore, greaterThanOrEqualTo(0.0));
            assertThat(nextScore, greaterThanOrEqualTo(priorScore));
            priorScore = nextScore;
        }
    }

}
