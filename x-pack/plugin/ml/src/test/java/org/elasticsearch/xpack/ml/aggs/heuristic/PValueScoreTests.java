/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.heuristic;

import org.apache.commons.math3.util.FastMath;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.test.AbstractSignificanceHeuristicTests;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Arrays;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class PValueScoreTests extends AbstractSignificanceHeuristicTests {

    private static final double eps = 0.00000001;

    @Override
    protected SignificanceHeuristic getHeuristic() {
        return new PValueScore(randomBoolean());
    }

    @Override
    protected boolean testZeroScore() {
        return true;
    }

    @Override
    public void testAssertions() {
        testBackgroundAssertions(new PValueScore(true), new PValueScore(false));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, Arrays.asList(new MachineLearning(Settings.EMPTY, null))).getNamedXContents()
        );
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(
            new SearchModule(Settings.EMPTY, Arrays.asList(new MachineLearning(Settings.EMPTY, null))).getNamedWriteables()
        );
    }

    public void testPValueScore_WhenAllDocsContainTerm() {
        PValueScore pValueScore = new PValueScore(randomBoolean());
        long supersetCount = randomNonNegativeLong();
        long subsetCount = randomLongBetween(0L, supersetCount);
        assertThat(pValueScore.getScore(subsetCount, subsetCount, supersetCount, supersetCount), equalTo(0.0));
    }

    public void testHighPValueScore() {
        boolean backgroundIsSuperset = randomBoolean();
        long supersetCount = randomLongBetween(0L, Long.MAX_VALUE/2);
        long subsetCount = randomLongBetween(0L, supersetCount);
        if (backgroundIsSuperset) {
            supersetCount += subsetCount;
        }

        PValueScore pValueScore = new PValueScore(backgroundIsSuperset);
        assertThat(pValueScore.getScore(subsetCount, subsetCount, subsetCount, supersetCount), greaterThanOrEqualTo(700.0));
    }

    public void testLowPValueScore() {
        boolean backgroundIsSuperset = randomBoolean();
        long supersetCount = randomLongBetween(0L, Long.MAX_VALUE/2);
        long subsetCount = randomLongBetween(0L, supersetCount);
        long subsetFreqCount = randomLongBetween(0L, subsetCount/5);
        if (backgroundIsSuperset) {
            supersetCount += subsetCount;
        }

        PValueScore pValueScore = new PValueScore(backgroundIsSuperset);
        assertThat(
            pValueScore.getScore(subsetFreqCount, subsetCount, subsetCount, supersetCount),
            allOf(lessThanOrEqualTo(5.0), greaterThanOrEqualTo(0.0))
        );
    }

    public void testPValueScore() {
        assertThat(
            FastMath.exp(-new PValueScore(false).getScore(10, 100, 100, 1000)),
            closeTo(1.0, eps)
        );
        assertThat(
            FastMath.exp(-new PValueScore(false).getScore(10, 100, 10, 1000)),
            closeTo(0.0014942974424670364, eps)
        );
        assertThat(
            FastMath.exp(-new PValueScore(false).getScore(10, 100, 200, 1000)),
            closeTo(1.0, eps)
        );
        assertThat(
            FastMath.exp(-new PValueScore(false).getScore(20, 10000, 5, 10000)),
            closeTo(0.3154715149153073, eps)
        );
    }

    public void testSmallChanges() {
        assertThat(
            FastMath.exp(-new PValueScore(false).getScore(1, 4205, 0, 821496)),
            closeTo(0.47862401010222105, eps)
        );
        // Same(ish) ratios
        assertThat(
            FastMath.exp(-new PValueScore(false).getScore(10, 4205, 195, 82149)),
            closeTo(0.4946943227464169, eps)
        );
        assertThat(
            FastMath.exp(-new PValueScore(false).getScore(10, 4205, 1950, 821496)),
            closeTo(0.49338445847730966, eps)
        );

        // 4% vs 0%
        assertThat(
            FastMath.exp(-new PValueScore(false).getScore(168, 4205, 0, 821496)),
            closeTo(6.340459324365642e-27, eps)
        );
        // 4% vs 2%
        assertThat(
            FastMath.exp(-new PValueScore(false).getScore(168, 4205, 16429, 821496)),
            closeTo(2.392323732118125e-06, eps)
        );
        // 4% vs 3.5%
        assertThat(
            FastMath.exp(-new PValueScore(false).getScore(168, 4205, 28752, 821496)),
            closeTo(0.2364469224974871, eps)
        );
    }

}
